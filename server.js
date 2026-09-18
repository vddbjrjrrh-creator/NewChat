/* ===========================================================
   Newchat — сервер
   Аккаунты, чаты, доставка сообщений в реальном времени,
   юзернеймы, биржа, кошелёк, жалобы, верификация.
   =========================================================== */

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');

/* Юзернеймы, которым сервер выдаёт галочку разработчика.
   Впиши сюда себя и друга. */
const DEV_USERNAMES = (process.env.DEV_USERNAMES || 'shadow')
  .split(',').map(s => s.trim().toLowerCase().replace(/^@/, '')).filter(Boolean);
/* Помощники разработчика: плашка CO-DEV, но без доступа к дев-панели */
const CODEV_USERNAMES = (process.env.CODEV_USERNAMES || '')
  .split(',').map(s => s.trim().toLowerCase().replace(/^@/, '')).filter(Boolean);

/* Ключ от SMS.ru. Если не задан — код показывается на экране (режим разработки). */
const SMSRU_API_ID = process.env.SMSRU_API_ID || '';
const SMS_ENABLED = !!SMSRU_API_ID;

/* Почта: обычный Gmail-ящик с «паролем приложения». Бесплатно, до 500 писем в сутки. */
const MAIL_USER = process.env.MAIL_USER || '';
const MAIL_PASS = process.env.MAIL_PASS || '';
const MAIL_HOST = process.env.MAIL_HOST || 'smtp.gmail.com';
const MAIL_PORT = Number(process.env.MAIL_PORT || 465);
/* Ключ Brevo: отправка писем по HTTPS. Нужен там, где хостинг режет порты SMTP. */
const BREVO_KEY = process.env.BREVO_API_KEY || '';
/* Почтовый мостик на Google Apps Script: письма шлёт твой же Gmail по HTTPS.
   Работает из любой страны и через любой хостинг. */
const MAIL_HOOK_URL = process.env.MAIL_HOOK_URL || '';
const MAIL_HOOK_SECRET = process.env.MAIL_HOOK_SECRET || '';
/* SendPulse: российский сервис, HTTPS API, работает без VPN и без иностранного телефона */
const SP_ID = process.env.SENDPULSE_ID || '';
const SP_SECRET = process.env.SENDPULSE_SECRET || '';
const SP_FROM = process.env.SENDPULSE_FROM || '';
/* Mailopost и Rusender — российские сервисы, работают без VPN */
const MP_KEY = process.env.MAILOPOST_KEY || '';
const RS_KEY = process.env.RUSENDER_KEY || '';
const MAIL_ENABLED = !!((MAIL_USER && MAIL_PASS) || BREVO_KEY || MAIL_HOOK_URL || (SP_ID && SP_SECRET) || MP_KEY || RS_KEY);

/* Telegram-бот для подтверждения номера. Бесплатно и без лимитов. */
const TG_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TG_BOT_NAME = (process.env.TELEGRAM_BOT_USERNAME || '').replace('@', '');
const TG_ENABLED = !!(TG_TOKEN && TG_BOT_NAME);
const TG_SECRET = crypto.randomBytes(16).toString('hex');
const PUBLIC_URL = (process.env.RENDER_EXTERNAL_URL || process.env.PUBLIC_URL || '').replace(/\/$/, '');

/* ================= ПРАВИЛА ================= */
/* Денег у площадки нет: покупатель платит продавцу напрямую, банк в банк.
   Сервер лишь замораживает юзернейм на время сделки и передаёт его. */

const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';

/* Премиум теперь не за деньги, а за приглашённых друзей */
const PREMIUM = {
  days: Number(process.env.PREMIUM_DAYS || 30),
  slots: 5,
  freeSlots: 3,
  invites: Number(process.env.PREMIUM_INVITES || 3)
};

/* Сделка на бирже: сколько часов ждём оплату и через сколько дней
   после «Я перевёл» юзернейм переходит покупателю автоматически */
const DEAL = {
  payHours: Number(process.env.DEAL_PAY_HOURS || 24),
  confirmDays: Number(process.env.DEAL_CONFIRM_DAYS || 7)
};

/* Продавать юзернеймы можно только с доверием не ниже порога */
const SELL_MIN_TRUST = Number(process.env.SELL_MIN_TRUST || 60);

/* Лимит ботов на человека */
const BOTS_PER_USER = Number(process.env.BOTS_PER_USER || 3);

/* Сколько мегабайт разрешено на одно вложение. Всё хранится в базе:
   на бесплатном Neon это 500 МБ на всех, поэтому по умолчанию скромно. */
const MAX_MB = Number(process.env.MAX_UPLOAD_MB || 10);
const MAX_BYTES = Math.round(MAX_MB * 1.37 * 1024 * 1024);
const MEDIA_KEEP_DAYS = Number(process.env.MEDIA_KEEP_DAYS || 7);
/* Музыка в профиле. Лежит в базе, поэтому лимит осознанный. */
const MUSIC_MB = Number(process.env.MUSIC_MB || 12);

/* ================= ХРАНИЛИЩЕ ================= */

let db = {
  users: {},      // userId -> { id, phone, name, username, photo, banner, status, verified, dev, trust, requisites, createdAt }
  usernames: {},  // username -> { owner, main, forSale, price }
  chats: {},      // chatId -> { id, members:[a,b], service, msgs:[] }
  history: {},    // userId -> [операции кошелька]
  reports: [],    // жалобы
  verifyRequests: [],
  tokens: {},     // token -> userId
  codes: {},      // phone -> { code, expires }
  tgSessions: {}, // session -> { created, chatId, status, token, needsSetup }
  deals: {},      // dealId -> сделка на бирже
  stories: [],    // истории на 24 часа
  botTokens: {},  // token -> botId
  botUpdates: {}  // botId -> [входящие сообщения для бота]
};

/* Постоянное хранилище.
   На бесплатном Render диск стирается при каждом перезапуске и после сна,
   поэтому база живёт в Postgres, если задан DATABASE_URL.
   Без него — файл рядом с сервером (годится только для локальной разработки). */
const DATABASE_URL = process.env.DATABASE_URL || '';
let pgPool = null;

async function load() {
  if (DATABASE_URL) {
    try {
      const { Pool } = require('pg');
      pgPool = new Pool({
        connectionString: DATABASE_URL,
        ssl: { rejectUnauthorized: false },
        max: 3
      });
      await pgPool.query(
        'CREATE TABLE IF NOT EXISTS newchat_db (id int PRIMARY KEY, data jsonb, updated_at timestamptz)'
      );
      const r = await pgPool.query('SELECT data FROM newchat_db WHERE id = 1');
      if (r.rows.length && r.rows[0].data) {
        db = Object.assign(db, r.rows[0].data);
        ensureDbShape();
        console.log('База загружена из Postgres: ' + Object.keys(db.users).length + ' аккаунтов');
      } else {
        console.log('Postgres подключён, база пустая — первый запуск');
      }
      return;
    } catch (e) {
      console.error('Postgres недоступен:', e.message);
      pgPool = null;
      /* падаем на файл, чтобы сервер всё-таки поднялся */
    }
  }

  console.warn('=====================================================');
  console.warn('ВНИМАНИЕ: база хранится в ФАЙЛЕ, а не в Postgres.');
  console.warn('На Render файл стирается при каждом деплое и перезапуске —');
  console.warn('все аккаунты, чаты и карточки пропадут.');
  console.warn('Задайте переменную DATABASE_URL (строка подключения Neon).');
  console.warn('=====================================================');
  try {
    if (fs.existsSync(DATA_FILE)) {
      db = Object.assign(db, JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')));
      ensureDbShape();
      console.log('База загружена из файла');
    }
  } catch (e) {
    console.error('Не удалось прочитать базу:', e.message);
  }
}

function ensureDbShape() {
  db.users = db.users || {};
  db.usernames = db.usernames || {};
  db.tokens = db.tokens || {};
  db.chats = db.chats || {};
  db.reports = db.reports || [];
  db.history = db.history || {};
  db.tgSessions = db.tgSessions || {};
  db.deals = db.deals || {};
  db.stories = db.stories || [];
  db.botTokens = db.botTokens || {};
  db.botUpdates = db.botUpdates || {};
}

let saveTimer = null;
let saving = false;
let saveAgain = false;

async function writeNow() {
  if (saving) { saveAgain = true; return; }
  saving = true;
  try {
    if (pgPool) {
      await pgPool.query(
        `INSERT INTO newchat_db (id, data, updated_at) VALUES (1, $1, now())
         ON CONFLICT (id) DO UPDATE SET data = $1, updated_at = now()`,
        [JSON.stringify(db)]
      );
    } else {
      fs.writeFileSync(DATA_FILE, JSON.stringify(db));
    }
  } catch (e) {
    console.error('Не удалось сохранить базу:', e.message);
  } finally {
    saving = false;
    if (saveAgain) { saveAgain = false; writeNow(); }
  }
}

function save() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(writeNow, 300);
}

/* Render присылает SIGTERM перед сном — успеваем дописать всё на диск */
let bye = false;
async function shutdown() {
  if (bye) return;
  bye = true;
  clearTimeout(saveTimer);
  await writeNow();
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

const uid = () => crypto.randomBytes(9).toString('hex');
const now = () => Date.now();
const sockets = new Map(); // userId -> Set<ws>

/* ================= ОТПРАВКА SMS ================= */

async function sendSMS(phone10, code) {
  if (!SMS_ENABLED) return { sent: false, reason: 'not_configured' };

  const to = '7' + phone10;
  const text = `Newchat: код ${code}. Никому его не сообщайте.`;
  const url = 'https://sms.ru/sms/send?api_id=' + encodeURIComponent(SMSRU_API_ID) +
    '&to=' + to + '&msg=' + encodeURIComponent(text) + '&json=1';

  try {
    /* Если сервис не ответил за 10 секунд — не заставляем человека ждать */
    const res = await fetch(url, { signal: AbortSignal.timeout(10000) });
    const data = await res.json();

    if (data.status === 'OK') {
      const info = data.sms && data.sms[to];
      if (info && info.status === 'OK') {
        console.log(`SMS отправлено на +${to}`);
        return { sent: true };
      }
      console.error('SMS не доставлено:', info && info.status_text);
      return { sent: false, reason: (info && info.status_text) || 'Не удалось отправить' };
    }

    console.error('Ошибка SMS.ru:', data.status_text);
    return { sent: false, reason: data.status_text || 'Ошибка сервиса SMS' };
  } catch (e) {
    console.error('Сбой связи с SMS.ru:', e.message);
    return { sent: false, reason: 'Сервис SMS недоступен' };
  }
}

/* ================= ХЕЛПЕРЫ ================= */

function normPhone(p) {
  return String(p || '').replace(/\D/g, '').slice(-10);
}
function normUsername(u) {
  return String(u || '').trim().toLowerCase().replace(/[^a-z0-9_]/g, '').slice(0, 20);
}
function isOnline(userId) {
  const set = sockets.get(userId);
  if (!set) return false;
  for (const ws of set) if (ws.readyState === 1) return true;
  return false;
}
function publicUser(u) {
  if (!u) return null;
  return {
    id: u.id, name: u.name, username: u.username,
    cover: u.cover, ava: u.ava, status: u.status,
    photo: u.photo || null, banner: typeof u.banner === 'number' ? u.banner : 0,
    bot: !!u.isBot, anon: !!u.anon,
    phoneOk: !!u.phone,
    anonPhone: u.anonPhone ? ('+0 ' + u.anonPhone.slice(1, 4) + ' •••-••-' + u.anonPhone.slice(-2)) : '',
    hasKey: !!u.pubkey,
    codev: isCodev(u),
    bio: u.bio || '',
    gifts: u.isBot ? [] : myGifts(u.id),
    music: (u.anon || u.isBot) ? null : (u.music || null),
    premium: isPremium(u),
    premiumUntil: isPremium(u) ? (u.premiumUntil || 0) : 0,
    coverImg: u.coverImg || '',
    trust: u.isBot ? undefined : ((isDev(u) || isCodev(u)) ? 100 : (u.trust || 0)),
    reportsOn: u.isBot ? 0 : db.reports.filter(r => r.against === u.id).length,
    ageDays: Math.max(0, Math.floor((now() - (u.createdAt || now())) / 86400e3)),
    dealsDone: u.isBot ? 0 : Object.values(db.deals).filter(d => d.seller === u.id && d.status === 'done').length,
    online: u.anon ? undefined : (u.isBot ? true : isOnline(u.id)),
    lastSeen: u.anon ? 0 : (u.lastSeen || 0),
    verified: !!u.verified, dev: isDev(u)
  };
}
function userByToken(token) {
  const id = db.tokens[token];
  return id ? db.users[id] : null;
}
/* Серверы для звонков. STUN подсказывает адрес, TURN пропускает звук
   через себя, когда прямое соединение не проходит — VPN, мобильный NAT. */
function iceServers() {
  const list = [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' }
  ];
  if (process.env.TURN_URL && process.env.TURN_USER) {
    list.push({
      urls: process.env.TURN_URL.split(',').map(v => v.trim()).filter(Boolean),
      username: process.env.TURN_USER,
      credential: process.env.TURN_PASS || ''
    });
  } else {
    /* Бесплатный публичный ретранслятор — на первое время */
    list.push({
      urls: [
        'turn:openrelay.metered.ca:80',
        'turn:openrelay.metered.ca:443',
        'turn:openrelay.metered.ca:443?transport=tcp'
      ],
      username: 'openrelayproject',
      credential: 'openrelayproject'
    });
  }
  return list;
}

function isDev(user) {
  return !!(user && (user.dev || DEV_USERNAMES.includes(user.username || '')));
}

function isCodev(user) {
  return !!(user && CODEV_USERNAMES.includes(user.username || ''));
}
function isPremium(user) {
  if (isDev(user)) return true; /* у разработчиков всегда премиум */
  return !!(user.premiumUntil && user.premiumUntil > now());
}
function slotLimit(user) {
  if (isDev(user)) return 999;
  return isPremium(user) ? PREMIUM.slots : PREMIUM.freeSlots;
}
function chatIdFor(a, b) {
  return [a, b].sort().join(':');
}
function chatView(c, userId) {
  let peer, type = 'dm';
  if (c.service) {
    type = 'service';
    peer = { id: 'service', name: 'Newchat', username: 'newchat', verified: true };
  } else if (c.type === 'channel') {
    type = 'channel';
    peer = {
      id: c.id, name: c.title, username: c.uname || '',
      photo: c.photo || null, channel: true
    };
  } else {
    const otherId = c.members.find(m => m !== userId) || c.members[0];
    peer = publicUser(db.users[otherId]);
  }
  const me = db.users[userId] || {};
  const myReadAt = (me.reads || {})[c.id] || 0;
  let peerReadAt = 0;
  if (type === 'dm' && peer && peer.id) {
    const other = db.users[peer.id];
    peerReadAt = ((other || {}).reads || {})[c.id] || 0;
  }
  return {
    id: c.id,
    type,
    peerReadAt,
    unread: c.msgs.filter(m => m.from !== userId && !m.deleted && m.time > myReadAt && m.time > ((c.clearedAt || {})[userId] || 0)).length,
    muted: !!(me.muted || {})[c.id],
    ttl: c.ttl || 0,
    secret: !!c.secret,
    secretOffer: (c.secretOffer && c.secretOffer.from !== userId) ? c.secretOffer : null,
    secretPending: !!(c.secretOffer && c.secretOffer.from === userId),
    wp: ((c.wpFor || {})[userId]) || c.wp || '',
    wpOffer: (c.wpOffer && c.wpOffer.from !== userId) ? c.wpOffer : null,
    blocked: type === 'dm' && peer && peer.id ? !!(me.blocked || {})[peer.id] : false,
    service: !!c.service,
    owner: c.owner || null,
    mine: c.owner === userId,
    subs: c.type === 'channel' ? c.members.length : undefined,
    canWrite: type === 'dm' || (type === 'channel' && c.owner === userId),
    peer,
    msgs: c.msgs.filter(m => m.time > ((c.clearedAt || {})[userId] || 0)).slice(-200).map(m => ({
      id: m.id, text: m.text, time: m.time,
      req: m.deleted ? null : (m.req || null),
      enc: m.deleted ? null : (m.enc || null),
      iv: m.deleted ? null : (m.iv || null),
      media: m.deleted ? null : (m.media || null),
      reply: m.deleted ? null : (m.reply || null),
      fwd: m.deleted ? null : (m.fwd || null),
      reactions: m.reactions || null,
      out: m.from === userId, deleted: !!m.deleted,
      from: c.type === 'channel' ? undefined : m.from
    }))
  };
}
function userChats(userId) {
  return Object.values(db.chats)
    .filter(c => c.members.includes(userId))
    .filter(c => !(c.hiddenFor || []).includes(userId))
    .map(c => chatView(c, userId))
    .sort((a, b) => {
      const la = a.msgs[a.msgs.length - 1], lb = b.msgs[b.msgs.length - 1];
      return (lb ? lb.time : 0) - (la ? la.time : 0);
    });
}
function myUsernames(userId) {
  return Object.entries(db.usernames)
    .filter(([, v]) => v.owner === userId)
    .map(([u, v]) => ({ u, main: !!v.main, forSale: !!v.forSale, price: v.price || 0 }));
}
function sellerStats(userId) {
  const deals = Object.values(db.deals).filter(d => d.seller === userId && d.status === 'done');
  return { deals: deals.length };
}
function giftMarket(forUser) {
  return (db.gifts || [])
    .filter(g => g.forSale && !g.frozen)
    .map(g => {
      const t = GIFT_TYPES[g.type] || (db.giftTypes || {})[g.type] || {};
      const sales = (db.giftSales || []).filter(x => x.type === g.type);
      const last = sales.length ? sales[sales.length - 1] : null;
      return {
        id: g.id, type: g.type, num: g.num,
        name: t.name || g.type,
        total: t.total || 0,
        rarity: t.rarity || '',
        rarityNum: t.rarityNum || 1,
        price: g.price || 0,
        mine: g.owner === forUser,
        lastPrice: last ? last.price : 0,
        salesCount: sales.length,
        seller: Object.assign(publicUser(db.users[g.owner]) || {}, {
          trust: (db.users[g.owner] || {}).trust || 0,
          stats: sellerStats(g.owner)
        })
      };
    });
}

function marketList(forUser) {
  return Object.entries(db.usernames)
    .filter(([, v]) => v.forSale && !v.frozen)
    .map(([u, v]) => ({
      u, price: v.price || 0,
      mine: v.owner === forUser,
      seller: Object.assign(publicUser(db.users[v.owner]) || {}, {
        trust: (db.users[v.owner] || {}).trust || 0,
        stats: sellerStats(v.owner)
      })
    }));
}
function dealView(d, userId) {
  const seller = db.users[d.seller], buyer = db.users[d.buyer];
  return {
    id: d.id, username: d.username, price: d.price, status: d.status,
    role: d.seller === userId ? 'seller' : 'buyer',
    seller: publicUser(seller), buyer: publicUser(buyer),
    requisites: d.requisites,
    createdAt: d.createdAt, paidAt: d.paidAt || 0,
    payDeadline: d.createdAt + DEAL.payHours * 3600e3,
    autoAt: d.paidAt ? d.paidAt + DEAL.confirmDays * 86400e3 : 0
  };
}
function myDeals(userId) {
  return Object.values(db.deals)
    .filter(d => d.seller === userId || d.buyer === userId)
    .sort((a, b) => b.createdAt - a.createdAt)
    .slice(0, 50)
    .map(d => dealView(d, userId));
}
function myBots(userId) {
  return Object.values(db.users)
    .filter(u => u.isBot && u.owner === userId)
    .map(b => ({
      id: b.id, name: b.name, username: b.username,
      token: Object.keys(db.botTokens).find(t => db.botTokens[t] === b.id)
    }));
}
function fullState(user) {
  try { grantDevCoin(user); } catch (e) {}
  try { ensureAnonPhone(user); } catch (e) {}
  try { grantAegis(user); } catch (e) {}
  return {
    user: Object.assign(publicUser(user), {
      phone: user.anonPhone
        ? ('+0 ' + user.anonPhone.slice(1, 4) + ' ' + user.anonPhone.slice(4, 7) + '-' + user.anonPhone.slice(7, 9) + '-' + user.anonPhone.slice(9, 11))
        : user.phone,
      anonPhone: user.anonPhone || '',
      trust: (isDev(user) || isCodev(user)) ? 100 : user.trust,
      premium: isPremium(user),
      premiumUntil: user.premiumUntil || 0,
      slots: slotLimit(user),
      premiumDays: PREMIUM.days,
      anonMode: !!user.anon,
      email: user.email || '',
      phoneOk: !!user.phone,
      codev: isCodev(user),
      bio: user.bio || '',
      gifts: myGifts(user.id),
      music: user.music || null,
      ringtone: user.ringtone ? { name: user.ringtone.name } : null,
      msgSound: user.msgSound ? { name: user.msgSound.name } : null,
      coverImg: user.coverImg || '',
      icon: user.icon || '',
      invites: user.inviteCount || 0,
      invitesNeeded: PREMIUM.invites,
      requisites: user.requisites || null
    }),
    chats: userChats(user.id),
    usernames: myUsernames(user.id),
    market: marketList(user.id),
    giftMarket: giftMarket(user.id),
    deals: myDeals(user.id),
    bots: myBots(user.id),
    stories: storiesFeed(user),
    history: db.history[user.id] || [],
    reports: db.reports.filter(r => r.from === user.id).length
  };
}

/* Системное сообщение от Newchat */
function serviceMessage(userId, text) {
  const id = 'service:' + userId;
  if (!db.chats[id]) {
    db.chats[id] = { id, members: [userId], service: true, msgs: [] };
  }
  const msg = { id: uid(), from: 'service', text, time: now() };
  db.chats[id].msgs.push(msg);
  save();
  push(userId, { type: 'message', chatId: id, message: { id: msg.id, text, time: msg.time, out: false } });
}

/* ================= TELEGRAM-БОТ ================= */

async function tg(method, params) {
  if (!TG_ENABLED) return null;
  try {
    const res = await fetch(`https://api.telegram.org/bot${TG_TOKEN}/${method}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(params || {}),
      signal: AbortSignal.timeout(15000)
    });
    return await res.json();
  } catch (e) {
    console.error('Telegram ' + method + ':', e.message);
    return null;
  }
}

/* Выдаём токен входа по подтверждённому номеру */
function loginByPhone(phone10, tgId) {
  let user = Object.values(db.users).find(u => u.phone === phone10);
  const token = crypto.randomBytes(24).toString('hex');

  if (!user) {
    const id = uid();
    user = {
      id, phone: phone10, name: '', username: '',
      cover: 0, ava: 0, status: '',
      verified: false, dev: false,
      trust: 100, createdAt: now(),
      telegramId: tgId || null
    };
    db.users[id] = user;
  } else if (tgId) {
    user.telegramId = tgId;
  }

  db.tokens[token] = user.id;
  save();
  return { token, user };
}

/* Обработка сообщений, которые присылает бот */
async function handleTelegramUpdate(update) {
  const msg = update && update.message;
  if (!msg || !msg.chat) return;

  const chatId = msg.chat.id;
  const text = msg.text || '';

  /* Шаг 1: человек нажал «Старт» по ссылке из приложения */
  if (text.startsWith('/start')) {
    const session = text.split(' ')[1];

    if (session && db.tgSessions[session]) {
      db.tgSessions[session].chatId = chatId;
      save();
      await tg('sendMessage', {
        chat_id: chatId,
        text: 'Чтобы войти в Newchat, подтвердите свой номер телефона.\n\nНажмите кнопку ниже — Telegram передаст номер сам, вводить ничего не нужно.',
        reply_markup: {
          keyboard: [[{ text: '📱 Подтвердить номер', request_contact: true }]],
          resize_keyboard: true,
          one_time_keyboard: true
        }
      });
    } else {
      await tg('sendMessage', {
        chat_id: chatId,
        text: 'Это бот входа в Newchat.\n\nОткройте приложение и нажмите «Войти через Telegram» — я пришлю кнопку подтверждения.'
      });
    }
    return;
  }

  /* Шаг 2: человек поделился контактом */
  if (msg.contact) {
    /* Важно: принимаем только собственный контакт, а не пересланный чужой */
    if (msg.contact.user_id !== msg.from.id) {
      await tg('sendMessage', {
        chat_id: chatId,
        text: 'Это чужой контакт. Нажмите кнопку «Подтвердить номер» — она передаёт именно ваш.'
      });
      return;
    }

    const session = Object.keys(db.tgSessions)
      .find(s => db.tgSessions[s].chatId === chatId && db.tgSessions[s].status === 'pending');

    /* Привязка телефона к уже существующему аккаунту (вход был по почте) */
    if (session && db.tgSessions[session].linkFor) {
      const target = db.users[db.tgSessions[session].linkFor];
      const phone = normPhone(msg.contact.phone_number);
      if (!target) {
        await tg('sendMessage', { chat_id: chatId, text: 'Аккаунт не найден. Откройте приложение заново.', reply_markup: { remove_keyboard: true } });
        return;
      }
      const busy = Object.values(db.users).find(u => u.phone === phone && u.id !== target.id);
      if (busy) {
        db.tgSessions[session].status = 'error';
        db.tgSessions[session].error = 'Этот номер уже привязан к другому аккаунту';
        save();
        await tg('sendMessage', { chat_id: chatId, text: '❌ Этот номер уже привязан к другому аккаунту Newchat.', reply_markup: { remove_keyboard: true } });
        return;
      }
      target.phone = phone;
      target.tgId = msg.from.id;
      db.tgSessions[session].status = 'ok';
      db.tgSessions[session].linked = true;
      save();
      push(target.id, { type: 'state' });
      serviceMessage(target.id, 'Телефон подтверждён. Биржа юзернеймов открыта.');
      await tg('sendMessage', {
        chat_id: chatId,
        text: '✅ Номер привязан к вашему аккаунту. Биржа открыта — возвращайтесь в приложение.',
        reply_markup: { remove_keyboard: true }
      });
      return;
    }

    if (!session) {
      await tg('sendMessage', {
        chat_id: chatId,
        text: 'Срок входа истёк. Откройте приложение и попробуйте снова.',
        reply_markup: { remove_keyboard: true }
      });
      return;
    }

    const phone = normPhone(msg.contact.phone_number);
    const { token, user } = loginByPhone(phone, msg.from.id);

    db.tgSessions[session].status = 'ok';
    db.tgSessions[session].token = token;
    db.tgSessions[session].needsSetup = !user.username;
    save();

    await tg('sendMessage', {
      chat_id: chatId,
      text: '✅ Номер подтверждён. Возвращайтесь в приложение — вход выполнен.',
      reply_markup: { remove_keyboard: true }
    });
  }
}

/* Получение обновлений: вебхук на хостинге, опрос при локальном запуске */
async function startTelegram() {
  if (!TG_ENABLED) {
    console.log('Telegram-бот не настроен');
    return;
  }

  const me = await tg('getMe');
  if (!me || !me.ok) {
    console.error('Неверный токен Telegram-бота');
    return;
  }
  console.log('Telegram-бот подключён: @' + me.result.username);

  if (PUBLIC_URL) {
    const r = await tg('setWebhook', {
      url: PUBLIC_URL + '/telegram/webhook',
      secret_token: TG_SECRET,
      allowed_updates: ['message'],
      drop_pending_updates: true
    });
    console.log('Вебхук установлен:', r && r.ok);
  } else {
    await tg('deleteWebhook', { drop_pending_updates: true });
    console.log('Режим опроса Telegram');
    let offset = 0;
    (async function poll() {
      for (;;) {
        try {
          const r = await tg('getUpdates', { offset, timeout: 25, allowed_updates: ['message'] });
          if (r && r.ok) {
            for (const u of r.result) {
              offset = u.update_id + 1;
              await handleTelegramUpdate(u);
            }
          }
        } catch (e) { }
        await new Promise(r => setTimeout(r, 1000));
      }
    })();
  }
}

/* ================= СДЕЛКИ НА БИРЖЕ ================= */
/* Как на Авито, только в залоге лежит не деньги, а сам юзернейм:
   1. Покупатель жмёт «Купить» — юзернейм замораживается, видны реквизиты продавца.
   2. Покупатель переводит деньги напрямую (банк в банк) и жмёт «Я перевёл».
   3. Продавец жмёт «Деньги пришли» — юзернейм переходит покупателю.
   4. Продавец молчит 7 дней — юзернейм переходит автоматически.
   5. Продавец жмёт «Денег нет» — спор, разбирает модерация. */

function finishDeal(deal, how) {
  const rec = db.usernames[deal.username];
  const seller = db.users[deal.seller];
  const buyer = db.users[deal.buyer];

  if (how === 'done' && deal.kind === 'gift') {
    /* Карточка меняет владельца, цена уходит в историю рынка */
    const g = (db.gifts || []).find(x => x.id === deal.giftId);
    if (g) {
      g.owner = deal.buyer;
      g.forSale = false;
      g.frozen = null;
      g.price = 0;
      db.giftSales = db.giftSales || [];
      db.giftSales.push({ type: g.type, price: deal.price, time: now() });
    }
  }
  if (how === 'done') {
    if (rec) {
      const wasMain = rec.main;
      rec.owner = deal.buyer;
      rec.forSale = false;
      rec.frozen = null;
      rec.price = 0;
      rec.main = false;
      if (wasMain && seller) {
        /* Продан основной — запасной становится новым лицом продавца */
        const spare = Object.entries(db.usernames).find(([un, v]) =>
          v.owner === seller.id && !v.channel && !v.frozen);
        if (spare) {
          spare[1].main = true;
          seller.username = spare[0];
          serviceMessage(seller.id, `Ваш основной юзернейм теперь @${spare[0]}.`);
        }
      }
    }
    deal.status = 'done';
    deal.doneAt = now();
    db.history[deal.buyer] = db.history[deal.buyer] || [];
    db.history[deal.buyer].unshift({ amt: -deal.price, title: 'Покупка @' + deal.username, sub: 'оплата напрямую продавцу', time: now() });
    db.history[deal.seller] = db.history[deal.seller] || [];
    db.history[deal.seller].unshift({ amt: deal.price, title: 'Продажа @' + deal.username, sub: 'деньги пришли вам напрямую', time: now() });
    if (buyer) serviceMessage(buyer.id, `Сделка завершена: @${deal.username} теперь ваш.`);
    if (seller) serviceMessage(seller.id, `Сделка завершена: @${deal.username} передан покупателю.`);
  } else {
    if (rec && rec.frozen === deal.id) {
      rec.frozen = null;
      rec.forSale = true; /* лот возвращается на биржу */
    }
    if (deal.kind === 'gift') {
      const g = (db.gifts || []).find(x => x.id === deal.giftId);
      if (g) { g.frozen = null; g.forSale = true; }
    }
    deal.status = 'cancelled';
    deal.doneAt = now();
    if (buyer) serviceMessage(buyer.id, `Сделка по @${deal.username} отменена.`);
    if (seller) serviceMessage(seller.id, `Сделка по @${deal.username} отменена, лот снова на бирже.`);
  }
  save();
  push(deal.buyer, { type: 'state' });
  push(deal.seller, { type: 'state' });
}

/* Часовой сделок: отменяет неоплаченные и завершает подтверждённые времени */
setInterval(() => {
  /* Видео и кружки старше недели вычищаем — база бесплатная, место не резиновое */
  let vidCleaned = false;
  for (const c of Object.values(db.chats)) {
    for (const m of c.msgs) {
      if (m.media && ['video', 'circle', 'file'].includes(m.media.kind) && now() - m.time > MEDIA_KEEP_DAYS * 86400e3) {
        m.media = null;
        m.text = m.text || ('Вложение удалено (хранится ' + MEDIA_KEEP_DAYS + ' дн.)');
        m.expired = true;
        vidCleaned = true;
      }
    }
  }
  if (vidCleaned) save();

  /* Автоудаление: помечаем старые сообщения в чатах с таймером */
  let ttlChanged = false;
  for (const c of Object.values(db.chats)) {
    if (!c.ttl) continue;
    for (const m of c.msgs) {
      if (!m.deleted && now() - m.time > c.ttl) { m.deleted = true; ttlChanged = true; }
    }
  }
  if (ttlChanged) save();

  const before = db.stories ? db.stories.length : 0;
  if (db.stories) {
    db.stories = db.stories.filter(st => now() - st.time < 86400e3);
    if (db.stories.length !== before) save();
  }
  for (const d of Object.values(db.deals)) {
    if (d.status === 'pay' && now() - d.createdAt > DEAL.payHours * 3600e3) {
      finishDeal(d, 'cancelled');
    }
    if (d.status === 'paid' && now() - d.paidAt > DEAL.confirmDays * 86400e3) {
      /* Продавец не вышел на связь — защищаем покупателя, который оплатил */
      finishDeal(d, 'done');
    }
  }
}, Number(process.env.DEAL_TICK_MS || 5 * 60e3));

/* ================= WEBSOCKET ================= */

function push(userId, payload) {
  const set = sockets.get(userId);
  if (!set) return;
  const data = JSON.stringify(payload);
  for (const ws of set) {
    if (ws.readyState === 1) ws.send(data);
  }
}

/* ================= HTTP ================= */

function send(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Cache-Control': 'no-store'
  });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve) => {
    let data = '';
    req.on('data', c => {
      data += c;
      if (data.length > Math.max(MAX_BYTES, MUSIC_MB * 1.4 * 1024 * 1024) * 1.6) req.destroy();
    });
    req.on('end', () => {
      try { resolve(data ? JSON.parse(data) : {}); }
      catch { resolve({}); }
    });
  });
}

const routes = {};
const route = (method, url, fn) => { routes[method + ' ' + url] = fn; };

/* ---------- Авторизация ---------- */

route('POST', '/api/auth/request', async (req, res, body) => {
  const phone = normPhone(body.phone);
  if (phone.length !== 10) return send(res, 400, { error: 'Введите номер из 10 цифр' });

  const prev = db.codes[phone];

  /* Не чаще одного кода в минуту на номер */
  if (prev && prev.sentAt && now() - prev.sentAt < 60e3) {
    const wait = Math.ceil((60e3 - (now() - prev.sentAt)) / 1000);
    return send(res, 429, { error: `Повторный код можно запросить через ${wait} сек.` });
  }

  /* Не больше 5 кодов на номер в час — защита от перебора и слива денег на SMS */
  const hourAgo = now() - 3600e3;
  const recent = (prev && prev.log ? prev.log : []).filter(t => t > hourAgo);
  if (recent.length >= 5) {
    return send(res, 429, { error: 'Слишком много запросов. Попробуйте через час.' });
  }

  const code = String(Math.floor(10000 + Math.random() * 90000));
  db.codes[phone] = {
    code,
    expires: now() + 10 * 60 * 1000,
    sentAt: now(),
    attempts: 0,
    log: recent.concat(now())
  };
  save();

  if (SMS_ENABLED) {
    const result = await sendSMS(phone, code);
    if (!result.sent) {
      delete db.codes[phone];
      save();
      return send(res, 502, { error: result.reason || 'Не удалось отправить SMS' });
    }
    return send(res, 200, { ok: true });
  }

  /* SMS не подключены — показываем код на экране */
  console.log(`Код для +7${phone}: ${code}`);
  send(res, 200, { ok: true, devCode: code });
});

function normEmail(v) {
  return String(v || '').trim().toLowerCase().slice(0, 80);
}
function validEmail(v) {
  return /^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$/.test(v);
}

/* Отправка письма напрямую по SMTP — без внешних библиотек,
   чтобы почта работала сразу после заливки, без установки пакетов. */
function smtpSend(opts) {
  return new Promise((resolve, reject) => {
    const tls = require('tls');
    const net = require('net');
    const starttls = opts.port !== 465;

    let sock = starttls
      ? net.connect({ host: opts.host, port: opts.port })
      : tls.connect({ host: opts.host, port: opts.port, servername: opts.host });

    let buf = '';
    const queue = [];      /* кто ждёт ответа */
    const pending = [];    /* ответы, пришедшие раньше ожидания */
    let done = false;

    function fail(e) {
      if (done) return;
      done = true;
      try { sock.destroy(); } catch (x) {}
      const msg = (e && (e.message || e.code)) || String(e) || 'соединение закрыто';
      reject(new Error(msg));
    }
    function onData(chunk) {
      buf += chunk;
      let nl;
      while ((nl = buf.indexOf('\r\n')) !== -1) {
        const line = buf.slice(0, nl);
        buf = buf.slice(nl + 2);
        if (/^\d{3}-/.test(line)) continue;
        const answer = { code: line.slice(0, 3), line };
        const w = queue.shift();
        if (w) w(answer); else pending.push(answer);
      }
    }
    function bind(s) {
      s.setEncoding('utf8');
      s.setTimeout(20000);
      s.on('data', onData);
      s.on('timeout', () => fail(new Error('таймаут — порт ' + opts.port + ' закрыт хостингом')));
      s.on('error', fail);
    }
    function reply() {
      if (pending.length) return Promise.resolve(pending.shift());
      return new Promise(r => queue.push(r));
    }
    async function expect(codes, what) {
      const r = await reply();
      if (!codes.includes(r.code)) throw new Error(what + ': ' + r.line);
      return r;
    }
    function say(t) { sock.write(t + '\r\n'); }

    bind(sock);

    const start = async () => {
      try {
        await expect(['220'], 'приветствие');
        say('EHLO newchat');
        await expect(['250'], 'EHLO');

        if (starttls) {
          say('STARTTLS');
          await expect(['220'], 'STARTTLS');
          const plain = sock;
          plain.removeAllListeners('data');
          plain.removeAllListeners('error');
          plain.removeAllListeners('timeout');
          buf = '';
          pending.length = 0;
          sock = tls.connect({ socket: plain, servername: opts.host });
          bind(sock);
          await new Promise((res, rej) => { sock.once('secureConnect', res); sock.once('error', rej); });
          say('EHLO newchat');
          await expect(['250'], 'EHLO после STARTTLS');
        }

        say('AUTH LOGIN');
        await expect(['334'], 'AUTH');
        say(Buffer.from(opts.user).toString('base64'));
        await expect(['334'], 'логин');
        say(Buffer.from(opts.pass).toString('base64'));
        await expect(['235'], 'пароль отклонён (нужен пароль приложения, 16 символов без пробелов)');
        say('MAIL FROM:<' + opts.user + '>');
        await expect(['250'], 'MAIL FROM');
        say('RCPT TO:<' + opts.to + '>');
        await expect(['250', '251'], 'RCPT TO');
        say('DATA');
        await expect(['354'], 'DATA');
        sock.write(opts.message + '\r\n.\r\n');
        await expect(['250'], 'отправка');
        say('QUIT');
        done = true;
        sock.end();
        resolve(true);
      } catch (e) { fail(e); }
    };

    if (starttls) sock.once('connect', start);
    else sock.once('secureConnect', start);
  });
}

function brevoSend(to, subject, html) {
  return new Promise((resolve, reject) => {
    const https = require('https');
    const payload = JSON.stringify({
      sender: { name: 'Newchat', email: MAIL_USER || 'noreply@newchat.app' },
      to: [{ email: to }],
      subject, htmlContent: html
    });
    const req = https.request({
      hostname: 'api.brevo.com', path: '/v3/smtp/email', method: 'POST',
      headers: {
        'api-key': BREVO_KEY,
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(payload)
      }
    }, res => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) resolve(true);
        else reject(new Error('Brevo ' + res.statusCode + ': ' + body.slice(0, 200)));
      });
    });
    req.setTimeout(15000, () => { req.destroy(); reject(new Error('Brevo: таймаут')); });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

function hookSend(to, subject, html) {
  return new Promise((resolve, reject) => {
    const https = require('https');
    const { URL } = require('url');
    const u = new URL(MAIL_HOOK_URL);
    const payload = JSON.stringify({ secret: MAIL_HOOK_SECRET, to, subject, html });
    const go = (host, path, redirects) => {
      const req = https.request({ hostname: host, path, method: 'POST',
        headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) }
      }, res => {
        /* Apps Script отвечает через редирект — идём за ним */
        if ([301, 302, 307].includes(res.statusCode) && res.headers.location && redirects > 0) {
          const r = new URL(res.headers.location);
          const req2 = https.request({ hostname: r.hostname, path: r.pathname + r.search, method: 'GET' }, res2 => {
            let b = '';
            res2.on('data', d => b += d);
            res2.on('end', () => b.includes('"ok"') || b.includes('ok') ? resolve(true) : reject(new Error('мостик: ' + b.slice(0, 120))));
          });
          req2.on('error', reject);
          req2.end();
          return;
        }
        let b = '';
        res.on('data', d => b += d);
        res.on('end', () => {
          if (res.statusCode >= 200 && res.statusCode < 300 && (b.includes('ok') || b === '')) resolve(true);
          else reject(new Error('мостик ' + res.statusCode + ': ' + b.slice(0, 120)));
        });
      });
      req.setTimeout(15000, () => { req.destroy(); reject(new Error('мостик: таймаут')); });
      req.on('error', reject);
      req.write(payload);
      req.end();
    };
    go(u.hostname, u.pathname + u.search, 2);
  });
}

function httpsJson(host, path, method, headers, bodyObj) {
  return new Promise((resolve, reject) => {
    const https = require('https');
    const payload = bodyObj ? JSON.stringify(bodyObj) : null;
    const h = Object.assign({ 'content-type': 'application/json' }, headers || {});
    if (payload) h['content-length'] = Buffer.byteLength(payload);
    const req = https.request({ hostname: host, path, method, headers: h }, res => {
      let b = '';
      res.on('data', d => b += d);
      res.on('end', () => {
        let parsed = null;
        try { parsed = JSON.parse(b); } catch (e) {}
        if (res.statusCode >= 200 && res.statusCode < 300) resolve(parsed || {});
        else reject(new Error(res.statusCode + ': ' + b.slice(0, 160)));
      });
    });
    req.setTimeout(15000, () => { req.destroy(); reject(new Error('таймаут')); });
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

let spToken = { value: '', until: 0 };
async function sendPulse(to, subject, html) {
  if (!spToken.value || spToken.until < now()) {
    const r = await httpsJson('api.sendpulse.com', '/oauth/access_token', 'POST', null, {
      grant_type: 'client_credentials', client_id: SP_ID, client_secret: SP_SECRET
    });
    if (!r.access_token) throw new Error('не выдан токен');
    spToken = { value: r.access_token, until: now() + (r.expires_in || 3600) * 1000 - 60000 };
  }
  await httpsJson('api.sendpulse.com', '/smtp/emails', 'POST',
    { authorization: 'Bearer ' + spToken.value },
    { email: {
        html: Buffer.from(html, 'utf8').toString('base64'),
        text: 'Ваш код для входа в Newchat',
        subject,
        from: { name: 'Newchat', email: MAIL_USER || SP_FROM },
        to: [{ email: to }]
    } });
  return true;
}

async function mailopost(to, subject, html) {
  await httpsJson('api.mailopost.ru', '/v1/email/messages', 'POST',
    { authorization: 'Bearer ' + MP_KEY },
    { from_email: MAIL_USER, from_name: 'Newchat', to, subject, html, payment: 'credit' });
  return true;
}

async function rusender(to, subject, html) {
  /* Хост api.beta.* больше не отвечает — рабочий адрес api.rusender.ru.
     Новые ключи (rs_ck_v1_...) идут через Bearer и id ключа отправки. */
  const body = { mail: {
    to: { email: to },
    from: { email: MAIL_USER, name: 'Newchat' },
    subject, html
  } };
  const isNew = /^rs_ck_v1_/.test(RS_KEY);
  const keyId = process.env.RUSENDER_KEY_ID || '';
  if (isNew) {
    if (!keyId) throw new Error('для ключа rs_ck_v1_ нужна переменная RUSENDER_KEY_ID');
    await httpsJson('api.rusender.ru', '/api/v1/external-mails/send/' + keyId, 'POST',
      { authorization: 'Bearer ' + RS_KEY }, body);
  } else {
    await httpsJson('api.rusender.ru', '/api/v1/external-mails/send', 'POST',
      { 'X-Api-Key': RS_KEY }, body);
  }
  return true;
}

async function sendMail(to, code) {
  if (!MAIL_ENABLED) return { sent: false, reason: 'not_configured' };
  const html = '<div style="font-family:Arial,Helvetica,sans-serif;max-width:420px;margin:0 auto;padding:28px 24px;background:#EEF0F5;border-radius:18px">' +
    '<div style="text-align:center;font-size:22px;font-weight:800;color:#17181D;margin-bottom:6px">Newchat</div>' +
    '<div style="text-align:center;font-size:13px;color:#787A86;margin-bottom:22px">Мессенджер, где скамеры отвечают по закону</div>' +
    '<div style="background:#fff;border-radius:14px;padding:22px;text-align:center">' +
    '<div style="font-size:13px;color:#787A86;margin-bottom:10px">Ваш код для входа</div>' +
    '<div style="font-size:36px;font-weight:800;letter-spacing:8px;color:#6C5CE7">' + code + '</div>' +
    '<div style="font-size:12px;color:#787A86;margin-top:12px">Действует 10 минут</div></div>' +
    '<div style="font-size:11px;color:#9A9CA8;text-align:center;margin-top:18px">Если вы не запрашивали вход, просто удалите это письмо.</div></div>';

  const message = [
    'From: Newchat <' + MAIL_USER + '>',
    'To: ' + to,
    'Subject: =?UTF-8?B?' + Buffer.from(code + ' - kod vhoda v Newchat').toString('base64') + '?=',
    'MIME-Version: 1.0',
    'Content-Type: text/html; charset=UTF-8',
    'Content-Transfer-Encoding: base64',
    '',
    Buffer.from(html, 'utf8').toString('base64').replace(/(.{76})/g, '$1\r\n')
  ].join('\r\n');

  /* Российские сервисы — работают без VPN */
  const errs = [];
  if (MP_KEY) {
    try { await mailopost(to, code + ' — код входа в Newchat', html); return { sent: true }; }
    catch (e) { console.error('Почта (Mailopost):', e.message); errs.push('Mailopost → ' + e.message); }
  }
  if (RS_KEY) {
    try { await rusender(to, code + ' — код входа в Newchat', html); return { sent: true }; }
    catch (e) { console.error('Почта (Rusender):', e.message); errs.push('Rusender → ' + e.message); }
  }

  /* SendPulse — работает из России по обычному HTTPS */
  if (SP_ID && SP_SECRET) {
    try {
      await sendPulse(to, code + ' — код входа в Newchat', html);
      return { sent: true };
    } catch (e) {
      console.error('Почта (SendPulse):', e.message); errs.push('SendPulse → ' + e.message);
      if (!MAIL_HOOK_URL && !BREVO_KEY && !(MAIL_USER && MAIL_PASS)) return { sent: false, reason: e.message };
    }
  }

  /* Мостик через Google — самый надёжный путь из России */
  if (MAIL_HOOK_URL) {
    try {
      await hookSend(to, code + ' — код входа в Newchat', html);
      return { sent: true };
    } catch (e) {
      console.error('Почта (мостик):', e.message); errs.push('мостик → ' + e.message);
      if (!BREVO_KEY && !(MAIL_USER && MAIL_PASS)) return { sent: false, reason: e.message };
    }
  }

  /* Сначала HTTPS-путь: его хостинги не блокируют */
  if (BREVO_KEY) {
    try {
      await brevoSend(to, code + ' — код входа в Newchat', html);
      return { sent: true };
    } catch (e) {
      console.error('Почта (Brevo):', e.message); errs.push('Brevo → ' + e.message);
      if (!MAIL_USER || !MAIL_PASS) return { sent: false, reason: e.message };
    }
  }

  /* Многие хостинги режут 465 — если не вышло, пробуем 587 через STARTTLS */
  const ports = MAIL_PORT === 465 ? [465, 587] : [MAIL_PORT];
  let lastErr = '';
  for (const port of ports) {
    try {
      await smtpSend({ host: MAIL_HOST, port, user: MAIL_USER, pass: MAIL_PASS, to, message });
      if (port !== MAIL_PORT) console.log('Почта: отправлено через порт ' + port);
      return { sent: true };
    } catch (e) {
      lastErr = e.message || 'неизвестная ошибка';
      console.error('Почта (порт ' + port + '):', lastErr); errs.push('SMTP:' + port + ' → ' + lastErr);
      if (/пароль отклонён|RCPT|MAIL FROM/.test(lastErr)) break; /* не сеть — пробовать другой порт бессмысленно */
    }
  }
  return { sent: false, reason: errs.length ? errs.join(' | ') : 'ничего не настроено' };
}

route('GET', '/api/mailcheck', async (req, res) => {
  send(res, 200, {
    mailopost: !!MP_KEY,
    rusender: !!RS_KEY,
    sendpulse: !!(SP_ID && SP_SECRET),
    hook: !!MAIL_HOOK_URL,
    brevo: !!BREVO_KEY,
    smtp: !!(MAIL_USER && MAIL_PASS),
    from: MAIL_USER || null,
    build: 'rusender-4'
  });
});

route('POST', '/api/auth/email/request', async (req, res, body) => {
  const email = normEmail(body.email);
  if (!validEmail(email)) return send(res, 400, { error: 'Проверьте адрес почты' });

  const key = 'mail:' + email;
  const prev = db.codes[key];
  if (prev && prev.sentAt && now() - prev.sentAt < 60e3) {
    return send(res, 429, { error: 'Код уже отправлен, подождите минуту' });
  }
  const hourAgo = now() - 3600e3;
  const recent = (prev && prev.log ? prev.log : []).filter(t => t > hourAgo);
  if (recent.length >= 5) return send(res, 429, { error: 'Слишком много запросов. Попробуйте через час.' });

  const code = String(Math.floor(10000 + Math.random() * 90000));
  db.codes[key] = { code, expires: now() + 10 * 60e3, sentAt: now(), attempts: 0, log: recent.concat(now()) };
  save();

  if (MAIL_ENABLED && validEmail(email)) {
    const r = await sendMail(email, code);
    if (!r.sent) {
      delete db.codes[key];
      save();
      return send(res, 502, { error: r.reason || 'Не удалось отправить письмо' });
    }
    return send(res, 200, { ok: true });
  }
  console.log(`Код для ${email}: ${code}`);
  send(res, 200, { ok: true, devCode: code });
});

route('POST', '/api/auth/email/verify', async (req, res, body) => {
  const email = normEmail(body.email);
  const code = String(body.code || '').replace(/\D/g, '');
  const key = 'mail:' + email;
  const rec = db.codes[key];

  if (!rec || rec.expires < now()) return send(res, 400, { error: 'Код истёк, запросите новый' });
  rec.attempts = (rec.attempts || 0) + 1;
  if (rec.attempts > 5) {
    delete db.codes[key];
    save();
    return send(res, 429, { error: 'Слишком много попыток. Запросите новый код.' });
  }
  if (rec.code !== code) {
    save();
    return send(res, 400, { error: 'Неверный код' });
  }
  delete db.codes[key];

  let user = Object.values(db.users).find(u => u.email === email);
  const token = crypto.randomBytes(24).toString('hex');
  if (user) {
    const mine = Object.entries(db.tokens).filter(([, id]) => id === user.id);
    while (mine.length >= 5) delete db.tokens[mine.shift()[0]];
  }
  if (!user) {
    user = {
      id: uid(), email, phone: '', name: '', username: '',
      cover: 0, ava: 0, status: '', trust: 100, createdAt: now()
    };
    db.users[user.id] = user;
  }
  db.tokens[token] = user.id;
  save();

  send(res, 200, {
    token,
    needsSetup: !user.username,
    state: user.username ? fullState(user) : null
  });
});

route('POST', '/api/profile/phone/start', async (req, res, body, user) => {
  if (!TG_ENABLED) return send(res, 400, { error: 'Привязка через Telegram не настроена' });
  if (user.phone) return send(res, 400, { error: 'Телефон уже привязан' });

  const cutoff = now() - 15 * 60e3;
  for (const s of Object.keys(db.tgSessions)) {
    const r = db.tgSessions[s];
    if (r.created < cutoff || (r.usedAt && r.usedAt < now() - 2 * 60e3)) delete db.tgSessions[s];
  }

  const session = crypto.randomBytes(12).toString('hex');
  db.tgSessions[session] = { created: now(), chatId: null, status: 'pending', token: null, linkFor: user.id };
  save();
  send(res, 200, { session, link: `https://t.me/${TG_BOT_NAME}?start=${session}` });
});

route('POST', '/api/profile/phone/check', async (req, res, body, user) => {
  const rec = db.tgSessions[String(body.session || '')];
  if (!rec || rec.linkFor !== user.id) return send(res, 404, { error: 'Сессия не найдена' });
  if (rec.status === 'error') {
    const err = rec.error || 'Не удалось привязать';
    delete db.tgSessions[String(body.session)];
    save();
    return send(res, 400, { error: err });
  }
  if (rec.status !== 'ok') return send(res, 200, { status: 'pending' });
  rec.usedAt = now();
  save();
  send(res, 200, { status: 'ok', state: fullState(user) });
});

route('POST', '/api/auth/verify', async (req, res, body) => {
  const phone = normPhone(body.phone);
  const code = String(body.code || '').replace(/\D/g, '');
  const rec = db.codes[phone];

  if (!rec || rec.expires < now()) return send(res, 400, { error: 'Код истёк, запросите новый' });

  /* Не больше 5 попыток ввода — иначе код можно подобрать перебором */
  rec.attempts = (rec.attempts || 0) + 1;
  if (rec.attempts > 5) {
    delete db.codes[phone];
    save();
    return send(res, 429, { error: 'Слишком много попыток. Запросите новый код.' });
  }

  if (rec.code !== code) {
    save();
    return send(res, 400, { error: 'Неверный код' });
  }

  delete db.codes[phone];

  let user = Object.values(db.users).find(u => u.phone === phone);
  const token = crypto.randomBytes(24).toString('hex');
  if (user) {
    /* Старые сессии сверх пяти умирают — украденный давний токен бесполезен */
    const mine = Object.entries(db.tokens).filter(([, id]) => id === user.id);
    while (mine.length >= 5) delete db.tokens[mine.shift()[0]];
  }

  if (!user) {
    const id = uid();
    user = {
      id, phone, name: '', username: '',
      cover: 0, ava: 0, status: '',
      verified: false, dev: false,
      trust: 100, createdAt: now()
    };
    db.users[id] = user;
  }

  db.tokens[token] = user.id;
  save();

  send(res, 200, {
    token,
    needsSetup: !user.username,
    state: user.username ? fullState(user) : null
  });
});

route('POST', '/api/profile/setup', async (req, res, body, user) => {
  const name = String(body.name || '').trim().slice(0, 30);
  const username = normUsername(body.username);

  if (!name) return send(res, 400, { error: 'Введите имя' });
  if (username.length < 5) return send(res, 400, { error: 'Юзернейм — минимум 5 символов' });
  if (db.usernames[username]) return send(res, 400, { error: 'Этот юзернейм уже занят' });

  user.name = name;
  user.username = username;
  user.dev = DEV_USERNAMES.includes(username);
  user.codev = CODEV_USERNAMES.includes(username);
  if (user.dev || user.codev) user.verified = true;

  db.usernames[username] = { owner: user.id, main: true, forSale: false, price: 0 };

  /* Пришёл по ссылке друга — засчитываем приглашение (один раз) */
  const ref = normUsername(body.ref);
  if (ref && !user.invitedBy && ref !== username && db.usernames[ref]) {
    const inviter = db.users[db.usernames[ref].owner];
    if (inviter && !inviter.isBot) {
      user.invitedBy = inviter.id;
      awardInvite(inviter);
    }
  }
  save();

  serviceMessage(user.id, `Добро пожаловать в Newchat, ${name}! Здесь будут уведомления о жалобах, покупках и безопасности аккаунта.`);

  send(res, 200, { state: fullState(user) });
});

/* ---------- Состояние ---------- */

route('GET', '/api/state', async (req, res, body, user) => {
  send(res, 200, { state: fullState(user) });
});

route('POST', '/api/profile/update', async (req, res, body, user) => {
  if (typeof body.cover === 'number') user.cover = body.cover;
  if (typeof body.ava === 'number') user.ava = body.ava;
  if (typeof body.banner === 'number') user.banner = Math.max(0, Math.min(7, body.banner));
  if (typeof body.status === 'string') user.status = body.status.slice(0, 40);
  if (typeof body.name === 'string' && body.name.trim()) user.name = body.name.trim().slice(0, 30);
  if (typeof body.photo === 'string') {
    /* Аватарка: маленький jpeg в base64, клиент сжимает сам */
    if (body.photo === '') user.photo = null;
    else if (/^data:image\/(jpeg|png|webp);base64,/.test(body.photo) && body.photo.length < 200000) {
      user.photo = body.photo;
    } else {
      return send(res, 400, { error: 'Фото слишком большое' });
    }
  }
  save();
  send(res, 200, { ok: true, user: publicUser(user) });
});

/* ---------- Чаты и сообщения ---------- */

route('POST', '/api/chats/create', async (req, res, body, user) => {
  let peer = null;
  if (body.userId) {
    peer = db.users[String(body.userId)];
  } else {
    const username = normUsername(body.username);
    if (username === user.username) return send(res, 400, { error: 'Это ваш собственный юзернейм' });
    const rec = db.usernames[username];
    if (rec && rec.channel) return send(res, 400, { error: 'Это канал — найдите его через поиск' });
    peer = rec && db.users[rec.owner];
  }
  if (!peer || peer.id === user.id) return send(res, 404, { error: 'Пользователь не найден' });
  const existing0 = Object.values(db.chats).find(c => !c.service && c.type !== 'channel' && c.members.includes(user.id) && c.members.includes(peer.id));
  if (existing0 && (existing0.hiddenFor || []).includes(user.id)) {
    existing0.hiddenFor = existing0.hiddenFor.filter(x => x !== user.id);
    save();
  }
  if (!existing0) {
    if (peer.anon && !peer.isBot) return send(res, 404, { error: 'Пользователь не найден' });
    if ((peer.blocked || {})[user.id]) return send(res, 403, { error: 'Пользователь ограничил переписку' });
    if ((user.blocked || {})[peer.id]) return send(res, 403, { error: 'Вы заблокировали этого пользователя' });
  }

  const id = chatIdFor(user.id, peer.id);
  if (!db.chats[id]) {
    db.chats[id] = { id, members: [user.id, peer.id], service: false, msgs: [] };
    save();
    push(peer.id, { type: 'chats' });
  }
  send(res, 200, { chatId: id, chats: userChats(user.id) });
});

function validMedia(media) {
  if (!media || typeof media !== 'object') return null;
  const data = String(media.data || '');
  if (media.kind === 'photo') {
    if (!/^data:image\/(jpeg|png|webp);base64,/.test(data) || data.length > 700000) return null;
    return { kind: 'photo', data };
  }
  if (media.kind === 'voice') {
    if (!/^data:audio\/(webm|ogg|mp4|mpeg|wav)(;codecs=[a-z0-9]+)?;base64,/.test(data) || data.length > 3000000) return null;
    return { kind: 'voice', data, dur: Math.min(300, Math.max(1, Math.round(Number(media.dur) || 1))) };
  }
  if (media.kind === 'video' || media.kind === 'circle') {
    if (!/^data:video\/[a-z0-9.+-]+(;codecs=[a-z0-9,.\s"']+)?;base64,/i.test(data)) return null;
    if (data.length > MAX_BYTES) return null;
    return {
      kind: media.kind, data,
      dur: Math.min(300, Math.max(1, Math.round(Number(media.dur) || 1)))
    };
  }
  if (media.kind === 'file') {
    /* Любой файл: архив, документ, что угодно */
    if (!/^data:[a-z0-9.+\/-]*;base64,/i.test(data)) return null;
    if (data.length > MAX_BYTES) return null;
    const name = String(media.name || 'файл').replace(/[\r\n]/g, '').slice(0, 80);
    return { kind: 'file', data, name, size: Math.round(data.length * 0.75) };
  }
  return null;
}

route('POST', '/api/messages/send', async (req, res, body, user) => {
  const chat = db.chats[body.chatId];
  const text = String(body.text || '').trim().slice(0, 4000);
  const media = validMedia(body.media);

  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });
  if (body.media && !media) return send(res, 400, { error: 'Файл не подходит или больше ' + MAX_MB + ' МБ' });
  if (!text && !media && !body.enc) return send(res, 400, { error: 'Пустое сообщение' });
  if (chat.service) return send(res, 400, { error: 'В служебный чат писать нельзя' });
  if (chat.type === 'channel' && chat.owner !== user.id) {
    return send(res, 403, { error: 'В канале пишет только владелец' });
  }
  if (chat.type !== 'channel' && !chat.service) {
    const pid = chat.members.find(m => m !== user.id);
    const p = pid && db.users[pid];
    if (p && (p.blocked || {})[user.id]) return send(res, 403, { error: 'Пользователь ограничил переписку' });
    if (p && (user.blocked || {})[p.id]) return send(res, 403, { error: 'Вы заблокировали этого пользователя — разблокируйте в меню чата' });
  }

  let outText = text;
  /* Старые версии приложения не умеют показывать медиа — им достанется понятная надпись */
  if (media && !text) outText = media.kind === 'photo' ? '📷 Фото'
    : media.kind === 'voice' ? '🎤 Голосовое сообщение'
    : media.kind === 'circle' ? '⭕ Видеосообщение'
    : media.kind === 'file' ? ('📎 ' + media.name) : '🎬 Видео';
  const msg = { id: uid(), from: user.id, text: outText, time: now(), deleted: false };
  if (body.enc) {
    /* Шифротекст: сервер хранит и передаёт, но прочитать не может */
    msg.enc = String(body.enc).slice(0, 300000);
    msg.iv = String(body.iv || '').slice(0, 64);
    msg.text = '';
  }
  if (media) msg.media = media;

  /* Ответ на сообщение — храним короткий снимок цитаты */
  if (body.replyTo) {
    const orig = chat.msgs.find(m => m.id === String(body.replyTo) && !m.deleted);
    if (orig) {
      const author = db.users[orig.from];
      msg.reply = {
        id: orig.id,
        name: orig.from === user.id ? 'Вы' : ((author && author.name) || 'Собеседник'),
        text: (orig.text || (orig.media ? (orig.media.kind === 'photo' ? '📷 Фото' : '🎤 Голосовое') : '')).slice(0, 70)
      };
    }
  }
  chat.msgs.push(msg);
  if (chat.hiddenFor && chat.hiddenFor.length) chat.hiddenFor = []; /* удалённый у себя чат оживает */
  save();

  if (chat.type === 'channel') {
    /* Пост уходит всем подписчикам */
    for (const m of chat.members) {
      if (m !== user.id) push(m, { type: 'message', chatId: chat.id, message: { id: msg.id, text: msg.text, enc: msg.enc || null, iv: msg.iv || null, media: msg.media || null, reply: msg.reply || null, fwd: msg.fwd || null, time: msg.time, out: false } });
    }
  } else {
    const peerId = chat.members.find(m => m !== user.id);
    const peer = peerId && db.users[peerId];
    if (peer && peer.isBot) {
      /* Сообщение боту — кладём в очередь, её заберёт код бота */
      db.botUpdates[peer.id] = db.botUpdates[peer.id] || [];
      const q = db.botUpdates[peer.id];
      q.push({
        update_id: (q.length ? q[q.length - 1].update_id : 0) + 1,
        chatId: chat.id,
        from: publicUser(user),
        text, time: msg.time
      });
      if (q.length > 500) q.splice(0, q.length - 500);
      save();
    } else if (peerId) {
      push(peerId, { type: 'message', chatId: chat.id, message: { id: msg.id, text: msg.text, enc: msg.enc || null, iv: msg.iv || null, media: msg.media || null, reply: msg.reply || null, fwd: msg.fwd || null, time: msg.time, out: false } });
      notifyPush(peerId);
    }
  }
  send(res, 200, { message: { id: msg.id, text: msg.text, enc: msg.enc || null, iv: msg.iv || null, media: msg.media || null, reply: msg.reply || null, fwd: msg.fwd || null, reactions: null, time: msg.time, out: true } });
});

route('POST', '/api/messages/forward', async (req, res, body, user) => {
  const src = db.chats[String(body.fromChatId || '')];
  const dst = db.chats[String(body.toChatId || '')];
  if (!src || !src.members.includes(user.id)) return send(res, 404, { error: 'Исходный чат не найден' });
  if (!dst || !dst.members.includes(user.id)) return send(res, 404, { error: 'Чат для пересылки не найден' });
  if (dst.service) return send(res, 400, { error: 'В служебный чат писать нельзя' });
  if (dst.type === 'channel' && dst.owner !== user.id) return send(res, 403, { error: 'В канале пишет только владелец' });

  const orig = src.msgs.find(m => m.id === String(body.messageId || '') && !m.deleted);
  if (!orig) return send(res, 404, { error: 'Сообщение не найдено' });

  if (dst.type !== 'channel') {
    const pid = dst.members.find(m => m !== user.id);
    const p = pid && db.users[pid];
    if (p && (p.blocked || {})[user.id]) return send(res, 403, { error: 'Пользователь ограничил переписку' });
    if (p && (user.blocked || {})[p.id]) return send(res, 403, { error: 'Вы заблокировали этого пользователя' });
  }

  const author = db.users[orig.from];
  const msg = {
    id: uid(), from: user.id,
    text: orig.text || '', time: now(), deleted: false,
    fwd: { name: (author && author.name) || 'Newchat' }
  };
  if (orig.media) msg.media = orig.media;
  dst.msgs.push(msg);
  if (dst.hiddenFor && dst.hiddenFor.length) dst.hiddenFor = [];
  save();

  const payload = { id: msg.id, text: msg.text, media: msg.media || null, reply: null, fwd: msg.fwd, time: msg.time, out: false };
  if (dst.type === 'channel') {
    for (const m of dst.members) if (m !== user.id) push(m, { type: 'message', chatId: dst.id, message: payload });
  } else {
    const pid = dst.members.find(m => m !== user.id);
    const p = pid && db.users[pid];
    if (p && p.isBot) {
      db.botUpdates[p.id] = db.botUpdates[p.id] || [];
      const q = db.botUpdates[p.id];
      q.push({ update_id: (q.length ? q[q.length - 1].update_id : 0) + 1, chatId: dst.id, from: publicUser(user), text: msg.text, time: msg.time });
      if (q.length > 500) q.splice(0, q.length - 500);
      save();
    } else if (pid) {
      push(pid, { type: 'message', chatId: dst.id, message: payload });
      notifyPush(pid);
    }
  }
  send(res, 200, { message: Object.assign({}, payload, { out: true }) });
});

route('POST', '/api/messages/react', async (req, res, body, user) => {
  const chat = db.chats[body.chatId];
  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });
  const msg = chat.msgs.find(m => m.id === String(body.messageId || ''));
  if (!msg || msg.deleted) return send(res, 404, { error: 'Сообщение не найдено' });

  const emoji = String(body.emoji || '').slice(0, 4);
  const ALLOWED = ['❤️', '👍', '🔥', '😂', '😮', '💩'];
  if (!ALLOWED.includes(emoji)) return send(res, 400, { error: 'Такой реакции нет' });

  msg.reactions = msg.reactions || {};
  const list = msg.reactions[emoji] || [];
  /* Одна реакция от человека: повторный тап снимает, другая — заменяет */
  for (const e of Object.keys(msg.reactions)) {
    msg.reactions[e] = msg.reactions[e].filter(id => id !== user.id);
    if (!msg.reactions[e].length) delete msg.reactions[e];
  }
  if (!list.includes(user.id)) {
    msg.reactions[emoji] = msg.reactions[emoji] || [];
    msg.reactions[emoji].push(user.id);
  }
  if (!Object.keys(msg.reactions).length) msg.reactions = null;
  save();

  for (const m of chat.members) {
    if (m !== user.id) push(m, { type: 'react', chatId: chat.id, messageId: msg.id, reactions: msg.reactions });
  }
  send(res, 200, { reactions: msg.reactions });
});

route('POST', '/api/messages/delete', async (req, res, body, user) => {
  const chat = db.chats[body.chatId];
  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });

  const msg = chat.msgs.find(m => m.id === body.messageId && m.from === user.id);
  if (!msg) return send(res, 404, { error: 'Сообщение не найдено' });

  /* Помечаем удалённым, но текст храним — он нужен для жалоб в полицию */
  msg.deleted = true;
  save();

  const peerId = chat.members.find(m => m !== user.id);
  if (peerId) push(peerId, { type: 'deleted', chatId: chat.id, messageId: msg.id });
  send(res, 200, { ok: true });
});

/* ---------- Жалобы ---------- */

function esc(v) {
  return String(v || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function reportHtml(r) {
  const dt = t => new Date(t).toLocaleString('ru-RU');
  const rows = r.snapshot.map(m => {
    const who = m.from === r.against
      ? (r.peerName || 'Собеседник') + ' (' + (r.peerUsername ? '@' + r.peerUsername : 'без юзернейма') + ')'
      : (r.authorName || 'Заявитель');
    const mark = m.deleted ? '<span class="del">УДАЛЕНО ОТПРАВИТЕЛЕМ</span> ' : '';
    const med = m.media ? '<span class="med">[вложение: ' + esc(m.media) + ']</span> ' : '';
    return '<tr><td class="t">' + dt(m.time) + '</td><td class="w">' + esc(who) +
      '</td><td>' + mark + med + esc(m.text) + '</td></tr>';
  }).join('');

  return '<!doctype html><html lang="ru"><head><meta charset="utf-8">' +
    '<meta name="viewport" content="width=device-width,initial-scale=1">' +
    '<title>Протокол переписки №' + r.num + '</title><style>' +
    'body{font-family:Arial,Helvetica,sans-serif;max-width:900px;margin:0 auto;padding:24px;color:#17181D;line-height:1.5}' +
    'h1{font-size:20px;margin:0 0 4px}h2{font-size:15px;margin:26px 0 8px;border-bottom:2px solid #6C5CE7;padding-bottom:4px}' +
    '.sub{color:#666;font-size:13px;margin-bottom:20px}' +
    '.card{background:#F4F5F9;border-radius:8px;padding:14px 16px;margin:10px 0;font-size:14px}' +
    'table{width:100%;border-collapse:collapse;font-size:12.5px;margin-top:8px}' +
    'th{background:#1B3A6B;color:#fff;text-align:left;padding:7px 8px;font-size:12px}' +
    'td{border-bottom:1px solid #E3E5EC;padding:7px 8px;vertical-align:top}' +
    '.t{white-space:nowrap;color:#666;width:130px}.w{width:200px;font-weight:bold}' +
    '.del{color:#C9342A;font-weight:bold;font-size:11px}' +
    '.med{color:#6C5CE7;font-size:11px}' +
    'ol{padding-left:20px}li{margin-bottom:8px;font-size:14px}' +
    '.warn{background:#FFF4E5;border-left:4px solid #E8890B;padding:12px 14px;margin:14px 0;font-size:13.5px}' +
    '@media print{body{padding:0}h2{page-break-after:avoid}}' +
    '</style></head><body>' +

    '<h1>Протокол переписки №' + r.num + '</h1>' +
    '<div class="sub">Мессенджер Newchat · тип обращения: ' + esc(r.kind) +
    ' · сформирован ' + dt(r.time) + '</div>' +

    '<h2>Заявитель</h2><div class="card">' +
    'Имя: ' + esc(r.authorName) + '<br>Юзернейм: ' + (r.authorUsername ? '@' + esc(r.authorUsername) : '—') +
    '</div>' +

    '<h2>Лицо, в отношении которого подано обращение</h2><div class="card">' +
    'Имя в мессенджере: ' + esc(r.peerName) + '<br>' +
    'Юзернейм: ' + (r.peerUsername ? '@' + esc(r.peerUsername) : '—') + '<br>' +
    'Телефон при регистрации: ' + (r.peerPhone
      ? '+7 ' + esc(r.peerPhone.slice(0, 3)) + ' •••-••-' + esc(r.peerPhone.slice(-2)) + ' <i style="color:#777">(скрыт)</i>'
      : 'не привязан') + '<br>' +
    'Почта: ' + (r.peerEmail
      ? esc(r.peerEmail.slice(0, 2)) + '•••@' + esc((r.peerEmail.split('@')[1] || '')) + ' <i style="color:#777">(скрыта)</i>'
      : 'не указана') + '<br>' +
    'Аккаунт создан: ' + (r.peerCreated ? dt(r.peerCreated) : '—') +
    '</div>' +

    '<h2>Переписка полностью</h2>' +
    '<div class="sub">Включая сообщения, удалённые отправителем. ' +
    'Всего записей: ' + r.snapshot.length + '</div>' +
    '<table><tr><th>Время</th><th>Отправитель</th><th>Сообщение</th></tr>' + rows + '</table>' +

    '<div class="warn" style="background:#EEF1FF;border-left-color:#6C5CE7">' +
    '<b>Почему телефон скрыт.</b> Мы не раскрываем личные данные по обращению частного лица — ' +
    'иначе протокол сам стал бы способом узнать чужой номер. Полные данные пользователя ' +
    '(телефон, почта, IP-адреса, история входов) администрация Newchat передаёт ' +
    '<b>только по официальному запросу правоохранительных органов</b> с указанием номера протокола №' + r.num + '. ' +
    'Следователь направляет запрос — мы отвечаем в установленный законом срок.</div>' +

    '<h2>Что делать дальше</h2>' +
    '<div class="warn">Сохраните эту страницу в PDF: в браузере нажмите «Поделиться» или «⋮» → ' +
    '«Печать» → «Сохранить как PDF». Получится файл, который можно приложить к заявлению.</div>' +
    '<ol>' +
    '<li><b>Подайте заявление онлайн.</b> Портал МВД России: мвд.рф → раздел «Приём обращений». ' +
    'Заявление рассматривается официально, ответ приходит в течение 30 дней.</li>' +
    '<li><b>Либо придите в отдел полиции</b> по месту жительства с распечаткой этого протокола ' +
    'и паспортом. Попросите зарегистрировать заявление и выдать талон-уведомление.</li>' +
    '<li><b>Если речь о хищении денег</b> — приложите чеки и выписки о переводах. ' +
    'Статья 159 УК РФ (мошенничество).</li>' +
    '<li><b>Если распространены личные данные</b> — укажите это в заявлении. ' +
    'Статья 137 УК РФ (нарушение неприкосновенности частной жизни), ' +
    'плюс жалоба в Роскомнадзор: rkn.gov.ru.</li>' +
    '<li><b>Укажите в заявлении номер протокола</b> — №' + r.num + '. ' +
    'По нему следователь запросит у администрации Newchat полные данные нарушителя ' +
    'и подтверждение подлинности переписки. Без этого номера запрос обработать нельзя.</li>' +
    '</ol>' +

    '<h2>О документе</h2><div class="card" style="font-size:12.5px;color:#555">' +
    'Протокол сформирован автоматически из базы данных мессенджера Newchat. ' +
    'Содержит сообщения в том виде, в каком они хранятся на сервере, включая удалённые пользователями. ' +
    'Документ не является заключением экспертизы и не устанавливает вину: ' +
    'оценку даёт следствие и суд.' +
    '</div></body></html>';
}

route('POST', '/api/reports/create', async (req, res, body, user) => {
  const chat = db.chats[body.chatId];
  const kind = body.kind === 'докс' ? 'докс' : 'скам';
  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });
  if (chat.type === 'channel' || chat.service) return send(res, 400, { error: 'Жаловаться можно на личную переписку' });
  if (chat.secret) return send(res, 400, { error: 'В секретном чате сервер не хранит текст — протокол собрать невозможно' });

  /* Пауза после предыдущей жалобы — чтобы протокол не использовали
     как способ читать удалённые сообщения */
  const onTeam = isDev(user) || isCodev(user);
  if (!onTeam && user.reportBlockUntil && user.reportBlockUntil > now()) {
    const hrs = Math.ceil((user.reportBlockUntil - now()) / 3600e3);
    return send(res, 429, { error: 'Следующую жалобу можно подать через ' + hrs + ' ч. Ограничение защищает от злоупотреблений.' });
  }

  const peerId = chat.members.find(m => m !== user.id);
  const peer = db.users[peerId] || {};
  if (isDev(peer) || isCodev(peer)) {
    return send(res, 400, { error: 'Это аккаунт команды Newchat — жалоба на него не имеет смысла. Напишите разработчику напрямую.' });
  }
  const email = normEmail(body.email);   /* почта необязательна */

  const num = 4000 + db.reports.length + 1;
  const token = crypto.randomBytes(16).toString('hex');
  const report = {
    num, token, from: user.id, against: peerId, chatId: chat.id,
    kind, time: now(), email,
    peerName: peer.name || '', peerUsername: peer.username || '',
    peerPhone: peer.phone || '', peerEmail: peer.email || '',
    peerCreated: peer.createdAt || 0,
    authorName: user.name || '', authorUsername: user.username || '',
    /* Полный слепок переписки, включая удалённые сообщения */
    snapshot: chat.msgs.map(m => ({
      from: m.from, text: m.text || '', time: m.time,
      deleted: !!m.deleted, media: m.media ? m.media.kind : null
    }))
  };
  db.reports.push(report);

  /* Двое суток без новых жалоб и без биржи. Команду это не касается:
     мы помогаем людям, которым угрожают прямо сейчас. */
  if (!onTeam) user.reportBlockUntil = now() + 2 * 86400e3;
  save();

  const link = PUBLIC_URL + '/report/' + num + '?t=' + token;

  if (MAIL_ENABLED) {
    const html = '<div style="font-family:Arial,sans-serif;max-width:520px;margin:0 auto;padding:24px">' +
      '<h2 style="color:#17181D">Протокол переписки №' + num + '</h2>' +
      '<p>Вы подали жалобу «' + kind + '» в Newchat. Протокол переписки со всеми сообщениями, ' +
      'включая удалённые, доступен по ссылке:</p>' +
      '<p><a href="' + link + '" style="color:#6C5CE7;font-weight:bold">Открыть протокол №' + num + '</a></p>' +
      '<p>Откройте ссылку и сохраните страницу в PDF: в браузере «Поделиться» → «Печать» → «Сохранить как PDF».</p>' +
      '<p>Внутри протокола — инструкция, куда и как подать заявление.</p>' +
      '<p style="color:#787A86;font-size:13px">Ссылка действует 30 дней. Не пересылайте её посторонним: ' +
      'в протоколе есть личные данные.</p></div>';
    sendMail(email, num).catch(() => {});
    /* письмо с кодом не подходит — шлём собственное */
    try {
      if (typeof rusender === 'function' && RS_KEY) {
        rusender(email, 'Протокол переписки №' + num + ' — Newchat', html).catch(() => {});
      } else if (BREVO_KEY) {
        brevoSend(email, 'Протокол переписки №' + num + ' — Newchat', html).catch(() => {});
      } else if (MAIL_HOOK_URL) {
        hookSend(email, 'Протокол переписки №' + num + ' — Newchat', html).catch(() => {});
      }
    } catch (e) {}
  }

  /* Человек должен знать, что на него подали жалобу: тайный сбор данных недопустим */
  if (peerId) {
    serviceMessage(peerId, 'На вас подана жалоба «' + kind + '» (протокол №' + num + '). ' +
      'Сформирован слепок переписки. Ваши личные данные не раскрыты: они передаются ' +
      'только по официальному запросу правоохранительных органов. ' +
      'Если жалоба ложная, это скажется на доверии подавшего.');
    push(peerId, { type: 'state' });
  }

  serviceMessage(user.id,
    'Жалоба №' + num + ' (' + kind + ') зарегистрирована.\n\n' +
    'Протокол переписки со всеми сообщениями, включая удалённые, откроется по ссылке: ' + link + '\n\n' +
    'Откройте ссылку и сохраните в PDF, затем подайте заявление — инструкция внутри протокола.\n\n' +
    (onTeam
      ? 'Вы из команды — ограничение на частоту жалоб к вам не применяется.'
      : 'Следующую жалобу можно подать через 2 суток: ограничение защищает от тех, ' +
        'кто использовал бы протокол ради чтения удалённых сообщений.'));
  push(user.id, { type: 'state' });

  send(res, 200, { num, link, mailed: MAIL_ENABLED && validEmail(email) });
});

/* ---------- Реквизиты для получения денег ---------- */
/* Деньги идут напрямую покупателю -> продавцу. Сервер хранит только
   реквизиты, которые продавец сам решил показывать покупателям. */

route('POST', '/api/profile/requisites', async (req, res, body, user) => {
  const kind = body.kind === 'card' ? 'card' : 'sbp';
  const bank = String(body.bank || '').trim().slice(0, 30);
  let value = String(body.value || '').replace(/[^0-9+]/g, '');

  if (kind === 'card') {
    if (!/^[0-9]{16,19}$/.test(value)) return send(res, 400, { error: 'Номер карты — 16–19 цифр' });
  } else {
    value = value.replace(/^8/, '+7').replace(/^7/, '+7');
    if (!/^\+7[0-9]{10}$/.test(value)) return send(res, 400, { error: 'Номер телефона для СБП — в формате +7…' });
    if (!bank) return send(res, 400, { error: 'Укажите банк для СБП' });
  }

  user.requisites = { kind, value, bank };
  save();
  send(res, 200, { requisites: user.requisites });
});

route('POST', '/api/profile/requisites/delete', async (req, res, body, user) => {
  const active = Object.values(db.deals).some(d => d.seller === user.id && (d.status === 'pay' || d.status === 'paid' || d.status === 'dispute'));
  if (active) return send(res, 400, { error: 'Есть активные сделки — реквизиты пока нужны покупателям' });
  user.requisites = null;
  const mine = myUsernames(user.id);
  for (const m of mine) {
    if (m.forSale) { db.usernames[m.u].forSale = false; db.usernames[m.u].price = 0; }
  }
  save();
  send(res, 200, { ok: true });
});

/* ---------- Сделки ---------- */

route('POST', '/api/deals/start', async (req, res, body, user) => {
  const username = normUsername(body.username);
  const rec = db.usernames[username];

  if (!rec || !rec.forSale || rec.frozen) return send(res, 404, { error: 'Лот не найден или уже в сделке' });
  if (rec.owner === user.id) return send(res, 400, { error: 'Это ваш лот' });

  const seller = db.users[rec.owner];
  if (!seller || !seller.requisites) return send(res, 400, { error: 'Продавец не указал реквизиты' });

  if (!user.phone) return send(res, 403, { error: 'Для покупки привяжите телефон в профиле' });
  if (!isDev(user) && !isCodev(user) && user.reportBlockUntil && user.reportBlockUntil > now()) return send(res, 403, { error: 'Биржа закрыта на 2 суток после подачи жалобы' });
  const active = Object.values(db.deals).filter(d => d.buyer === user.id && (d.status === 'pay' || d.status === 'paid'));
  if (active.length >= 3) return send(res, 400, { error: 'У вас уже 3 активные сделки' });

  const deal = {
    id: uid(),
    username, price: rec.price,
    seller: seller.id, buyer: user.id,
    requisites: seller.requisites,
    status: 'pay',
    createdAt: now()
  };
  db.deals[deal.id] = deal;
  rec.frozen = deal.id;
  rec.forSale = false; /* с биржи лот уходит, юзернейм под замком до конца сделки */
  save();

  serviceMessage(seller.id, `На @${username} нашёлся покупатель за ${deal.price.toLocaleString('ru')} ₽. Юзернейм заморожен до конца сделки. Ждём перевод.`);
  push(seller.id, { type: 'state' });
  send(res, 200, { deal: dealView(deal, user.id), state: fullState(user) });
});

route('POST', '/api/deals/paid', async (req, res, body, user) => {
  const deal = db.deals[String(body.dealId || '')];
  if (!deal || deal.buyer !== user.id) return send(res, 404, { error: 'Сделка не найдена' });
  if (deal.status !== 'pay') return send(res, 400, { error: 'Сделка уже в другом статусе' });

  deal.status = 'paid';
  deal.paidAt = now();
  save();

  const days = DEAL.confirmDays;
  serviceMessage(deal.seller, `Покупатель отметил перевод ${deal.price.toLocaleString('ru')} ₽ за @${deal.username}. Проверьте поступление и подтвердите. Без ответа за ${days} дн. юзернейм перейдёт покупателю автоматически.`);
  push(deal.seller, { type: 'state' });
  send(res, 200, { deal: dealView(deal, user.id), state: fullState(user) });
});

route('POST', '/api/deals/confirm', async (req, res, body, user) => {
  const deal = db.deals[String(body.dealId || '')];
  if (!deal || deal.seller !== user.id) return send(res, 404, { error: 'Сделка не найдена' });
  if (deal.status !== 'paid') return send(res, 400, { error: 'Покупатель ещё не отметил перевод' });
  finishDeal(deal, 'done');
  send(res, 200, { state: fullState(user) });
});

route('POST', '/api/deals/cancel', async (req, res, body, user) => {
  const deal = db.deals[String(body.dealId || '')];
  if (!deal || (deal.buyer !== user.id && deal.seller !== user.id)) return send(res, 404, { error: 'Сделка не найдена' });
  /* Покупатель может передумать, пока не отметил перевод. Продавец — только пока нет оплаты. */
  if (deal.status !== 'pay') return send(res, 400, { error: 'После отметки о переводе отмена только через спор' });
  finishDeal(deal, 'cancelled');
  send(res, 200, { state: fullState(user) });
});

route('POST', '/api/deals/dispute', async (req, res, body, user) => {
  const deal = db.deals[String(body.dealId || '')];
  if (!deal || deal.seller !== user.id) return send(res, 404, { error: 'Сделка не найдена' });
  if (deal.status !== 'paid') return send(res, 400, { error: 'Спор открывается после отметки «Я перевёл»' });

  deal.status = 'dispute';
  deal.disputeAt = now();
  save();

  serviceMessage(deal.buyer, `Продавец @${(db.users[deal.seller] || {}).username || ''} сообщил, что перевод за @${deal.username} не пришёл. Сделка на проверке у модерации.`);
  push(deal.buyer, { type: 'state' });
  send(res, 200, { state: fullState(user) });
});

/* Модерация споров — только для владельца сервиса */
route('POST', '/api/admin/deals', async (req, res, body) => {
  if (!ADMIN_TOKEN || body.adminToken !== ADMIN_TOKEN) return send(res, 403, { error: 'Нет доступа' });

  if (body.action === 'list') {
    return send(res, 200, {
      deals: Object.values(db.deals)
        .filter(d => d.status === 'dispute')
        .map(d => Object.assign({}, d, {
          sellerUser: publicUser(db.users[d.seller]),
          buyerUser: publicUser(db.users[d.buyer])
        }))
    });
  }

  const deal = db.deals[String(body.dealId || '')];
  if (!deal) return send(res, 404, { error: 'Сделка не найдена' });

  if (body.action === 'release') { /* перевод был — отдать юзернейм покупателю */
    finishDeal(deal, 'done');
    return send(res, 200, { ok: true });
  }
  if (body.action === 'cancel') { /* перевода не было — вернуть лот, наказать покупателя */
    const buyer = db.users[deal.buyer];
    if (buyer) buyer.trust = Math.max(0, (buyer.trust || 0) - 20);
    finishDeal(deal, 'cancelled');
    return send(res, 200, { ok: true });
  }
  send(res, 400, { error: 'Неизвестное действие' });
});

/* ---------- Панель разработчика ---------- */
/* Доступна только аккаунтам из DEV_USERNAMES (переменная на Render) */

route('POST', '/api/dev/stats', async (req, res, body, user) => {
  if (!isDev(user)) return send(res, 403, { error: 'Только для разработчиков' });
  const users = Object.values(db.users).filter(u => !u.isBot);
  const day = now() - 86400e3;
  let msgs = 0, mediaBytes = 0;
  for (const c of Object.values(db.chats)) {
    msgs += c.msgs.length;
    for (const m of c.msgs) if (m.media && m.media.data) mediaBytes += m.media.data.length;
  }
  send(res, 200, {
    stats: {
      users: users.length,
      online: users.filter(u => isOnline(u.id)).length,
      activeDay: users.filter(u => (u.lastSeen || 0) > day).length,
      newDay: users.filter(u => (u.createdAt || 0) > day).length,
      premium: users.filter(u => isPremium(u)).length,
      banned: users.filter(u => u.banned).length,
      bots: Object.values(db.users).filter(u => u.isBot).length,
      chats: Object.keys(db.chats).length,
      channels: Object.values(db.chats).filter(c => c.type === 'channel').length,
      msgs,
      deals: Object.keys(db.deals).length,
      disputes: Object.values(db.deals).filter(d => d.status === 'dispute').length,
      lots: Object.values(db.usernames).filter(v => v.forSale).length,
      stories: (db.stories || []).length,
      reports: db.reports.length,
      mediaMb: Math.round(mediaBytes / 1048576 * 10) / 10,
      musicMb: Math.round(Object.values(db.users).reduce((a, u) => a + ((u.music && u.music.data) ? u.music.data.length : 0), 0) / 1048576 * 10) / 10,
      musicCount: Object.values(db.users).filter(u => u.music).length,
      dbMb: Math.round(JSON.stringify(db).length / 1048576 * 10) / 10
    }
  });
});

route('POST', '/api/dev/purge', async (req, res, body, user) => {
  if (!isDev(user)) return send(res, 403, { error: 'Только для разработчиков' });
  let freed = 0, n = 0;
  for (const c of Object.values(db.chats)) {
    for (const m of c.msgs) {
      if (m.media && m.media.data && ['video', 'circle', 'file'].includes(m.media.kind)) {
        freed += m.media.data.length; n++;
        m.media = null;
        m.text = m.text || 'Вложение удалено администратором';
      }
    }
  }
  save();
  send(res, 200, { freedMb: Math.round(freed / 1048576 * 10) / 10, count: n });
});

route('POST', '/api/dev/gift', async (req, res, body, user) => {
  if (!isDev(user)) return send(res, 403, { error: 'Только для разработчиков' });
  const uname = normUsername(body.username);
  const gift = normUsername(body.gift);
  const rec = db.usernames[uname];
  const target = rec && db.users[rec.owner];
  if (!target) return send(res, 404, { error: 'Пользователь не найден' });
  if (!gift || gift.length < 5) return send(res, 400, { error: 'Юзернейм от 5 символов' });
  if (db.usernames[gift]) return send(res, 400, { error: 'Этот юзернейм занят' });
  db.usernames[gift] = { owner: target.id, main: false, forSale: false, price: 0 };
  save();
  serviceMessage(target.id, 'Команда Newchat подарила вам юзернейм @' + gift + '.');
  push(target.id, { type: 'state' });
  send(res, 200, { done: ['подарен @' + gift] });
});

route('POST', '/api/dev/gift-card', async (req, res, body, user) => {
  if (!isDev(user)) return send(res, 403, { error: 'Только для разработчиков' });
  const uname = normUsername(body.username);
  const rec = db.usernames[uname];
  const target = rec && db.users[rec.owner];
  if (!target) return send(res, 404, { error: 'Пользователь не найден' });

  const type = String(body.type || '').trim().toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 24);
  if (!type) return send(res, 400, { error: 'Укажите код карточки латиницей' });
  /* Выдача снимает запрет, если карточку раньше изымали */
  if (target.giftDenied) target.giftDenied = target.giftDenied.filter(t => t !== type);

  db.gifts = db.gifts || [];
  db.giftTypes = db.giftTypes || {};

  /* Новый вид карточки — создаём на лету */
  if (!GIFT_TYPES[type] && !db.giftTypes[type]) {
    db.giftTypes[type] = {
      name: String(body.name || type).slice(0, 30),
      total: 0,
      rarity: String(body.rarity || 'Редкая').slice(0, 20),
      rarityNum: Math.max(1, Math.min(5, Number(body.rarityNum) || 3)),
      desc: String(body.desc || '').slice(0, 200)
    };
  }

  const t = GIFT_TYPES[type] || db.giftTypes[type];
  const minted = db.gifts.filter(g => g.type === type).length;

  if (GIFT_TYPES[type]) {
    /* Заводские карточки: тираж закрыт навсегда */
    if (minted >= GIFT_TYPES[type].total) {
      return send(res, 400, { error: 'Тираж «' + t.name + '» исчерпан: ' + t.total + ' шт.' });
    }
  } else {
    /* Свои карточки: тираж растёт с каждой выдачей */
    db.giftTypes[type].total = minted + 1;
  }

  if (db.gifts.some(g => g.type === type && g.owner === target.id)) {
    return send(res, 400, { error: 'У этого человека уже есть «' + t.name + '»' });
  }

  db.gifts.push({ id: uid(), type, num: minted + 1, owner: target.id, time: now() });
  save();

  serviceMessage(target.id, 'Команда Newchat вручила вам коллекционную карточку «' + t.name + '» №' + (minted + 1) + '. Она в вашем профиле.');
  push(target.id, { type: 'state' });
  send(res, 200, { done: ['выдана «' + t.name + '» №' + (minted + 1) + ' пользователю @' + uname] });
});

route('POST', '/api/dev/gift-types', async (req, res, body, user) => {
  if (!isDev(user)) return send(res, 403, { error: 'Только для разработчиков' });
  const all = [];
  for (const [k, t] of Object.entries(GIFT_TYPES)) {
    all.push({ type: k, name: t.name, total: t.total, minted: (db.gifts || []).filter(g => g.type === k).length, fixed: true });
  }
  for (const [k, t] of Object.entries(db.giftTypes || {})) {
    all.push({ type: k, name: t.name, total: t.total, minted: (db.gifts || []).filter(g => g.type === k).length, fixed: false });
  }
  send(res, 200, { types: all });
});

route('POST', '/api/dev/report-review', async (req, res, body, user) => {
  /* Разбор жалобы: ложная, подтверждённая или предупреждение */
  if (!isDev(user) && !isCodev(user)) return send(res, 403, { error: 'Только для команды' });
  const num = Number(body.num);
  const r = db.reports.find(x => x.num === num);
  if (!r) return send(res, 404, { error: 'Протокол №' + num + ' не найден' });
  if (r.verdict) return send(res, 400, { error: 'По жалобе уже есть решение: ' + r.verdict });

  const verdict = ['false', 'valid', 'warn'].includes(body.verdict) ? body.verdict : 'warn';
  const note = String(body.note || '').slice(0, 300);
  const author = db.users[r.from];
  const target = db.users[r.against];

  r.verdict = verdict;
  r.verdictBy = user.username || user.id;
  r.verdictAt = now();
  r.verdictNote = note;

  if (verdict === 'false') {
    /* Жалоба ложная: наказываем подавшего, возвращаем доверие обвинённому */
    if (author) {
      author.trust = Math.max(0, (author.trust || 0) - 15);
      author.reportBlockUntil = now() + 7 * 86400e3;
      serviceMessage(author.id,
        'Жалоба №' + num + ' признана ложной.\n\n' +
        'Доверие снижено на 15%, новые жалобы недоступны 7 дней.' +
        (note ? '\n\nКомментарий команды: ' + note : '') +
        '\n\nЕсли считаете решение ошибочным — напишите разработчику.');
      push(author.id, { type: 'state' });
    }
    if (target) {
      target.trust = Math.min(100, (target.trust || 0) + 10);
      serviceMessage(target.id,
        'Жалоба №' + num + ' против вас признана ложной. Доверие восстановлено на 10%.');
      push(target.id, { type: 'state' });
    }
  } else if (verdict === 'valid') {
    /* Жалоба подтверждена */
    if (target) {
      target.trust = Math.max(0, (target.trust || 0) - 25);
      serviceMessage(target.id,
        'Жалоба №' + num + ' против вас подтверждена командой Newchat.\n\n' +
        'Доверие снижено на 25%.' + (note ? '\n\nПричина: ' + note : '') +
        '\n\nПри повторных нарушениях аккаунт будет заблокирован.');
      push(target.id, { type: 'state' });
    }
    if (author) {
      author.trust = Math.min(100, (author.trust || 0) + 5);
      author.reportBlockUntil = 0;
      serviceMessage(author.id, 'Ваша жалоба №' + num + ' подтверждена. Доверие повышено, ограничение на жалобы снято.');
      push(author.id, { type: 'state' });
    }
  } else {
    /* Предупреждение без санкций — шанс исправиться */
    if (target) {
      serviceMessage(target.id,
        'По жалобе №' + num + ' команда вынесла предупреждение без санкций.' +
        (note ? '\n\n' + note : '') +
        '\n\nДоверие не тронуто. Если ситуация повторится, последствия будут серьёзнее.');
      push(target.id, { type: 'state' });
    }
    if (author) {
      author.reportBlockUntil = 0;
      serviceMessage(author.id, 'По жалобе №' + num + ' вынесено предупреждение нарушителю. Ограничение на подачу жалоб снято.');
      push(author.id, { type: 'state' });
    }
  }
  save();
  send(res, 200, { done: ['решение по №' + num + ': ' + verdict] });
});

route('POST', '/api/dev/report-list', async (req, res, body, user) => {
  if (!isDev(user) && !isCodev(user)) return send(res, 403, { error: 'Только для команды' });
  const list = db.reports.slice(-25).reverse().map(r => ({
    num: r.num, kind: r.kind, time: r.time,
    from: r.authorUsername, against: r.peerUsername,
    verdict: r.verdict || '', msgs: r.snapshot.length
  }));
  send(res, 200, { reports: list });
});

route('POST', '/api/dev/report-data', async (req, res, body, user) => {
  /* Полные данные по протоколу — для ответа на официальный запрос */
  if (!isDev(user)) return send(res, 403, { error: 'Только для разработчиков' });
  const num = Number(body.num);
  const r = db.reports.find(x => x.num === num);
  if (!r) return send(res, 404, { error: 'Протокол №' + num + ' не найден' });
  const target = db.users[r.against];
  const author = db.users[r.from];
  send(res, 200, {
    data: {
      num: r.num,
      kind: r.kind,
      time: r.time,
      opened: r.opened || 0,
      against: {
        name: r.peerName,
        username: r.peerUsername,
        phone: target ? (target.phone ? '+7' + target.phone : 'не привязан') : 'аккаунт удалён',
        email: target ? (target.email || 'не указана') : '',
        created: r.peerCreated,
        trust: target ? target.trust : 0,
        banned: target ? !!target.banned : false
      },
      author: {
        name: r.authorName,
        username: r.authorUsername,
        phone: author ? (author.phone ? '+7' + author.phone : 'не привязан') : '',
        reports: db.reports.filter(x => x.from === r.from).length
      },
      messages: r.snapshot.length
    }
  });
});

route('POST', '/api/dev/broadcast', async (req, res, body, user) => {
  if (!isDev(user)) return send(res, 403, { error: 'Только для разработчиков' });
  const text = String(body.text || '').trim().slice(0, 1000);
  if (!text) return send(res, 400, { error: 'Пустое сообщение' });
  let n = 0;
  for (const u of Object.values(db.users)) {
    if (u.isBot || u.id === user.id) continue;
    serviceMessage(u.id, text);
    push(u.id, { type: 'state' });
    n++;
  }
  send(res, 200, { sent: n });
});

route('POST', '/api/dev/user', async (req, res, body, user) => {
  if (!isDev(user)) return send(res, 403, { error: 'Только для разработчиков' });
  const uname = normUsername(body.username);
  const rec = db.usernames[uname];
  const target = rec && db.users[rec.owner];
  if (!target) return send(res, 404, { error: 'Пользователь не найден' });

  if ((isDev(target) || isCodev(target)) && (body.ban || body.trust !== undefined)) {
    return send(res, 400, { error: 'Разработчика и помощника трогать нельзя' });
  }

  const done = [];
  if (body.info) {
    return send(res, 200, {
      info: {
        id: target.id,
        name: target.name,
        phone: (target.phone || '').replace(/^(\d{3})\d+(\d{2})$/, '$1•••$2'),
        trust: target.trust || 0,
        premium: isPremium(target),
        premiumUntil: target.premiumUntil || 0,
        banned: !!target.banned,
        anon: !!target.anon,
        invites: target.inviteCount || 0,
        usernames: Object.entries(db.usernames).filter(([, v]) => v.owner === target.id && !v.channel).map(([un]) => '@' + un),
        createdAt: target.createdAt,
        lastSeen: target.lastSeen || 0
      }
    });
  }
  if (body.ban === true) { target.banned = true; done.push('аккаунт заблокирован'); }
  if (body.ban === false) { target.banned = false; done.push('аккаунт разблокирован'); }
  if (body.clearPremium) { target.premiumUntil = 0; done.push('премиум снят'); }
  if (body.resetPhoto) { target.photo = null; done.push('аватар сброшен'); }
  if (body.verified === true) { target.verified = true; done.push('галочка выдана'); }
  if (body.verified === false) { target.verified = false; done.push('галочка снята'); }
  if (Number(body.premiumDays)) {
    const d = Number(body.premiumDays);
    const from = target.premiumUntil && target.premiumUntil > now() ? target.premiumUntil : now();
    target.premiumUntil = from + d * 86400e3;
    done.push('премиум +' + d + ' дн.');
  }
  if (typeof body.trust === 'number') {
    target.trust = Math.max(0, Math.min(100, Math.round(body.trust)));
    done.push('доверие ' + target.trust + '%');
  }
  save();
  serviceMessage(target.id, 'Обновление аккаунта от команды Newchat: ' + (done.join(', ') || 'без изменений') + '.');
  push(target.id, { type: 'state' });
  send(res, 200, { done, target: publicUser(target) });
});

/* ---------- Сторисы (премиум) ---------- */

route('POST', '/api/stories/post', async (req, res, body, user) => {
  if (!isPremium(user)) return send(res, 403, { error: 'Сторисы — функция премиума. Пригласите ' + PREMIUM.invites + ' друзей!' });
  const photo = String(body.photo || '');
  const isVideo = /^data:video\//i.test(photo);
  if (isVideo) {
    if (photo.length > MAX_BYTES) return send(res, 400, { error: 'Видео больше ' + MAX_MB + ' МБ' });
  } else if (!/^data:image\/(jpeg|png|webp);base64,/.test(photo) || photo.length > 900000) {
    return send(res, 400, { error: 'Фото не подходит' });
  }
  const mine = db.stories.filter(st => st.user === user.id && now() - st.time < 86400e3);
  if (mine.length >= 10) return send(res, 400, { error: 'Не больше 10 историй в сутки' });

  db.stories.push({ id: uid(), user: user.id, photo, video: isVideo, text: String(body.text || '').slice(0, 100), time: now() });
  save();
  send(res, 200, { stories: storiesFeed(user) });
});

function storiesFeed(user) {
  const fresh = db.stories.filter(st => now() - st.time < 86400e3);
  const byUser = {};
  for (const st of fresh) {
    const author = db.users[st.user];
    if (!author) continue;
    if (author.anon && author.id !== user.id) continue; /* анонимы не светятся */
    if ((author.blocked || {})[user.id]) continue;
    byUser[st.user] = byUser[st.user] || { user: publicUser(author), mine: st.user === user.id, items: [] };
    byUser[st.user].items.push({ id: st.id, photo: st.photo, video: !!st.video, text: st.text, time: st.time });
  }
  return Object.values(byUser).sort((a, b) => (b.mine ? 1 : 0) - (a.mine ? 1 : 0));
}

route('POST', '/api/stories/list', async (req, res, body, user) => {
  send(res, 200, { stories: storiesFeed(user) });
});

route('POST', '/api/stories/delete', async (req, res, body, user) => {
  db.stories = db.stories.filter(st => !(st.id === String(body.id || '') && st.user === user.id));
  save();
  send(res, 200, { stories: storiesFeed(user) });
});

/* ---------- Приватность, блокировки, звук ---------- */

route('POST', '/api/auth/logout', async (req, res, body, user) => {
  const t = (req.headers.authorization || '').replace('Bearer ', '');
  if (db.tokens[t]) delete db.tokens[t];
  save();
  send(res, 200, { ok: true });
});

route('POST', '/api/keys/publish', async (req, res, body, user) => {
  /* Клиент присылает только ПУБЛИЧНЫЙ ключ. Приватный не покидает телефон. */
  const key = String(body.pub || '').slice(0, 800);
  if (!key || !/^[A-Za-z0-9+/=_-]+$/.test(key)) return send(res, 400, { error: 'Неверный ключ' });
  user.pubkey = key;
  user.keyAt = now();
  save();
  send(res, 200, { ok: true });
});

route('POST', '/api/keys/get', async (req, res, body, user) => {
  const uname = normUsername(body.username);
  const rec = db.usernames[uname];
  const target = rec && db.users[rec.owner];
  if (!target) return send(res, 404, { error: 'Пользователь не найден' });
  if (!target.pubkey) {
    /* Позовём собеседника: ключ создаётся при первом входе после обновления */
    if (!target.keyAsked || now() - target.keyAsked > 3600e3) {
      target.keyAsked = now();
      serviceMessage(target.id, (user.name || 'Собеседник') + ' хочет включить с вами секретный чат. Обновите приложение и зайдите в него — ключ шифрования создастся сам.');
      push(target.id, { type: 'state' });
      save();
    }
    return send(res, 400, { error: 'У собеседника ещё нет ключа. Мы отправили ему просьбу зайти в приложение — попробуйте через пару минут.' });
  }
  send(res, 200, { pub: target.pubkey, id: target.id });
});

route('POST', '/api/chats/wallpaper', async (req, res, body, user) => {
  const chat = db.chats[String(body.chatId || '')];
  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });
  let wp = String(body.wp || '').slice(0, 40);
  if (body.custom) {
    /* Своя картинка из галереи */
    const img = String(body.custom || '');
    if (!/^data:image\/(jpeg|png|webp);base64,/.test(img) || img.length > 900000) {
      return send(res, 400, { error: 'Картинка не подходит: до 600 КБ' });
    }
    wp = img;
  }
  const both = !!body.both;
  const pid = chat.members.find(m => m !== user.id);

  if (!both) {
    /* Только для себя */
    chat.wpFor = chat.wpFor || {};
    chat.wpFor[user.id] = wp;
    save();
    return send(res, 200, { chats: userChats(user.id) });
  }

  /* Для обоих — нужно согласие собеседника */
  chat.wpOffer = { from: user.id, wp, time: now() };
  askInChat(chat, user.id, 'wp', { wp, name: user.name || '' });
  save();
  if (pid) push(pid, { type: 'state' });
  send(res, 200, { offered: true, chats: userChats(user.id) });
});

route('POST', '/api/chats/wallpaper/answer', async (req, res, body, user) => {
  const chat = db.chats[String(body.chatId || '')];
  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });
  if (!chat.wpOffer || chat.wpOffer.from === user.id) return send(res, 400, { error: 'Нет предложения' });
  const who = db.users[chat.wpOffer.from];
  if (body.accept) {
    chat.wp = chat.wpOffer.wp;
    chat.wpFor = {};
    answerInChat(chat, 'wp', 'ok');
    if (who) push(who.id, { type: 'state' });
  } else {
    answerInChat(chat, 'wp', 'no');
    if (who) push(who.id, { type: 'state' });
  }
  chat.wpOffer = null;
  save();
  send(res, 200, { chats: userChats(user.id) });
});

route('POST', '/api/chats/secret', async (req, res, body, user) => {
  const chat = db.chats[String(body.chatId || '')];
  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });
  if (chat.type === 'channel' || chat.service) return send(res, 400, { error: 'Только для личных чатов' });
  const pid = chat.members.find(m => m !== user.id);

  /* Выключить можно в одиночку — это не вредит собеседнику */
  if (!body.on) {
    chat.secret = false;
    chat.secretOffer = null;
    save();
    if (pid) {
      serviceMessage(pid, 'Секретный чат выключен.');
      push(pid, { type: 'state' });
    }
    return send(res, 200, { secret: false, chats: userChats(user.id) });
  }

  /* Включить — только с согласия второго: иначе доксеры прятались бы
     за шифрованием, чтобы на них нельзя было пожаловаться */
  if (chat.secret) return send(res, 200, { secret: true, chats: userChats(user.id) });
  chat.secretOffer = { from: user.id, time: now() };
  askInChat(chat, user.id, 'secret', { name: user.name || '' });
  save();
  if (pid) push(pid, { type: 'state' });
  send(res, 200, { offered: true, chats: userChats(user.id) });
});

route('POST', '/api/chats/secret/answer', async (req, res, body, user) => {
  const chat = db.chats[String(body.chatId || '')];
  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });
  if (!chat.secretOffer || chat.secretOffer.from === user.id) return send(res, 400, { error: 'Нет предложения' });
  const who = db.users[chat.secretOffer.from];
  if (body.accept) {
    chat.secret = true;
    answerInChat(chat, 'secret', 'ok');
    if (who) push(who.id, { type: 'state' });
  } else {
    answerInChat(chat, 'secret', 'no');
    if (who) push(who.id, { type: 'state' });
  }
  chat.secretOffer = null;
  save();
  send(res, 200, { chats: userChats(user.id) });
});

route('POST', '/api/chats/read', async (req, res, body, user) => {
  const chat = db.chats[String(body.chatId || '')];
  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });
  user.reads = user.reads || {};
  user.reads[chat.id] = now();
  save();
  /* Собеседник увидит двойные галочки сразу */
  if (chat.type !== 'channel' && !chat.service) {
    const pid = chat.members.find(m => m !== user.id);
    if (pid) push(pid, { type: 'read', chatId: chat.id, at: user.reads[chat.id] });
  }
  send(res, 200, { ok: true });
});

route('POST', '/api/chats/mute', async (req, res, body, user) => {
  const chat = db.chats[String(body.chatId || '')];
  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });
  user.muted = user.muted || {};
  if (body.on) user.muted[chat.id] = true;
  else delete user.muted[chat.id];
  save();
  send(res, 200, { muted: !!user.muted[chat.id], chats: userChats(user.id) });
});

/* ---------- Поиск ---------- */

route('POST', '/api/search', async (req, res, body, user) => {
  const q = String(body.q || '').trim().toLowerCase().replace(/^@+/, '').slice(0, 30);
  if (q.length < 2) return send(res, 200, { results: [] });

  const results = [];
  const seen = {};

  /* Ищем по всем юзернеймам, включая дополнительные */
  for (const [uname, rec] of Object.entries(db.usernames)) {
    if (rec.channel || !uname.includes(q)) continue;
    const u = db.users[rec.owner];
    if (!u || u.id === user.id || !u.username || seen[u.id]) continue;
    if (u.anon || (u.blocked || {})[user.id]) continue;
    seen[u.id] = 1;
    results.push({ kind: u.isBot ? 'bot' : 'user', item: publicUser(u) });
    if (results.length >= 15) break;
  }
  /* И по именам */
  for (const u of Object.values(db.users)) {
    if (results.length >= 15) break;
    if (u.id === user.id || !u.username || seen[u.id]) continue;
    if (u.anon || (u.blocked || {})[user.id]) continue;
    if ((u.name || '').toLowerCase().includes(q)) {
      seen[u.id] = 1;
      results.push({ kind: u.isBot ? 'bot' : 'user', item: publicUser(u) });
    }
  }

  for (const c of Object.values(db.chats)) {
    if (c.type !== 'channel') continue;
    const hit = (c.title || '').toLowerCase().includes(q) || (c.uname || '').includes(q);
    if (hit) results.push({
      kind: 'channel',
      item: { id: c.id, name: c.title, username: c.uname || '', subs: c.members.length, member: c.members.includes(user.id) }
    });
    if (results.length >= 25) break;
  }

  send(res, 200, { results });
});

/* ---------- Каналы ---------- */

route('POST', '/api/channels/create', async (req, res, body, user) => {
  const title = String(body.title || '').trim().slice(0, 40);
  const uname = normUsername(body.username);
  if (!title) return send(res, 400, { error: 'Введите название канала' });
  if (uname && uname.length < 5) return send(res, 400, { error: 'Юзернейм канала — минимум 5 символов' });
  if (uname && db.usernames[uname]) return send(res, 400, { error: 'Этот юзернейм уже занят' });

  const mine = Object.values(db.chats).filter(c => c.type === 'channel' && c.owner === user.id);
  if (mine.length >= 5) return send(res, 400, { error: 'Не больше 5 каналов на аккаунт' });

  const id = 'ch:' + uid();
  db.chats[id] = { id, type: 'channel', title, uname: uname || '', owner: user.id, members: [user.id], msgs: [] };
  if (uname) db.usernames[uname] = { owner: user.id, main: false, forSale: false, price: 0, channel: id };
  save();
  send(res, 200, { chatId: id, chats: userChats(user.id) });
});

route('POST', '/api/channels/join', async (req, res, body, user) => {
  const chat = db.chats[String(body.chatId || '')];
  if (!chat || chat.type !== 'channel') return send(res, 404, { error: 'Канал не найден' });
  if (!chat.members.includes(user.id)) {
    chat.members.push(user.id);
    save();
    push(chat.owner, { type: 'state' });
  }
  send(res, 200, { chatId: chat.id, chats: userChats(user.id) });
});

route('POST', '/api/channels/leave', async (req, res, body, user) => {
  const chat = db.chats[String(body.chatId || '')];
  if (!chat || chat.type !== 'channel') return send(res, 404, { error: 'Канал не найден' });
  if (chat.owner === user.id) return send(res, 400, { error: 'Владелец не может покинуть свой канал' });
  chat.members = chat.members.filter(m => m !== user.id);
  save();
  send(res, 200, { chats: userChats(user.id) });
});

/* ---------- Боты ---------- */
/* Мини-платформа как BotFather: создал бота — получил токен.
   Дальше своим кодом опрашиваешь /api/bot/<токен>/updates и
   отвечаешь через /api/bot/<токен>/send. */

route('POST', '/api/bots/create', async (req, res, body, user) => {
  const name = String(body.name || '').trim().slice(0, 30);
  const uname = normUsername(body.username);

  if (!name) return send(res, 400, { error: 'Введите имя бота' });
  if (!uname.endsWith('bot')) return send(res, 400, { error: 'Юзернейм бота должен заканчиваться на «bot»' });
  if (uname.length < 5) return send(res, 400, { error: 'Юзернейм — минимум 5 символов' });
  if (db.usernames[uname]) return send(res, 400, { error: 'Этот юзернейм уже занят' });
  if (myBots(user.id).length >= BOTS_PER_USER) return send(res, 400, { error: `Не больше ${BOTS_PER_USER} ботов на аккаунт` });

  const bot = {
    id: uid(), isBot: true, owner: user.id,
    name, username: uname, trust: 100, createdAt: now()
  };
  db.users[bot.id] = bot;
  db.usernames[uname] = { owner: bot.id, main: true, forSale: false, price: 0 };

  const token = bot.id + ':' + crypto.randomBytes(16).toString('hex');
  db.botTokens[token] = bot.id;
  db.botUpdates[bot.id] = [];
  save();

  send(res, 200, { bot: { id: bot.id, name, username: uname, token }, bots: myBots(user.id) });
});

route('POST', '/api/bots/delete', async (req, res, body, user) => {
  const bot = db.users[String(body.botId || '')];
  if (!bot || !bot.isBot || bot.owner !== user.id) return send(res, 404, { error: 'Бот не найден' });

  for (const t of Object.keys(db.botTokens)) if (db.botTokens[t] === bot.id) delete db.botTokens[t];
  delete db.botUpdates[bot.id];
  if (bot.username && db.usernames[bot.username]) delete db.usernames[bot.username];
  delete db.users[bot.id];
  save();
  send(res, 200, { bots: myBots(user.id) });
});

/* API для кода бота — авторизация токеном в адресе */
async function handleBotApi(req, res, urlPath, body) {
  const m = urlPath.match(/^\/api\/bot\/([^/]+)\/(updates|send|me)$/);
  if (!m) return send(res, 404, { error: 'Не найдено' });

  const botId = db.botTokens[m[1]];
  const bot = botId && db.users[botId];
  if (!bot) return send(res, 401, { error: 'Неверный токен бота' });

  if (m[2] === 'me') return send(res, 200, { bot: publicUser(bot) });

  if (m[2] === 'updates') {
    const offset = Number(body.offset || 0);
    const list = (db.botUpdates[bot.id] || []).filter(u => u.update_id > offset);
    return send(res, 200, { updates: list.slice(0, 100) });
  }

  /* send */
  const chat = db.chats[String(body.chatId || '')];
  const text = String(body.text || '').trim().slice(0, 4000);
  if (!chat || !chat.members.includes(bot.id)) return send(res, 404, { error: 'Чат не найден' });
  if (!text) return send(res, 400, { error: 'Пустое сообщение' });

  const msg = { id: uid(), from: bot.id, text, time: now(), deleted: false };
  chat.msgs.push(msg);
  save();
  const peerId = chat.members.find(x => x !== bot.id);
  if (peerId) { push(peerId, { type: 'message', chatId: chat.id, message: { id: msg.id, text, time: msg.time, out: false } }); notifyPush(peerId); }
  send(res, 200, { ok: true, messageId: msg.id });
}

/* ---------- Премиум за приглашения ---------- */

function awardInvite(inviter) {
  inviter.inviteCount = (inviter.inviteCount || 0) + 1;
  if (inviter.inviteCount % PREMIUM.invites === 0) {
    const from = isPremium(inviter) ? inviter.premiumUntil : now();
    inviter.premiumUntil = from + PREMIUM.days * 86400e3;
    const until = new Date(inviter.premiumUntil).toLocaleDateString('ru');
    serviceMessage(inviter.id, `Вы пригласили ${inviter.inviteCount} друзей — премиум активен до ${until}! Слотов под юзернеймы: ${PREMIUM.slots}.`);
  } else {
    const left = PREMIUM.invites - (inviter.inviteCount % PREMIUM.invites);
    serviceMessage(inviter.id, `По вашей ссылке зарегистрировался новый человек. До премиума осталось приглашений: ${left}.`);
  }
  push(inviter.id, { type: 'state' });
}

/* ---------- Юзернеймы и биржа ---------- */

route('POST', '/api/usernames/claim', async (req, res, body, user) => {
  const username = normUsername(body.username);
  const mine = myUsernames(user.id);
  const limit = slotLimit(user);

  if (username.length < 5) return send(res, 400, { error: 'Минимум 5 символов' });
  if (db.usernames[username]) return send(res, 400, { error: 'Этот юзернейм уже занят' });
  if (mine.length >= limit) {
    return send(res, 400, {
      error: isPremium(user) ? 'Все слоты заняты' : `Занято ${limit} из ${limit}. Премиум даёт ${PREMIUM.slots} слотов.`
    });
  }

  db.usernames[username] = { owner: user.id, main: false, forSale: false, price: 0 };
  save();
  marketChanged();
  send(res, 200, { usernames: myUsernames(user.id), market: marketList(user.id) });
});

route('POST', '/api/usernames/sell', async (req, res, body, user) => {
  const username = normUsername(body.username);
  const price = Math.round(Number(body.price));
  const rec = db.usernames[username];

  if (!rec || rec.owner !== user.id) return send(res, 403, { error: 'Это не ваш юзернейм' });
  if (rec.channel) return send(res, 400, { error: 'Юзернейм канала продать нельзя' });
  if (rec.frozen) return send(res, 400, { error: 'Юзернейм в активной сделке' });
  if (!(price > 0)) return send(res, 400, { error: 'Укажите цену' });
  if (rec.main) {
    const spare = Object.entries(db.usernames).find(([un, v]) =>
      v.owner === user.id && un !== username && !v.channel && !v.frozen);
    if (!spare) return send(res, 400, { error: 'Это ваш единственный юзернейм — сначала займите запасной, он станет основным' });
  }
  if (!user.phone) return send(res, 403, { error: 'Для продажи привяжите телефон в профиле — так покупатели знают, с кем имеют дело' });
  if (!isDev(user) && !isCodev(user) && user.reportBlockUntil && user.reportBlockUntil > now()) return send(res, 403, { error: 'Биржа закрыта на 2 суток после подачи жалобы' });
  if ((user.trust || 0) < SELL_MIN_TRUST) return send(res, 400, { error: 'Продавать можно с доверием от ' + SELL_MIN_TRUST + '%' });
  if (!user.requisites) return send(res, 400, { error: 'Сначала укажите реквизиты в «Сделках» — их увидит покупатель' });

  rec.forSale = true;
  rec.price = price;
  save();
  marketChanged();
  send(res, 200, { usernames: myUsernames(user.id), market: marketList(user.id) });
});

route('POST', '/api/usernames/delete', async (req, res, body, user) => {
  const username = normUsername(body.username);
  const rec = db.usernames[username];
  if (!rec || rec.owner !== user.id) return send(res, 403, { error: 'Это не ваш юзернейм' });
  if (rec.main) {
    const spare = Object.entries(db.usernames).find(([un, v]) =>
      v.owner === user.id && un !== username && !v.channel && !v.frozen);
    if (!spare) return send(res, 400, { error: 'Это ваш единственный юзернейм — без него не войти' });
    spare[1].main = true;
    user.username = spare[0];
  }
  if (rec.channel) return send(res, 400, { error: 'Это юзернейм канала' });
  if (rec.frozen) return send(res, 400, { error: 'Юзернейм в активной сделке' });
  delete db.usernames[username]; /* снова свободен для всех */
  save();
  marketChanged();
  send(res, 200, { usernames: myUsernames(user.id), market: marketList(user.id) });
});

route('POST', '/api/usernames/unsell', async (req, res, body, user) => {
  const username = normUsername(body.username);
  const rec = db.usernames[username];
  if (!rec || rec.owner !== user.id) return send(res, 403, { error: 'Это не ваш юзернейм' });
  if (rec.frozen) return send(res, 400, { error: 'Юзернейм в активной сделке' });
  rec.forSale = false;
  rec.price = 0;
  save();
  marketChanged();
  send(res, 200, { usernames: myUsernames(user.id), market: marketList(user.id) });
});

/* ---------- Верификация ---------- */

route('POST', '/api/verify/request', async (req, res, body, user) => {
  const name = String(body.name || '').trim().slice(0, 60);
  const kind = ['company', 'public', 'dev'].includes(body.kind) ? body.kind : 'company';
  if (!name) return send(res, 400, { error: 'Укажите название или имя' });

  db.verifyRequests.push({ user: user.id, name, kind, proof: String(body.proof || '').slice(0, 200), time: now() });
  save();

  const labels = { company: 'компании', public: 'известной личности', dev: 'разработчика' };
  serviceMessage(user.id, `Заявка на верификацию ${labels[kind]} («${name}») принята. Команда рассмотрит её вручную.`);
  send(res, 200, { ok: true });
});

/* ---------- Вход через Telegram ---------- */

route('GET', '/api/config', async (req, res) => {
  send(res, 200, {
    telegram: TG_ENABLED,
    botName: TG_BOT_NAME,
    sms: SMS_ENABLED,
    mail: MAIL_ENABLED,
    team: DEV_USERNAMES.concat(CODEV_USERNAMES).slice(0, 6),
    ice: iceServers(),
    vapid: VAPID_PUBLIC,
    maxMb: MAX_MB,
    musicMb: MUSIC_MB,
    videoDays: MEDIA_KEEP_DAYS,
    deal: { payHours: DEAL.payHours, confirmDays: DEAL.confirmDays }
  });
});

route('POST', '/api/auth/telegram/start', async (req, res) => {
  if (!TG_ENABLED) return send(res, 400, { error: 'Вход через Telegram не настроен' });

  /* Чистим сессии старше 15 минут и уже использованные */
  const cutoff = now() - 15 * 60e3;
  for (const s of Object.keys(db.tgSessions)) {
    const r = db.tgSessions[s];
    if (r.created < cutoff || (r.usedAt && r.usedAt < now() - 2 * 60e3)) delete db.tgSessions[s];
  }

  const session = crypto.randomBytes(12).toString('hex');
  db.tgSessions[session] = { created: now(), chatId: null, status: 'pending', token: null };
  save();

  send(res, 200, {
    session,
    link: `https://t.me/${TG_BOT_NAME}?start=${session}`
  });
});

route('POST', '/api/auth/telegram/check', async (req, res, body) => {
  const key = String(body.session || '');
  const rec = db.tgSessions[key];
  if (!rec) return send(res, 404, { error: 'Сессия не найдена, начните заново' });
  if (rec.created < now() - 15 * 60e3) return send(res, 400, { error: 'Время вышло, начните заново' });
  if (rec.status !== 'ok' && rec.status !== 'used') return send(res, 200, { status: 'pending' });

  const user = db.users[db.tokens[rec.token]];
  if (!user) return send(res, 404, { error: 'Сессия не найдена, начните заново' });
  const token = rec.token;

  /* Сессию не удаляем сразу: приложение опрашивает сервер параллельно
     (таймер + возврат из Telegram), и второй запрос получал бы 404,
     выкидывая человека обратно на вход. Держим две минуты, потом чистим. */
  rec.status = 'used';
  rec.usedAt = now();
  save();

  send(res, 200, {
    status: 'ok',
    token,
    needsSetup: !user.username,
    state: user.username ? fullState(user) : null
  });
});

route('POST', '/telegram/webhook', async (req, res, body) => {
  if (req.headers['x-telegram-bot-api-secret-token'] !== TG_SECRET) {
    return send(res, 403, { error: 'forbidden' });
  }
  send(res, 200, { ok: true });
  handleTelegramUpdate(body).catch(e => console.error('Ошибка обработки:', e.message));
});

/* ---------- Служебное ---------- */

route('GET', '/api/health', async (req, res) => {
  send(res, 200, {
    ok: true,
    users: Object.keys(db.users).length,
    storage: pgPool ? 'postgres' : 'file',
    persistent: !!pgPool
  });
});

/* ================= СЕРВЕР ================= */

const OPEN_ROUTES = [
  'POST /api/auth/request',
  'POST /api/auth/verify',
  'GET /api/mailcheck',
  'POST /api/auth/email/request',
  'POST /api/auth/email/verify',
  'POST /api/auth/telegram/start',
  'POST /api/auth/telegram/check',
  'POST /telegram/webhook',
  'POST /api/admin/deals',
  'GET /api/config',
  'GET /api/health',
  'GET /api/health'
];


/* ===== PUSH-УВЕДОМЛЕНИЯ ===== */
const VAPID_PUBLIC = process.env.VAPID_PUBLIC || 'BGA17iH6l25CJBuj94BkyOxiSjqU9Y3DMSTe-yrCnYBkQ6zWVngCz_oRu53O7tNNFknpLfU5NmLYpSvHCXYDCLs';
const VAPID_PRIVATE = process.env.VAPID_PRIVATE || 'vOrmFqu0vvr-cYlc_MQPqu7dn-D3zul3HCvwnFSlO7I';
const VAPID_SUBJECT = process.env.VAPID_SUBJECT || 'mailto:newchat@example.com';

function b64url(buf) {
  return Buffer.from(buf).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}
function vapidJwt(audience) {
  const header = b64url(JSON.stringify({ typ: 'JWT', alg: 'ES256' }));
  const payload = b64url(JSON.stringify({ aud: audience, exp: Math.floor(Date.now() / 1000) + 12 * 3600, sub: VAPID_SUBJECT }));
  const data = header + '.' + payload;
  const key = crypto.createPrivateKey({
    key: { kty: 'EC', crv: 'P-256', d: VAPID_PRIVATE,
           x: b64url(Buffer.from(VAPID_PUBLIC, 'base64url').slice(1, 33)),
           y: b64url(Buffer.from(VAPID_PUBLIC, 'base64url').slice(33, 65)) },
    format: 'jwk'
  });
  return data + '.' + b64url(crypto.sign('sha256', Buffer.from(data), { key, dsaEncoding: 'ieee-p1363' }));
}
function pushWeb(sub, ttl) {
  return new Promise(resolve => {
    try {
      const https = require('https');
      const { URL } = require('url');
      const u = new URL(sub.endpoint);
      const req = https.request({
        hostname: u.hostname, path: u.pathname + u.search, method: 'POST',
        headers: { 'TTL': String(ttl || 3600), 'Content-Length': 0, 'Urgency': 'high',
                   'Authorization': 'vapid t=' + vapidJwt(u.origin) + ', k=' + VAPID_PUBLIC }
      }, res => {
        if (res.statusCode === 404 || res.statusCode === 410) sub.dead = true;
        res.resume(); resolve(res.statusCode);
      });
      req.setTimeout(8000, () => { req.destroy(); resolve(0); });
      req.on('error', () => resolve(0));
      req.end();
    } catch (e) { resolve(0); }
  });
}
function notifyPush(userId) {
  const u = db.users[userId];
  if (!u || !u.pushSubs || !u.pushSubs.length) return;
  if (isOnline(userId)) return;
  for (const sub of u.pushSubs) if (!sub.dead) pushWeb(sub, 3600);
  u.pushSubs = u.pushSubs.filter(x => !x.dead);
}

/* Запрос согласия прямо в переписке: обе стороны видят карточку с кнопками */
function askInChat(chat, fromId, kind, extra) {
  const msg = {
    id: uid(), from: fromId, text: '', time: now(), deleted: false,
    req: Object.assign({ kind, status: 'pending', from: fromId }, extra || {})
  };
  chat.msgs.push(msg);
  save();
  const payload = { id: msg.id, text: '', req: msg.req, time: msg.time, out: false };
  for (const m of chat.members) {
    if (m === fromId) continue;
    push(m, { type: 'message', chatId: chat.id, message: payload });
  }
  return msg;
}

function answerInChat(chat, kind, status) {
  for (let i = chat.msgs.length - 1; i >= 0; i--) {
    const m = chat.msgs[i];
    if (m.req && m.req.kind === kind && m.req.status === 'pending') {
      m.req.status = status;
      save();
      for (const u of chat.members) push(u, { type: 'state' });
      return m;
    }
  }
  return null;
}

/* Биржа изменилась — короткий сигнал, не чаще раза в 5 секунд */
let lastMarketPing = 0;
function marketChanged() {
  if (now() - lastMarketPing < 5000) return;
  lastMarketPing = now();
  for (const u of Object.values(db.users)) {
    if (u.isBot || !isOnline(u.id)) continue;
    push(u.id, { type: 'market' });
  }
}

/* ===== КОЛЛЕКЦИОННЫЕ КАРТОЧКИ ===== */
const GIFT_TYPES = {
  aegis: {
    name: 'Aegis', total: 50, rarity: 'Эпическая', rarityNum: 4,
    desc: 'Щит Newchat — награда первым, кто настроил защиту своего аккаунта. Отчеканено 50 штук, больше не будет.',
    quest: true
  },
  devcoin: {
    name: 'DevCoin', total: 2, rarity: 'Легендарная', rarityNum: 5,
    desc: 'Монета создателей Newchat. Отчеканено две — по числу тех, кто писал этот мессенджер с нуля.',
    devOnly: true
  }
};

function myGifts(userId) {
  return (db.gifts || []).filter(g => g.owner === userId).map(g => {
    const t = GIFT_TYPES[g.type] || (db.giftTypes || {})[g.type] || {};
    const sales = (db.giftSales || []).filter(s => s.type === g.type);
    const last = sales.length ? sales[sales.length - 1] : null;
    return {
      id: g.id, type: g.type, num: g.num,
      forSale: !!g.forSale, price: g.price || 0, frozen: !!g.frozen,
      name: t.name || g.type, total: t.total || 0,
      rarity: t.rarity || '', rarityNum: t.rarityNum || 1,
      desc: t.desc || '', issued: g.time,
      lastPrice: last ? last.price : 0,
      lastSaleAt: last ? last.time : 0,
      salesCount: sales.length
    };
  });
}

function grantDevCoin(user) {
  db.gifts = db.gifts || [];
  let changed = false;
  const devs = Object.values(db.users).filter(u => !u.isBot && (isDev(u) || isCodev(u)));
  for (const d of devs) {
    if ((d.giftDenied || []).includes('devcoin')) continue;
    if (db.gifts.some(g => g.type === 'devcoin' && g.owner === d.id)) continue;
    const minted = db.gifts.filter(g => g.type === 'devcoin').length;
    if (minted >= GIFT_TYPES.devcoin.total) break;
    db.gifts.push({ id: uid(), type: 'devcoin', num: minted + 1, owner: d.id, time: now() });
    changed = true;
    serviceMessage(d.id, 'Вам выдана коллекционная монета DevCoin №' + (minted + 1) + ' из ' + GIFT_TYPES.devcoin.total + '. Она в вашем профиле.');
    push(d.id, { type: 'state' });
  }
  if (changed) save();
}

/* Три задания на «Aegis» */
function aegisQuests(user) {
  const hasChat = Object.values(db.chats).some(c =>
    !c.service && c.type !== 'channel' && c.members.includes(user.id) &&
    c.msgs.some(m => m.from === user.id));
  const hasSecret = Object.values(db.chats).some(c => c.members.includes(user.id) && c.secret);
  return [
    { id: 'profile', title: 'Оформите профиль', hint: 'Аватар и пара слов о себе', done: !!(user.photo && user.bio) },
    { id: 'talk', title: 'Напишите кому-нибудь', hint: 'Хотя бы одно сообщение в личном чате', done: hasChat },
    { id: 'secret', title: 'Включите секретный чат', hint: 'Меню чата → «Секретный чат», с согласия собеседника', done: hasSecret }
  ];
}

const QUEST_MS = 60 * 60e3;

function grantAegis(user) {
  db.gifts = db.gifts || [];
  if ((user.giftDenied || []).includes('aegis')) return false;
  if (db.gifts.some(g => g.type === 'aegis' && g.owner === user.id)) return false;
  if (!user.questStart || now() - user.questStart > QUEST_MS) return false;
  if (!aegisQuests(user).every(q => q.done)) return false;
  const minted = db.gifts.filter(g => g.type === 'aegis').length;
  if (minted >= GIFT_TYPES.aegis.total) return false;
  db.gifts.push({ id: uid(), type: 'aegis', num: minted + 1, owner: user.id, time: now() });
  save();
  serviceMessage(user.id, 'Все задания выполнены! Вам вручён щит «Aegis» №' + (minted + 1) + ' из ' + GIFT_TYPES.aegis.total + '. Он в вашем профиле.');
  push(user.id, { type: 'state' });
  return true;
}

/* Анонимный номер команде: +0 и девять цифр */
function ensureAnonPhone(user) {
  if (!user || user.anonPhone) return;
  if (!isDev(user) && !isCodev(user)) return;
  let num;
  do {
    num = '0' + String(Math.floor(1000000000 + Math.random() * 8999999999));
  } while (Object.values(db.users).some(u => u.anonPhone === num));
  user.anonPhone = num;
  save();
  const pretty = '+0 ' + num.slice(1, 4) + ' ' + num.slice(4, 7) + '-' + num.slice(7, 9) + '-' + num.slice(9, 11);
  serviceMessage(user.id, 'Вам выдан анонимный номер ' + pretty +
    '. Настоящий телефон скрыт — другие видят только +0 ' + num.slice(1, 4) + ' •••-••-' + num.slice(-2) + '.');
}

route('POST', '/api/quests', async (req, res, body, user) => {
  let minted = (db.gifts || []).filter(g => g.type === 'aegis').length;
  let mine = (db.gifts || []).some(g => g.type === 'aegis' && g.owner === user.id);
  const soldOut = minted >= GIFT_TYPES.aegis.total;
  if (body.start && !user.questStart && !mine && !soldOut) { user.questStart = now(); save(); }
  const got = grantAegis(user);
  if (got) { minted = (db.gifts || []).filter(g => g.type === 'aegis').length; mine = true; }
  const left = user.questStart ? Math.max(0, QUEST_MS - (now() - user.questStart)) : QUEST_MS;
  send(res, 200, {
    name: GIFT_TYPES.aegis.name, desc: GIFT_TYPES.aegis.desc, rarity: GIFT_TYPES.aegis.rarity,
    quests: aegisQuests(user), have: mine, started: !!user.questStart,
    msLeft: mine ? 0 : left, expired: !!user.questStart && left <= 0 && !mine,
    minted, total: GIFT_TYPES.aegis.total, soldOut, justGot: got
  });
});

route('POST', '/api/dev/gift-take', async (req, res, body, user) => {
  /* Забрать карточку у человека */
  if (!isDev(user)) return send(res, 403, { error: 'Только для разработчиков' });
  const uname = normUsername(body.username);
  const rec = db.usernames[uname];
  const target = rec && db.users[rec.owner];
  if (!target) return send(res, 404, { error: 'Пользователь не найден' });

  db.gifts = db.gifts || [];
  const type = String(body.type || '').trim().toLowerCase();
  const own = db.gifts.filter(g => g.owner === target.id && (!type || g.type === type));
  if (!own.length) return send(res, 404, { error: type ? ('У @' + uname + ' нет карточки «' + type + '»') : ('У @' + uname + ' нет карточек') });

  const taken = [];
  target.giftDenied = target.giftDenied || [];
  for (const g of own) {
    if (g.frozen) { taken.push('«' + g.type + '» в сделке — пропущена'); continue; }
    const t = GIFT_TYPES[g.type] || (db.giftTypes || {})[g.type] || {};
    db.gifts = db.gifts.filter(x => x.id !== g.id);
    /* Помечаем, иначе карточка выдастся заново на следующем же запросе */
    if (!target.giftDenied.includes(g.type)) target.giftDenied.push(g.type);
    taken.push((t.name || g.type) + ' №' + g.num);
  }
  save();
  serviceMessage(target.id, 'Команда Newchat изъяла у вас карточку: ' + taken.join(', ') + '.' +
    (body.reason ? '\n\nПричина: ' + String(body.reason).slice(0, 200) : ''));
  push(target.id, { type: 'state' });
  send(res, 200, { done: ['изъято у @' + uname + ': ' + taken.join(', ')] });
});

route('POST', '/api/gifts/list', async (req, res, body, user) => {
  grantDevCoin(user);
  send(res, 200, { gifts: myGifts(user.id) });
});

route('POST', '/api/gifts/sell', async (req, res, body, user) => {
  const g = (db.gifts || []).find(x => x.id === String(body.id || ''));
  if (!g || g.owner !== user.id) return send(res, 403, { error: 'Это не ваша карточка' });
  if (g.frozen) return send(res, 400, { error: 'Карточка в сделке' });
  if (!user.phone) return send(res, 403, { error: 'Для продажи привяжите телефон в профиле' });
  if (!user.requisites) return send(res, 400, { error: 'Сначала укажите реквизиты для получения денег' });
  const price = Math.round(Number(body.price) || 0);
  if (!(price > 0)) return send(res, 400, { error: 'Укажите цену' });
  if (price > 5000000) return send(res, 400, { error: 'Слишком большая цена' });
  g.forSale = true; g.price = price;
  save(); marketChanged();
  send(res, 200, { gifts: myGifts(user.id), giftMarket: giftMarket(user.id) });
});

route('POST', '/api/gifts/unsell', async (req, res, body, user) => {
  const g = (db.gifts || []).find(x => x.id === String(body.id || ''));
  if (!g || g.owner !== user.id) return send(res, 403, { error: 'Это не ваша карточка' });
  if (g.frozen) return send(res, 400, { error: 'Идёт сделка — снять нельзя' });
  g.forSale = false; g.price = 0;
  save(); marketChanged();
  send(res, 200, { gifts: myGifts(user.id), giftMarket: giftMarket(user.id) });
});

route('POST', '/api/gifts/buy', async (req, res, body, user) => {
  const g = (db.gifts || []).find(x => x.id === String(body.id || ''));
  if (!g || !g.forSale) return send(res, 404, { error: 'Карточка не продаётся' });
  if (g.owner === user.id) return send(res, 400, { error: 'Это ваша карточка' });
  if (g.frozen) return send(res, 400, { error: 'По карточке уже идёт сделка' });
  if (!user.phone) return send(res, 403, { error: 'Для покупки привяжите телефон в профиле' });
  const seller = db.users[g.owner];
  if (!seller || !seller.requisites) return send(res, 400, { error: 'У продавца нет реквизитов' });
  const t = GIFT_TYPES[g.type] || (db.giftTypes || {})[g.type] || {};
  const deal = {
    id: uid(), kind: 'gift', giftId: g.id,
    username: (t.name || g.type) + ' №' + g.num,
    seller: seller.id, buyer: user.id, price: g.price, status: 'pay',
    createdAt: now(), requisites: seller.requisites
  };
  db.deals[deal.id] = deal;
  g.frozen = deal.id;
  save();
  serviceMessage(seller.id, 'Покупатель хочет забрать вашу карточку «' + deal.username + '» за ' + g.price + ' ₽. Ожидайте перевод.');
  push(seller.id, { type: 'state' });
  push(user.id, { type: 'state' });
  marketChanged();
  send(res, 200, { deal: dealView(deal, user.id), state: fullState(user) });
});

route('GET', '/api/health', async (req, res, body, user) => {
  /* Открытая проверка: куда сохраняются данные и сколько их */
  send(res, 200, {
    ok: true,
    storage: pgPool ? 'postgres' : 'file',
    warning: pgPool ? '' : 'Данные в файле — пропадут при перезапуске сервера. Задайте DATABASE_URL.',
    users: Object.keys(db.users).length,
    chats: Object.keys(db.chats).length,
    messages: Object.values(db.chats).reduce((a, c) => a + (c.msgs ? c.msgs.length : 0), 0),
    gifts: (db.gifts || []).length,
    uptimeMin: Math.round(process.uptime() / 60),
    build: 'v54'
  });
});

route('GET', '/api/market', async (req, res, body, user) => {
  send(res, 200, {
    market: marketList(user.id),
    giftMarket: giftMarket(user.id),
    gifts: myGifts(user.id),
    usernames: myUsernames(user.id),
    deals: Object.values(db.deals)
      .filter(d => d.seller === user.id || d.buyer === user.id)
      .sort((a, b) => b.createdAt - a.createdAt).slice(0, 20)
      .map(d => dealView(d, user.id))
  });
});

route('POST', '/api/push/subscribe', async (req, res, body, user) => {
  const sub = body.sub;
  if (!sub || !sub.endpoint) return send(res, 400, { error: 'Нет подписки' });
  user.pushSubs = (user.pushSubs || []).filter(s => s.endpoint !== sub.endpoint);
  user.pushSubs.push({ endpoint: String(sub.endpoint).slice(0, 500), time: now() });
  if (user.pushSubs.length > 5) user.pushSubs = user.pushSubs.slice(-5);
  save();
  send(res, 200, { ok: true });
});

route('POST', '/api/push/unsubscribe', async (req, res, body, user) => {
  user.pushSubs = (user.pushSubs || []).filter(s => s.endpoint !== body.endpoint);
  save();
  send(res, 200, { ok: true });
});

route('POST', '/api/push/peek', async (req, res, body, user) => {
  let best = null;
  for (const c of Object.values(db.chats)) {
    if (!c.members.includes(user.id)) continue;
    const readAt = (user.reads || {})[c.id] || 0;
    for (const m of c.msgs) {
      if (m.from === user.id || m.deleted || m.time <= readAt) continue;
      if (!best || m.time > best.time) {
        const author = db.users[m.from];
        best = { time: m.time, chatId: c.id,
          name: c.service ? 'Newchat' : ((author && author.name) || 'Сообщение'),
          text: m.enc ? 'Зашифрованное сообщение' : (m.text || (m.media ? 'Вложение' : '')).slice(0, 120) };
      }
    }
  }
  send(res, 200, { msg: best });
});


/* Оформление профиля: описание, обложка, музыка, свои звуки */
route('POST', '/api/profile/style', async (req, res, body, user) => {
  if (body.ringtone !== undefined) {
    const r = body.ringtone;
    if (!r) { user.ringtone = null; }
    else {
      const data = String(r.data || '');
      if (!/^data:audio\/[a-z0-9.+-]+;base64,/i.test(data)) return send(res, 400, { error: 'Нужен музыкальный файл' });
      if (data.length > 4200000) return send(res, 400, { error: 'Рингтон больше 3 МБ — возьмите короче' });
      user.ringtone = { data, name: String(r.name || 'Свой рингтон').replace(/\.[a-z0-9]+$/i, '').slice(0, 50) };
    }
  }
  if (body.msgSound !== undefined) {
    const m = body.msgSound;
    if (!m) { user.msgSound = null; }
    else {
      const data = String(m.data || '');
      if (!/^data:audio\/[a-z0-9.+-]+;base64,/i.test(data)) return send(res, 400, { error: 'Нужен музыкальный файл' });
      if (data.length > 700000) return send(res, 400, { error: 'Звук сообщения больше 500 КБ — возьмите короче' });
      user.msgSound = { data, name: String(m.name || 'Свой звук').replace(/\.[a-z0-9]+$/i, '').slice(0, 50) };
    }
  }
  if (body.music !== undefined) {
    const m = body.music;
    if (!m) { user.music = null; }
    else {
      const data = String(m.data || '');
      if (!/^data:audio\/[a-z0-9.+-]+;base64,/i.test(data)) return send(res, 400, { error: 'Нужен музыкальный файл' });
      if (data.length > MUSIC_MB * 1.37 * 1024 * 1024) return send(res, 400, { error: 'Трек больше ' + MUSIC_MB + ' МБ' });
      user.music = { data, name: String(m.name || 'Трек').replace(/\.[a-z0-9]+$/i, '').slice(0, 60), size: Math.round(data.length * 0.75) };
    }
  }
  if (body.coverImg !== undefined) {
    const img = String(body.coverImg || '');
    if (!img) { user.coverImg = null; }
    else {
      if (!isPremium(user)) return send(res, 403, { error: 'Своя обложка доступна с премиумом' });
      if (!/^data:image\/(jpeg|png|webp);base64,/.test(img) || img.length > 900000) {
        return send(res, 400, { error: 'Картинка не подходит: до 600 КБ' });
      }
      user.coverImg = img;
    }
  }
  if (body.cover !== undefined) {
    const n = Math.max(0, Math.min(11, Number(body.cover) || 0));
    if (n > 5 && !isPremium(user)) return send(res, 403, { error: 'Эта обложка доступна с премиумом' });
    user.cover = n;
    user.coverImg = null;
  }
  if (body.bio !== undefined) user.bio = String(body.bio || '').slice(0, 140);
  if (body.icon !== undefined) {
    if (!isPremium(user)) return send(res, 403, { error: 'Смена иконки доступна с премиумом' });
    user.icon = String(body.icon || '').slice(0, 20);
  }
  save();
  send(res, 200, { state: fullState(user) });
});

/* Сам звук отдаём отдельно: в общем состоянии он был бы слишком тяжёлым */
route('POST', '/api/profile/sound', async (req, res, body, user) => {
  const kind = body.kind === 'msg' ? 'msgSound' : 'ringtone';
  const snd = user[kind];
  send(res, 200, { data: snd ? snd.data : '', name: snd ? snd.name : '' });
});


/* ---------- Восстановленные маршруты ---------- */

route('POST', '/api/chats/ttl', async (req, res, body, user) => {
  const chat = db.chats[String(body.chatId || '')];
  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });
  const ttl = Number(body.ttl) || 0;
  chat.ttl = [0, 3600e3, 86400e3].includes(ttl) ? ttl : 0;
  save();
  const pid = chat.members.find(m => m !== user.id);
  if (pid) {
    serviceMessage(pid, chat.ttl
      ? 'Собеседник включил автоудаление: сообщения будут исчезать через ' + (chat.ttl === 3600e3 ? 'час' : '24 часа') + '.'
      : 'Автоудаление выключено.');
    push(pid, { type: 'state' });
  }
  send(res, 200, { ttl: chat.ttl, chats: userChats(user.id) });
});

route('POST', '/api/chats/clear', async (req, res, body, user) => {
  /* Чистим историю только у себя: у собеседника переписка остаётся */
  const chat = db.chats[String(body.chatId || '')];
  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });
  chat.clearedFor = chat.clearedFor || {};
  chat.clearedFor[user.id] = now();
  save();
  send(res, 200, { chats: userChats(user.id) });
});

route('POST', '/api/chats/delete', async (req, res, body, user) => {
  const chat = db.chats[String(body.chatId || '')];
  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });
  chat.hiddenFor = chat.hiddenFor || {};
  chat.hiddenFor[user.id] = now();
  chat.clearedFor = chat.clearedFor || {};
  chat.clearedFor[user.id] = now();
  save();
  send(res, 200, { chats: userChats(user.id) });
});

route('POST', '/api/channels/delete', async (req, res, body, user) => {
  const chat = db.chats[String(body.chatId || '')];
  if (!chat || chat.type !== 'channel') return send(res, 404, { error: 'Канал не найден' });
  if (chat.owner !== user.id) return send(res, 403, { error: 'Удалить может только владелец' });
  const uname = chat.username;
  delete db.chats[chat.id];
  if (uname && db.usernames[uname] && db.usernames[uname].channel) delete db.usernames[uname];
  save();
  send(res, 200, { chats: userChats(user.id), state: fullState(user) });
});

route('POST', '/api/users/block', async (req, res, body, user) => {
  const uname = normUsername(body.username);
  const rec = db.usernames[uname];
  const target = rec && db.users[rec.owner];
  if (!target) return send(res, 404, { error: 'Пользователь не найден' });
  user.blocked = user.blocked || [];
  const on = !user.blocked.includes(target.id);
  user.blocked = on ? user.blocked.concat(target.id) : user.blocked.filter(x => x !== target.id);
  save();
  send(res, 200, { blocked: on, state: fullState(user) });
});

route('POST', '/api/profile/settings', async (req, res, body, user) => {
  if (typeof body.anon === 'boolean') user.anon = body.anon;
  if (typeof body.readReceipts === 'boolean') user.readReceipts = body.readReceipts;
  save();
  send(res, 200, { state: fullState(user) });
});

/* ===== ПОМОЩНИК ЧАТТИ ===== */
const HELP_TOPICS = [
  { k: ['продать', 'выставить', 'продажа', 'цена', 'лот'],
    a: 'Продать юзернейм: «Биржа» → «Мои» → «Юзернеймы» → «Продать», укажите цену. Карточку так же, в подвкладке «Карточки». Нужен привязанный телефон и реквизиты для получения денег.' },
  { k: ['купить', 'покупка', 'каталог', 'приобрести'],
    a: 'Купить: «Биржа» → «Каталог». Сверху фильтры и сортировка. Нажимаете лот → открывается сделка → переводите деньги по реквизитам продавца → жмёте «Оплатил» → продавец подтверждает, и покупка ваша.' },
  { k: ['сделк', 'перевод', 'реквизит', 'деньги', 'спор'],
    a: 'Сделки — во вкладке «Сделки». Покупатель платит и жмёт «Оплатил», продавец подтверждает получение. Если продавец пропал, через 7 дней передача произойдёт сама. Если обманули — откройте спор или подайте жалобу «Скам».' },
  { k: ['скам', 'докс', 'жалоб', 'обман', 'мошенн', 'полиц', 'кинул', 'развел', 'развёл', 'угроз'],
    a: 'Жалоба: чат → меню «⋮» → «Пожаловаться» → «Скам» или «Докс». Соберём протокол переписки со всеми удалёнными сообщениями, он откроется ссылкой прямо в приложении — сохраняете в PDF и подаёте заявление. Телефон нарушителя скрыт: полные данные отдаём только по запросу полиции.' },
  { k: ['секрет', 'шифр', 'приватн', 'ключ'],
    a: 'Секретный чат: меню чата → «Секретный чат». Нужно согласие собеседника. Сообщения шифруются на телефонах, но жалобы в таком чате недоступны. Ключ хранится в памяти телефона — очистите данные, и старые сообщения не восстановить.' },
  { k: ['звон', 'позвон', 'дозвон', 'трубк', 'вызов', 'громк'],
    a: 'Звонок — кнопка трубки в шапке чата. Есть видео и громкая связь. Если бесконечное «Соединение» — выключите VPN или перейдите на Wi-Fi. Рингтон меняется в «Настройки» → «Внешний вид».' },
  { k: ['голосов', 'микрофон', 'кружок', 'кружк', 'записать'],
    a: 'Голосовое: зажмите микрофон, отпустите — уйдёт. Свайп вверх закрепляет запись. Кружок: короткий тап по микрофону переключает режим, дальше так же зажимаете.' },
  { k: ['фото', 'видео', 'файл', 'скрепк', 'картинк', 'пропал', 'хранят'],
    a: 'Фото и видео — кнопка галереи, файлы — скрепка, до 10 МБ. Видео, кружки и файлы хранятся 7 дней, потом удаляются. Фото, текст и голосовые остаются навсегда.' },
  { k: ['обои', 'фон', 'тема', 'тёмн', 'темн', 'шрифт', 'внешн', 'оформлен', 'обложк', 'аватар'],
    a: 'Обои чата: меню чата → «Обои чата», себе или обоим с согласия. Шрифт, тёмная тема, обложка, эффекты и звуки — «Настройки» → «Внешний вид».' },
  { k: ['музык', 'трек', 'песн', 'плеер', 'mp3'],
    a: 'Музыка в профиле: «Настройки» → «Внешний вид» → «Музыка в профиле», до 12 МБ. Трек виден всем: можно послушать, замедлить, поставить на повтор и скачать.' },
  { k: ['премиум', 'подписк'],
    a: 'Премиум даёт сторис, больше слотов для юзернеймов и особые обложки. Выдаётся вручную командой — напишите разработчику через плашку DEV.' },
  { k: ['карточк', 'nft', 'коллекц', 'devcoin', 'aegis', 'редкость', 'задани', 'квест'],
    a: 'Коллекционные карточки нельзя купить — их выдают за задания и события. Зелёная плашка над чатами открывает задания: нажимаете «Начать», и есть час на три простых шага. Карточки лежат в профиле, перепродать можно на бирже.' },
  { k: ['юзернейм', 'ник', 'занять', 'слот'],
    a: 'Юзернеймы — «Биржа» → «Мои». Свободный занимается кнопкой «Занять». Основной помечен «осн.», его можно продать только при наличии запасного. Слотов 3, с премиумом больше.' },
  { k: ['канал', 'бот', 'токен'],
    a: 'Канал и бота создаёте кнопкой «плюс» на вкладке «Чаты». В канале пишет только владелец. Боту выдаётся токен для подключения вашей программы.' },
  { k: ['выйти', 'выход', 'удалить аккаунт', 'привязать', 'вход', 'войти'],
    a: 'Вход по Telegram или номеру телефона. Выйти: «Настройки» → «Выйти». Удаление аккаунта — через обращение к разработчику.' },
  { k: ['уведомлен', 'звук', 'рингтон', 'не приходят', 'тишин'],
    a: 'Звуки — «Настройки» → «Внешний вид»: рингтон звонка, звук сообщений, можно загрузить свои mp3. Уведомления при закрытом приложении работают только если открыть сайт в Chrome и добавить на главный экран.' },
  { k: ['анонимн', 'скрыть', 'приватност', 'заблокир', 'блок'],
    a: 'Суперанонимность включается в настройках: вас не найдут поиском. Телефон другим не показывается. Заблокировать собеседника можно в меню чата.' },
  { k: ['автоудален', 'исчеза', 'таймер', 'очистить истор'],
    a: 'Автоудаление: меню чата → «Автоудаление», по кругу выкл → 1 час → 24 часа. «Очистить историю» убирает переписку только у вас.' },
  { k: ['не работает', 'виснет', 'ошибк', 'баг', 'тормоз', 'зависа', 'глюч', 'медленн'],
    a: 'Если приложение подтормаживает при первом входе — сервер просыпается, это до минуты. Обновление: «Настройки» → «Проверить обновления». Не помогло — напишите разработчику через плашку DEV.' },
  { k: ['разработчик', 'связаться', 'поддержк', 'команд', 'админ', 'помощь'],
    a: 'Написать команде: тапните плашку DEV или CO-DEV рядом с именем разработчика — откроется меню с темами: скам, докс, угрозы, ошибка в приложении.' }
];

function findTopic(q) {
  const words = q.split(/\s+/).filter(Boolean);
  let best = null, bestScore = 0;
  for (const t of HELP_TOPICS) {
    let score = 0;
    for (const key of t.k) {
      if (key.includes(' ')) { if (q.includes(key)) score += 4; continue; }
      for (const w of words) {
        if (w.startsWith(key) || (key.startsWith(w) && w.length >= 4)) { score += key.length > 5 ? 3 : 2; break; }
      }
    }
    if (score > bestScore) { bestScore = score; best = t; }
  }
  return { best, score: bestScore };
}

route('POST', '/api/help/ask', async (req, res, body, user) => {
  const raw = String(body.q || '').toLowerCase().slice(0, 300);
  if (!raw) return send(res, 400, { error: 'Пустой вопрос' });
  const q = raw.replace(/[^а-яёa-z0-9 ]/gi, ' ');
  const { best, score } = findTopic(q);
  if (best && score >= 2) return send(res, 200, { answer: best.a });
  send(res, 200, {
    answer: 'Не нашёл точного ответа, но вот что я умею объяснить:\n\n' +
      '• Биржа: продажа и покупка юзернеймов и карточек\n' +
      '• Сделки: перевод, реквизиты, споры\n' +
      '• Жалобы: скам, докс, протокол для полиции\n' +
      '• Секретные чаты и шифрование\n' +
      '• Звонки, голосовые, кружки\n' +
      '• Фото, видео, файлы и сроки хранения\n' +
      '• Обои, шрифты, тёмная тема, музыка\n' +
      '• Премиум, каналы, боты, задания и карточки\n\n' +
      'Спросите своими словами. Если вопрос не про приложение — напишите разработчику через плашку DEV.'
  });
});

const server = http.createServer(async (req, res) => {
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
    });
    return res.end();
  }

  const url = req.url.split('?')[0];

  /* API ботов: авторизация токеном в адресе, обычный вход не нужен */
  if (url.startsWith('/api/bot/')) {
    try {
      const body = req.method === 'POST' ? await readBody(req) : {};
      return await handleBotApi(req, res, url, body);
    } catch (e) {
      console.error(e);
      return send(res, 500, { error: 'Ошибка сервера' });
    }
  }

  /* Протокол переписки: открывается по личной ссылке из письма */
  if (url.startsWith('/report/') && req.method === 'GET') {
    const num = Number(url.split('/')[2]);
    const t = (req.url.split('?')[1] || '').replace(/^t=/, '');
    const r = db.reports.find(x => x.num === num);
    if (!r || !r.token || r.token !== t) {
      res.writeHead(404, { 'content-type': 'text/html; charset=utf-8' });
      return res.end('<meta charset="utf-8"><h2 style="font-family:Arial">Протокол не найден</h2>' +
        '<p style="font-family:Arial">Проверьте ссылку целиком, вместе с кодом после знака вопроса.</p>');
    }
    if (now() - r.time > 30 * 86400e3) {
      res.writeHead(410, { 'content-type': 'text/html; charset=utf-8' });
      return res.end('<meta charset="utf-8"><h2 style="font-family:Arial">Срок хранения истёк</h2>' +
        '<p style="font-family:Arial">Протокол доступен 30 дней с момента подачи жалобы.</p>');
    }
    r.opened = (r.opened || 0) + 1;
    r.lastOpen = now();
    save();
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    return res.end(reportHtml(r));
  }

  const key = req.method + ' ' + url;
  const handler = routes[key];

  if (!handler) return send(res, 404, { error: 'Не найдено' });

  let user = null;
  if (!OPEN_ROUTES.includes(key)) {
    const token = (req.headers.authorization || '').replace('Bearer ', '');
    user = userByToken(token);
    if (!user) return send(res, 401, { error: 'Требуется вход' });
    if (user.banned) return send(res, 403, { error: 'Аккаунт заблокирован модерацией Newchat' });
    user.lastSeen = now(); /* «был в сети» обновляется любым запросом */
  }

  try {
    const body = req.method === 'POST' ? await readBody(req) : {};
    await handler(req, res, body, user);
  } catch (e) {
    console.error(e);
    send(res, 500, { error: 'Ошибка сервера' });
  }
});

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  const token = new URL(req.url, 'http://x').searchParams.get('token');
  const user = userByToken(token);
  if (!user) return ws.close();

  if (!sockets.has(user.id)) sockets.set(user.id, new Set());
  sockets.get(user.id).add(ws);

  /* Сигналинг звонков: клиенты обмениваются WebRTC-пакетами через нас.
     Сами звонки идут напрямую между телефонами, сервер видит только «конверты». */
  ws.on('message', raw => {
    let d;
    try { d = JSON.parse(String(raw).slice(0, 200000)); } catch (e) { return; }

    if (d.type === 'typing') {
      const chat = db.chats[String(d.chatId || '')];
      if (!chat || chat.service || chat.type === 'channel' || !chat.members.includes(user.id)) return;
      const peerId = chat.members.find(m => m !== user.id);
      const peer = peerId && db.users[peerId];
      if (!peer || peer.isBot) return;
      if ((peer.blocked || {})[user.id] || (user.blocked || {})[peerId]) return;
      push(peerId, { type: 'typing', chatId: chat.id });
      return;
    }

    if (d.type !== 'rtc') return;

    const chat = db.chats[String(d.chatId || '')];
    if (!chat || chat.service || chat.type === 'channel' || !chat.members.includes(user.id)) return;

    const peerId = chat.members.find(m => m !== user.id);
    const peer = peerId && db.users[peerId];
    if (!peer || peer.isBot) return;
    if ((peer.blocked || {})[user.id] || (user.blocked || {})[peerId]) return;

    push(peerId, {
      type: 'rtc',
      chatId: chat.id,
      from: publicUser(user),
      payload: d.payload || {}
    });
  });

  ws.on('close', () => {
    const set = sockets.get(user.id);
    if (set) {
      set.delete(ws);
      if (!set.size) sockets.delete(user.id);
    }
  });
  ws.on('error', () => {});
});

/* Пинг, чтобы соединения не рвались на бесплатных хостингах */
setInterval(() => {
  wss.clients.forEach(ws => { if (ws.readyState === 1) ws.ping(); });
}, 30000);

load().then(() => {
  server.listen(PORT, () => {
    console.log('Newchat-сервер запущен на порту ' + PORT);
  const ways = [];
  if (MP_KEY) ways.push('Mailopost');
  if (RS_KEY) ways.push('Rusender');
  if (SP_ID && SP_SECRET) ways.push('SendPulse');
  if (MAIL_HOOK_URL) ways.push('мостик Google');
  if (BREVO_KEY) ways.push('Brevo');
  if (MAIL_USER && MAIL_PASS) ways.push('SMTP ' + MAIL_HOST);
  console.log('Почта: ' + (ways.length ? 'пути отправки — ' + ways.join(', ') : 'НЕ НАСТРОЕНА, коды идут на экран'));
  console.log('Почта: отправитель — ' + (MAIL_USER || 'не задан (MAIL_USER)'));
    startTelegram();
  });
});
