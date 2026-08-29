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
  .split(',').map(s => s.trim().toLowerCase()).filter(Boolean);

/* Ключ от SMS.ru. Если не задан — код показывается на экране (режим разработки). */
const SMSRU_API_ID = process.env.SMSRU_API_ID || '';
const SMS_ENABLED = !!SMSRU_API_ID;

/* Почта: обычный Gmail-ящик с «паролем приложения». Бесплатно, до 500 писем в сутки. */
const MAIL_USER = process.env.MAIL_USER || '';
const MAIL_PASS = process.env.MAIL_PASS || '';
const MAIL_HOST = process.env.MAIL_HOST || 'smtp.gmail.com';
const MAIL_PORT = Number(process.env.MAIL_PORT || 465);
const MAIL_ENABLED = !!(MAIL_USER && MAIL_PASS);

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

  console.warn('ВНИМАНИЕ: база хранится в файле. На бесплатном Render аккаунты пропадут при перезапуске. Задайте DATABASE_URL.');
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
    trust: u.isBot ? undefined : (u.trust || 0),
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
function isDev(user) {
  return !!(user && (user.dev || DEV_USERNAMES.includes(user.username || '')));
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
    blocked: type === 'dm' && peer && peer.id ? !!(me.blocked || {})[peer.id] : false,
    service: !!c.service,
    owner: c.owner || null,
    mine: c.owner === userId,
    subs: c.type === 'channel' ? c.members.length : undefined,
    canWrite: type === 'dm' || (type === 'channel' && c.owner === userId),
    peer,
    msgs: c.msgs.filter(m => m.time > ((c.clearedAt || {})[userId] || 0)).slice(-200).map(m => ({
      id: m.id, text: m.text, time: m.time,
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
  return {
    user: Object.assign(publicUser(user), {
      phone: user.phone, trust: user.trust,
      premium: isPremium(user),
      premiumUntil: user.premiumUntil || 0,
      slots: slotLimit(user),
      premiumDays: PREMIUM.days,
      anonMode: !!user.anon,
      email: user.email || '',
      phoneOk: !!user.phone,
      invites: user.inviteCount || 0,
      invitesNeeded: PREMIUM.invites,
      requisites: user.requisites || null
    }),
    chats: userChats(user.id),
    usernames: myUsernames(user.id),
    market: marketList(user.id),
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
      if (m.media && (m.media.kind === 'video' || m.media.kind === 'circle') && now() - m.time > 7 * 86400e3) {
        m.media = null;
        m.text = m.text || 'Видео удалено (хранится 7 дней)';
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
      if (data.length > 6e6) req.destroy();
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
    const sock = tls.connect({ host: opts.host, port: opts.port, servername: opts.host });
    sock.setEncoding('utf8');
    sock.setTimeout(20000);

    let buf = '';
    const queue = [];        /* ожидания ответов по очереди */
    let done = false;

    function fail(e) {
      if (done) return;
      done = true;
      try { sock.destroy(); } catch (x) {}
      reject(e instanceof Error ? e : new Error(String(e)));
    }

    /* Разбираем поток на законченные ответы SMTP (учитывая многострочные) */
    sock.on('data', chunk => {
      buf += chunk;
      let nl;
      while ((nl = buf.indexOf('\r\n')) !== -1) {
        const line = buf.slice(0, nl);
        buf = buf.slice(nl + 2);
        if (/^\d{3}-/.test(line)) continue;      /* продолжение — ждём финальную строку */
        const code = line.slice(0, 3);
        const waiter = queue.shift();
        if (waiter) waiter(code, line);
      }
    });

    function reply() {
      return new Promise(res => queue.push((code, line) => res({ code, line })));
    }
    async function expect(codes, what) {
      const r = await reply();
      if (!codes.includes(r.code)) throw new Error('SMTP ' + what + ' → ' + r.line);
      return r;
    }
    function say(text) { sock.write(text + '\r\n'); }

    sock.on('timeout', () => fail(new Error('SMTP: таймаут')));
    sock.on('error', fail);

    sock.on('secureConnect', async () => {
      try {
        await expect(['220'], 'приветствие');
        say('EHLO newchat');
        await expect(['250'], 'EHLO');
        say('AUTH LOGIN');
        await expect(['334'], 'AUTH');
        say(Buffer.from(opts.user).toString('base64'));
        await expect(['334'], 'логин');
        say(Buffer.from(opts.pass).toString('base64'));
        await expect(['235'], 'пароль (проверьте пароль приложения)');
        say('MAIL FROM:<' + opts.user + '>');
        await expect(['250'], 'MAIL FROM');
        say('RCPT TO:<' + opts.to + '>');
        await expect(['250', '251'], 'RCPT TO');
        say('DATA');
        await expect(['354'], 'DATA');
        sock.write(opts.message + '\r\n.\r\n');
        await expect(['250'], 'отправка письма');
        say('QUIT');
        done = true;
        sock.end();
        resolve(true);
      } catch (e) {
        fail(e);
      }
    });
  });
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

  try {
    await smtpSend({ host: MAIL_HOST, port: MAIL_PORT, user: MAIL_USER, pass: MAIL_PASS, to, message });
    return { sent: true };
  } catch (e) {
    console.error('Почта:', e.message);
    return { sent: false, reason: 'Не удалось отправить письмо: ' + e.message };
  }
}

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

  if (MAIL_ENABLED) {
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
  if (user.dev) user.verified = true;

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
    /* Видео тяжёлое: держим короткое и лёгкое, иначе бесплатная база кончится за неделю */
    if (!/^data:video\/(webm|mp4|quicktime)(;codecs=[a-z0-9,.\s]+)?;base64,/.test(data)) return null;
    if (data.length > 3600000) return null;
    return {
      kind: media.kind, data,
      dur: Math.min(60, Math.max(1, Math.round(Number(media.dur) || 1)))
    };
  }
  return null;
}

route('POST', '/api/messages/send', async (req, res, body, user) => {
  const chat = db.chats[body.chatId];
  const text = String(body.text || '').trim().slice(0, 4000);
  const media = validMedia(body.media);

  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });
  if (!text && !media) return send(res, 400, { error: 'Пустое сообщение' });
  if (body.media && !media) return send(res, 400, { error: 'Файл не подходит: фото до 500 КБ, голос до 5 минут, видео до 60 секунд и 2,5 МБ' });
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
    : media.kind === 'circle' ? '⭕ Видеосообщение' : '🎬 Видео';
  const msg = { id: uid(), from: user.id, text: outText, time: now(), deleted: false };
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
      if (m !== user.id) push(m, { type: 'message', chatId: chat.id, message: { id: msg.id, text: msg.text, media: msg.media || null, reply: msg.reply || null, fwd: msg.fwd || null, time: msg.time, out: false } });
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
      push(peerId, { type: 'message', chatId: chat.id, message: { id: msg.id, text: msg.text, media: msg.media || null, reply: msg.reply || null, fwd: msg.fwd || null, time: msg.time, out: false } });
    }
  }
  send(res, 200, { message: { id: msg.id, text: msg.text, media: msg.media || null, reply: msg.reply || null, fwd: msg.fwd || null, reactions: null, time: msg.time, out: true } });
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

route('POST', '/api/reports/create', async (req, res, body, user) => {
  const chat = db.chats[body.chatId];
  const kind = body.kind === 'докс' ? 'докс' : 'скам';
  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });
  if (chat.type === 'channel' || chat.service) return send(res, 400, { error: 'Жаловаться можно на личную переписку' });

  const peerId = chat.members.find(m => m !== user.id);

  /* Антинакрутка: считаем жалобы этого пользователя за последний час */
  const hourAgo = now() - 3600e3;
  const recent = db.reports.filter(r => r.from === user.id && r.time > hourAgo);
  if (recent.length >= 5) {
    user.trust = Math.max(0, user.trust - 10);
    save();
    return send(res, 429, { error: 'Слишком много жалоб за час. Доверие снижено.' });
  }

  const num = 4000 + db.reports.length + 1;
  db.reports.push({
    num, from: user.id, against: peerId, chatId: chat.id,
    kind, time: now(),
    /* Полный слепок переписки, включая удалённые сообщения */
    snapshot: chat.msgs.map(m => ({ from: m.from, text: m.text, time: m.time, deleted: !!m.deleted }))
  });
  save();

  serviceMessage(user.id, `Жалоба №${num} (${kind}) принята и передана в полицию. Статус можно отслеживать здесь.`);
  send(res, 200, { num });
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

  if (isDev(target) && (body.ban || body.trust !== undefined)) {
    return send(res, 400, { error: 'Разработчика забанить или понизить нельзя' });
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
  if (!/^data:image\/(jpeg|png|webp);base64,/.test(photo) || photo.length > 700000) {
    return send(res, 400, { error: 'Фото не подходит' });
  }
  const mine = db.stories.filter(st => st.user === user.id && now() - st.time < 86400e3);
  if (mine.length >= 10) return send(res, 400, { error: 'Не больше 10 историй в сутки' });

  db.stories.push({ id: uid(), user: user.id, photo, text: String(body.text || '').slice(0, 100), time: now() });
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
    byUser[st.user].items.push({ id: st.id, photo: st.photo, text: st.text, time: st.time });
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

route('POST', '/api/profile/settings', async (req, res, body, user) => {
  if (typeof body.anon === 'boolean') user.anon = body.anon;
  save();
  send(res, 200, { anon: !!user.anon });
});

route('POST', '/api/users/block', async (req, res, body, user) => {
  const target = db.users[String(body.userId || '')];
  if (!target || target.id === user.id) return send(res, 404, { error: 'Пользователь не найден' });
  user.blocked = user.blocked || {};
  if (body.on) user.blocked[target.id] = true;
  else delete user.blocked[target.id];
  save();
  send(res, 200, { blocked: !!user.blocked[target.id], chats: userChats(user.id) });
});

route('POST', '/api/chats/delete', async (req, res, body, user) => {
  const chat = db.chats[String(body.chatId || '')];
  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });
  if (chat.service) return send(res, 400, { error: 'Служебный чат удалить нельзя' });
  if (chat.type === 'channel') return send(res, 400, { error: 'Канал удаляется в его меню' });
  /* Скрываем у себя; копия остаётся на сервере — жалобы «Докс» и «Скам» работают как раньше */
  chat.hiddenFor = chat.hiddenFor || [];
  if (!chat.hiddenFor.includes(user.id)) chat.hiddenFor.push(user.id);
  chat.clearedAt = chat.clearedAt || {};
  chat.clearedAt[user.id] = now(); /* при возврате чат будет чистым */
  save();
  send(res, 200, { chats: userChats(user.id) });
});

route('POST', '/api/chats/clear', async (req, res, body, user) => {
  const chat = db.chats[String(body.chatId || '')];
  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });
  chat.clearedAt = chat.clearedAt || {};
  chat.clearedAt[user.id] = now();
  save();
  send(res, 200, { chats: userChats(user.id) });
});

route('POST', '/api/channels/delete', async (req, res, body, user) => {
  const chat = db.chats[String(body.chatId || '')];
  if (!chat || chat.type !== 'channel') return send(res, 404, { error: 'Канал не найден' });
  if (chat.owner !== user.id) return send(res, 403, { error: 'Удалить канал может только владелец' });

  const members = chat.members.slice();
  if (chat.uname && db.usernames[chat.uname] && db.usernames[chat.uname].channel === chat.id) {
    delete db.usernames[chat.uname]; /* юзернейм канала освобождается */
  }
  delete db.chats[chat.id];
  save();
  for (const m of members) {
    if (m !== user.id) {
      serviceMessage(m, `Канал «${chat.title}» удалён владельцем.`);
      push(m, { type: 'state' });
    }
  }
  send(res, 200, { chats: userChats(user.id) });
});

route('POST', '/api/chats/ttl', async (req, res, body, user) => {
  const chat = db.chats[String(body.chatId || '')];
  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });
  if (chat.service || chat.type === 'channel') return send(res, 400, { error: 'Только для личных чатов' });
  const hours = [0, 1, 24].includes(Number(body.hours)) ? Number(body.hours) : 0;
  chat.ttl = hours ? hours * 3600e3 : 0;
  save();
  const label = hours === 0 ? 'выключено' : (hours === 1 ? '1 час' : '24 часа');
  const pid = chat.members.find(m => m !== user.id);
  if (pid) {
    serviceMessage(pid, `Собеседник изменил автоудаление сообщений в вашем чате: ${label}.`);
    push(pid, { type: 'state' });
  }
  send(res, 200, { ttl: chat.ttl, chats: userChats(user.id) });
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
  if (peerId) push(peerId, { type: 'message', chatId: chat.id, message: { id: msg.id, text, time: msg.time, out: false } });
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
  if ((user.trust || 0) < SELL_MIN_TRUST) return send(res, 400, { error: 'Продавать можно с доверием от ' + SELL_MIN_TRUST + '%' });
  if (!user.requisites) return send(res, 400, { error: 'Сначала укажите реквизиты в «Сделках» — их увидит покупатель' });

  rec.forSale = true;
  rec.price = price;
  save();
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
    videoDays: 7,
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
  'POST /api/auth/email/request',
  'POST /api/auth/email/verify',
  'POST /api/auth/telegram/start',
  'POST /api/auth/telegram/check',
  'POST /telegram/webhook',
  'POST /api/admin/deals',
  'GET /api/config',
  'GET /api/health'
];

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
    startTelegram();
  });
});
