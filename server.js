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

/* Telegram-бот для подтверждения номера. Бесплатно и без лимитов. */
const TG_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TG_BOT_NAME = (process.env.TELEGRAM_BOT_USERNAME || '').replace('@', '');
const TG_ENABLED = !!(TG_TOKEN && TG_BOT_NAME);
const TG_SECRET = crypto.randomBytes(16).toString('hex');
const PUBLIC_URL = (process.env.RENDER_EXTERNAL_URL || process.env.PUBLIC_URL || '').replace(/\/$/, '');

/* ================= ПЛАТЕЖИ ================= */
/* Ключи ЮKassa. Без них кошелёк работает в тестовом режиме без реальных денег. */
const YK_SHOP_ID = process.env.YOOKASSA_SHOP_ID || '';
const YK_SECRET = process.env.YOOKASSA_SECRET_KEY || '';
const YK_ENABLED = !!(YK_SHOP_ID && YK_SECRET);

/* Выплаты включаются отдельно и только после договора с провайдером.
   Пока выключено — заявки копятся и ждут ручного подтверждения. */
const PAYOUTS_AUTO = process.env.PAYOUTS_AUTO === 'yes';

/* Пароль для страницы модерации выплат */
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';

/* Комиссия площадки с продажи юзернейма, в процентах */
const FEE_PERCENT = Number(process.env.FEE_PERCENT || 5);

/* Премиум: цена и что даёт */
const PREMIUM = {
  price: Number(process.env.PREMIUM_PRICE || 500),
  days: Number(process.env.PREMIUM_DAYS || 30),
  slots: 5,
  freeSlots: 3
};

/* Правила вывода. Меняются переменными окружения, если понадобится. */
const RULES = {
  holdDays: Number(process.env.HOLD_DAYS || 7),          // сколько дней деньги «остывают» после пополнения
  earnedHoldDays: Number(process.env.EARNED_HOLD_DAYS || 14), // выручка с продаж ждёт дольше
  minWithdraw: Number(process.env.MIN_WITHDRAW || 500),
  maxPerDay: Number(process.env.MAX_PER_DAY || 50000),
  maxPerRequest: Number(process.env.MAX_PER_REQUEST || 30000),
  minTrust: Number(process.env.MIN_TRUST || 70),         // с низким доверием вывод закрыт
  manualAbove: Number(process.env.MANUAL_ABOVE || 5000)  // выше этой суммы — только ручное подтверждение
};

/* ================= ХРАНИЛИЩЕ ================= */

let db = {
  users: {},      // userId -> { id, phone, name, username, cover, ava, status, verified, dev, balance, trust, createdAt }
  usernames: {},  // username -> { owner, main, forSale, price }
  chats: {},      // chatId -> { id, members:[a,b], service, msgs:[] }
  history: {},    // userId -> [операции кошелька]
  reports: [],    // жалобы
  verifyRequests: [],
  tokens: {},     // token -> userId
  codes: {},      // phone -> { code, expires }
  tgSessions: {}, // session -> { created, chatId, status, token, needsSetup }
  payments: {},   // paymentId -> платёж от провайдера
  deposits: {},   // userId -> [пополнения с данными карты]
  payouts: [],    // заявки на вывод
  cards: {}       // userId -> [карты, подтверждённые пополнением]
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
      console.log('База загружена из файла');
    }
  } catch (e) {
    console.error('Не удалось прочитать базу:', e.message);
  }
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
function publicUser(u) {
  if (!u) return null;
  return {
    id: u.id, name: u.name, username: u.username,
    cover: u.cover, ava: u.ava, status: u.status,
    verified: !!u.verified, dev: !!u.dev
  };
}
function userByToken(token) {
  const id = db.tokens[token];
  return id ? db.users[id] : null;
}
function isPremium(user) {
  return !!(user.premiumUntil && user.premiumUntil > now());
}
function slotLimit(user) {
  return isPremium(user) ? PREMIUM.slots : PREMIUM.freeSlots;
}
function chatIdFor(a, b) {
  return [a, b].sort().join(':');
}
function userChats(userId) {
  return Object.values(db.chats)
    .filter(c => c.members.includes(userId))
    .map(c => {
      const otherId = c.members.find(m => m !== userId) || c.members[0];
      const other = db.users[otherId];
      return {
        id: c.id,
        service: !!c.service,
        peer: c.service
          ? { id: 'service', name: 'Newchat', username: 'newchat', verified: true }
          : publicUser(other),
        msgs: c.msgs.slice(-200).map(m => ({
          id: m.id, text: m.text, time: m.time,
          out: m.from === userId, deleted: !!m.deleted
        }))
      };
    })
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
function marketList(exceptUser) {
  return Object.entries(db.usernames)
    .filter(([, v]) => v.forSale && v.owner !== exceptUser)
    .map(([u, v]) => ({
      u, price: v.price || 0,
      seller: publicUser(db.users[v.owner])
    }));
}
function fullState(user) {
  return {
    user: Object.assign(publicUser(user), {
      phone: user.phone, balance: user.balance, trust: user.trust,
      frozen: !!user.frozen,
      premium: isPremium(user),
      premiumUntil: user.premiumUntil || 0,
      slots: slotLimit(user),
      premiumPrice: PREMIUM.price,
      premiumDays: PREMIUM.days
    }),
    chats: userChats(user.id),
    usernames: myUsernames(user.id),
    market: marketList(user.id),
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
      balance: 0, trust: 100, createdAt: now(),
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

/* ================= ПЛАТЁЖНАЯ СИСТЕМА ================= */

async function yk(method, path, body, idempotenceKey) {
  const headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Basic ' + Buffer.from(YK_SHOP_ID + ':' + YK_SECRET).toString('base64')
  };
  if (idempotenceKey) headers['Idempotence-Key'] = idempotenceKey;

  try {
    const res = await fetch('https://api.yookassa.ru/v3' + path, {
      method,
      headers,
      body: body ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(20000)
    });
    return await res.json();
  } catch (e) {
    console.error('ЮKassa ' + path + ':', e.message);
    return null;
  }
}

/* Отпечаток карты: по нему понимаем, что это та же самая карта */
function cardKey(details) {
  if (!details) return null;
  if (details.type === 'sbp') {
    /* СБП — платёж по номеру телефона через банк */
    return 'sbp:' + (details.payer_bank_details && details.payer_bank_details.bank_id || 'unknown');
  }
  if (details.card) {
    const c = details.card;
    return 'card:' + (c.first6 || '') + ':' + (c.last4 || '');
  }
  return null;
}

function cardTitle(details) {
  if (!details) return 'Неизвестно';
  if (details.type === 'sbp') {
    const b = details.payer_bank_details;
    return 'СБП · ' + ((b && b.bank_name) || 'банк');
  }
  if (details.card) return 'Карта •• ' + (details.card.last4 || '????');
  return 'Платёж';
}

/* Зачисление денег — только по подтверждению от провайдера, никогда по слову клиента */
function creditPayment(payment) {
  if (db.payments[payment.id] && db.payments[payment.id].credited) return false;

  const meta = payment.metadata || {};
  const user = db.users[meta.userId];
  if (!user) return false;

  const amount = Math.round(parseFloat(payment.amount.value));
  const details = payment.payment_method || {};
  const key = cardKey(details);
  const title = cardTitle(details);

  user.balance += amount;

  db.deposits[user.id] = db.deposits[user.id] || [];
  db.deposits[user.id].push({
    id: payment.id,
    amount,
    left: amount,          // сколько из этого пополнения ещё можно вывести
    cardKey: key,
    cardTitle: title,
    time: now(),
    refunded: 0
  });

  /* Карта попадает в список выплат только потому, что с неё пришли деньги */
  if (key) {
    db.cards[user.id] = db.cards[user.id] || [];
    if (!db.cards[user.id].some(c => c.key === key)) {
      db.cards[user.id].push({ key, title, addedAt: now(), paymentId: payment.id });
    }
  }

  db.payments[payment.id] = { userId: user.id, amount, credited: true, time: now() };
  db.history[user.id] = db.history[user.id] || [];
  db.history[user.id].unshift({
    amt: amount,
    title: 'Пополнение',
    sub: title,
    time: now()
  });
  save();

  serviceMessage(user.id, `Кошелёк пополнен на ${amount.toLocaleString('ru')} ₽ (${title}).`);
  push(user.id, { type: 'state' });
  return true;
}

/* Возврат или спорная операция — забираем деньги обратно */
function handleRefund(payment) {
  const rec = db.payments[payment.id];
  if (!rec) return;
  const user = db.users[rec.userId];
  if (!user) return;

  const refunded = Math.round(parseFloat(payment.refunded_amount ? payment.refunded_amount.value : payment.amount.value));
  const dep = (db.deposits[user.id] || []).find(d => d.id === payment.id);
  if (dep) {
    dep.refunded = refunded;
    dep.left = Math.max(0, dep.amount - refunded);
  }

  user.balance -= refunded;
  /* Ушли в минус — значит вывели то, что вернули по спору. Замораживаем. */
  if (user.balance < 0) {
    user.frozen = true;
    user.trust = Math.max(0, user.trust - 30);
    serviceMessage(user.id, 'По одному из пополнений прошёл возврат. Кошелёк заморожен, свяжитесь с поддержкой.');
  } else {
    serviceMessage(user.id, `Возврат по пополнению: ${refunded.toLocaleString('ru')} ₽ списано с баланса.`);
  }

  db.history[user.id] = db.history[user.id] || [];
  db.history[user.id].unshift({ amt: -refunded, title: 'Возврат пополнения', sub: 'спор по платежу', time: now() });
  save();
  push(user.id, { type: 'state' });
}

/* ================= ЧТО МОЖНО ВЫВЕСТИ ================= */

function withdrawInfo(user) {
  const deposits = db.deposits[user.id] || [];
  const holdMs = RULES.holdDays * 86400e3;

  /* Готовы к выводу только «остывшие» пополнения */
  const perCard = {};
  let ripe = 0, waiting = 0;

  for (const d of deposits) {
    if (d.left <= 0) continue;
    if (now() - d.time >= holdMs) {
      ripe += d.left;
      if (d.cardKey) perCard[d.cardKey] = (perCard[d.cardKey] || 0) + d.left;
    } else {
      waiting += d.left;
    }
  }

  /* Заработанное внутри (продажи) — отдельный счёт с длинной выдержкой */
  const earned = Math.max(0, user.balance - deposits.reduce((s, d) => s + d.left, 0));

  const dayAgo = now() - 86400e3;
  const usedToday = db.payouts
    .filter(p => p.userId === user.id && p.time > dayAgo && p.status !== 'rejected')
    .reduce((s, p) => s + p.amount, 0);

  const cards = (db.cards[user.id] || []).map(c => ({
    key: c.key,
    title: c.title,
    available: perCard[c.key] || 0
  }));

  const blocks = [];
  if (user.frozen) blocks.push('Кошелёк заморожен');
  if ((user.trust || 0) < RULES.minTrust) blocks.push('Низкая батарейка доверия');
  if (!cards.length) blocks.push('Сначала пополните кошелёк — вывод возможен только на свою карту');

  return {
    ripe, waiting, earned, cards,
    usedToday,
    dayLimit: RULES.maxPerDay,
    minWithdraw: RULES.minWithdraw,
    holdDays: RULES.holdDays,
    earnedHoldDays: RULES.earnedHoldDays,
    blocks
  };
}

/* ================= WEBSOCKET ================= */

const sockets = new Map(); // userId -> Set<ws>

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
      if (data.length > 1e6) req.destroy();
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

  if (!user) {
    const id = uid();
    user = {
      id, phone, name: '', username: '',
      cover: 0, ava: 0, status: '',
      verified: false, dev: false,
      balance: 0, trust: 100, createdAt: now()
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
  if (username.length < 3) return send(res, 400, { error: 'Юзернейм — минимум 3 символа' });
  if (db.usernames[username]) return send(res, 400, { error: 'Этот юзернейм уже занят' });

  user.name = name;
  user.username = username;
  user.dev = DEV_USERNAMES.includes(username);
  if (user.dev) user.verified = true;

  db.usernames[username] = { owner: user.id, main: true, forSale: false, price: 0 };
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
  if (typeof body.status === 'string') user.status = body.status.slice(0, 4);
  save();
  send(res, 200, { ok: true });
});

/* ---------- Чаты и сообщения ---------- */

route('POST', '/api/chats/create', async (req, res, body, user) => {
  const username = normUsername(body.username);
  if (username === user.username) return send(res, 400, { error: 'Это ваш собственный юзернейм' });

  const rec = db.usernames[username];
  if (!rec) return send(res, 404, { error: 'Пользователь не найден' });

  const peer = db.users[rec.owner];
  if (!peer) return send(res, 404, { error: 'Пользователь не найден' });

  const id = chatIdFor(user.id, peer.id);
  if (!db.chats[id]) {
    db.chats[id] = { id, members: [user.id, peer.id], service: false, msgs: [] };
    save();
    push(peer.id, { type: 'chats' });
  }
  send(res, 200, { chatId: id, chats: userChats(user.id) });
});

route('POST', '/api/messages/send', async (req, res, body, user) => {
  const chat = db.chats[body.chatId];
  const text = String(body.text || '').trim().slice(0, 4000);

  if (!chat || !chat.members.includes(user.id)) return send(res, 404, { error: 'Чат не найден' });
  if (!text) return send(res, 400, { error: 'Пустое сообщение' });
  if (chat.service) return send(res, 400, { error: 'В служебный чат писать нельзя' });

  const msg = { id: uid(), from: user.id, text, time: now(), deleted: false };
  chat.msgs.push(msg);
  save();

  const peerId = chat.members.find(m => m !== user.id);
  if (peerId) {
    push(peerId, {
      type: 'message', chatId: chat.id,
      message: { id: msg.id, text, time: msg.time, out: false }
    });
  }
  send(res, 200, { message: { id: msg.id, text, time: msg.time, out: true } });
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

/* ---------- Кошелёк ---------- */

const RATES = { rub: 1, usd: 91, aed: 24.8 };

/* Создание платежа. Деньги на баланс НЕ зачисляются здесь —
   только после подтверждения от платёжной системы. */
route('POST', '/api/wallet/payment/create', async (req, res, body, user) => {
  const amount = Math.round(Number(body.amount));
  const method = body.method === 'card' ? 'bank_card' : 'sbp';

  if (!(amount >= 100)) return send(res, 400, { error: 'Минимум 100 ₽' });
  if (amount > 100000) return send(res, 400, { error: 'Максимум 100 000 ₽ за раз' });
  if (user.frozen) return send(res, 403, { error: 'Кошелёк заморожен' });

  if (!YK_ENABLED) {
    /* Тестовый режим: платёжка не подключена, зачисляем сразу и честно об этом пишем */
    const fake = {
      id: 'test-' + uid(),
      amount: { value: String(amount) },
      metadata: { userId: user.id },
      payment_method: { type: 'bank_card', card: { first6: '555555', last4: '4444' } }
    };
    creditPayment(fake);
    return send(res, 200, { testMode: true, balance: user.balance });
  }

  const payment = await yk('POST', '/payments', {
    amount: { value: amount.toFixed(2), currency: 'RUB' },
    payment_method_data: { type: method },
    confirmation: {
      type: 'redirect',
      return_url: (body.returnUrl || PUBLIC_URL || 'https://example.com') + '?paid=1'
    },
    capture: true,
    description: 'Пополнение кошелька Newchat',
    metadata: { userId: user.id }
  }, uid());

  if (!payment || !payment.id) {
    return send(res, 502, { error: 'Платёжная система недоступна' });
  }

  db.payments[payment.id] = { userId: user.id, amount, credited: false, time: now() };
  save();

  send(res, 200, {
    paymentId: payment.id,
    url: payment.confirmation && payment.confirmation.confirmation_url
  });
});

/* Приложение спрашивает: платёж прошёл? Проверяем у провайдера, не у клиента. */
route('POST', '/api/wallet/payment/check', async (req, res, body, user) => {
  const id = String(body.paymentId || '');
  const rec = db.payments[id];
  if (!rec || rec.userId !== user.id) return send(res, 404, { error: 'Платёж не найден' });
  if (rec.credited) return send(res, 200, { status: 'ok', balance: user.balance });
  if (!YK_ENABLED) return send(res, 200, { status: 'pending' });

  const payment = await yk('GET', '/payments/' + id);
  if (!payment) return send(res, 200, { status: 'pending' });

  if (payment.status === 'succeeded') {
    creditPayment(payment);
    return send(res, 200, { status: 'ok', balance: user.balance });
  }
  if (payment.status === 'canceled') return send(res, 200, { status: 'canceled' });
  send(res, 200, { status: 'pending' });
});

/* Уведомление от платёжной системы */
route('POST', '/yookassa/webhook', async (req, res, body) => {
  send(res, 200, { ok: true });
  try {
    const object = body && body.object;
    if (!object || !object.id) return;

    /* Телу уведомления не доверяем — перезапрашиваем платёж у провайдера */
    const payment = YK_ENABLED ? await yk('GET', '/payments/' + object.id) : object;
    if (!payment) return;

    if (body.event === 'payment.succeeded' && payment.status === 'succeeded') {
      creditPayment(payment);
    }
    if (body.event === 'refund.succeeded' || payment.refunded_amount) {
      handleRefund(payment);
    }
  } catch (e) {
    console.error('Ошибка вебхука оплаты:', e.message);
  }
});

/* Что человек видит на экране вывода */
route('GET', '/api/wallet/withdraw/info', async (req, res, body, user) => {
  send(res, 200, withdrawInfo(user));
});

/* Заявка на вывод */
route('POST', '/api/wallet/withdraw', async (req, res, body, user) => {
  const amount = Math.round(Number(body.amount));
  const cardKeyReq = String(body.card || '');
  const info = withdrawInfo(user);

  if (info.blocks.length) return send(res, 403, { error: info.blocks[0] });
  if (!(amount >= RULES.minWithdraw)) return send(res, 400, { error: `Минимум ${RULES.minWithdraw} ₽` });
  if (amount > RULES.maxPerRequest) return send(res, 400, { error: `Максимум ${RULES.maxPerRequest.toLocaleString('ru')} ₽ за раз` });
  if (amount > user.balance) return send(res, 400, { error: 'Недостаточно средств' });
  if (info.usedToday + amount > RULES.maxPerDay) return send(res, 400, { error: 'Превышен дневной лимит' });

  const card = info.cards.find(c => c.key === cardKeyReq);
  if (!card) return send(res, 400, { error: 'Выберите карту, с которой пополняли' });
  if (amount > card.available) {
    return send(res, 400, {
      error: `На эту карту доступно ${card.available.toLocaleString('ru')} ₽. Остальное ещё «остывает» после пополнения.`
    });
  }

  /* Списываем сразу, чтобы нельзя было подать две заявки на одни деньги */
  user.balance -= amount;

  let rest = amount;
  for (const d of (db.deposits[user.id] || [])) {
    if (rest <= 0) break;
    if (d.cardKey !== cardKeyReq || d.left <= 0) continue;
    if (now() - d.time < RULES.holdDays * 86400e3) continue;
    const take = Math.min(d.left, rest);
    d.left -= take;
    rest -= take;
  }

  const payout = {
    id: uid(),
    userId: user.id,
    amount,
    card: cardKeyReq,
    cardTitle: card.title,
    status: (PAYOUTS_AUTO && amount <= RULES.manualAbove) ? 'processing' : 'pending',
    time: now()
  };
  db.payouts.push(payout);

  db.history[user.id] = db.history[user.id] || [];
  db.history[user.id].unshift({
    amt: -amount,
    title: 'Вывод на ' + card.title,
    sub: payout.status === 'pending' ? 'на проверке' : 'отправлено',
    time: now()
  });
  save();

  serviceMessage(user.id, payout.status === 'pending'
    ? `Заявка на вывод ${amount.toLocaleString('ru')} ₽ принята и проверяется. Обычно занимает до суток.`
    : `Вывод ${amount.toLocaleString('ru')} ₽ отправлен на ${card.title}.`);

  send(res, 200, { ok: true, status: payout.status, balance: user.balance });
});

/* ---------- Модерация выплат (только для владельца) ---------- */

route('POST', '/api/admin/payouts', async (req, res, body) => {
  if (!ADMIN_TOKEN || body.adminToken !== ADMIN_TOKEN) return send(res, 403, { error: 'Нет доступа' });

  if (body.action === 'list') {
    return send(res, 200, {
      payouts: db.payouts.slice(-100).reverse().map(p => {
        const u = db.users[p.userId];
        return Object.assign({}, p, {
          user: u ? { name: u.name, username: u.username, phone: u.phone, trust: u.trust } : null
        });
      })
    });
  }

  const payout = db.payouts.find(p => p.id === body.payoutId);
  if (!payout) return send(res, 404, { error: 'Заявка не найдена' });
  const user = db.users[payout.userId];

  if (body.action === 'approve') {
    payout.status = 'done';
    save();
    if (user) serviceMessage(user.id, `Вывод ${payout.amount.toLocaleString('ru')} ₽ выполнен.`);
    return send(res, 200, { ok: true });
  }

  if (body.action === 'reject') {
    payout.status = 'rejected';
    payout.reason = String(body.reason || 'не прошло проверку');
    if (user) {
      user.balance += payout.amount;  // возвращаем деньги на баланс
      serviceMessage(user.id, `Вывод ${payout.amount.toLocaleString('ru')} ₽ отклонён: ${payout.reason}. Деньги вернулись на баланс.`);
      push(user.id, { type: 'state' });
    }
    save();
    return send(res, 200, { ok: true });
  }

  send(res, 400, { error: 'Неизвестное действие' });
});

/* ---------- Премиум ---------- */

route('POST', '/api/premium/buy', async (req, res, body, user) => {
  if (user.frozen) return send(res, 403, { error: 'Кошелёк заморожен' });
  if (user.balance < PREMIUM.price) {
    return send(res, 400, { error: `Не хватает ${(PREMIUM.price - user.balance).toLocaleString('ru')} ₽. Пополните кошелёк.` });
  }

  user.balance -= PREMIUM.price;
  /* Если премиум ещё действует — продлеваем, а не обнуляем */
  const from = isPremium(user) ? user.premiumUntil : now();
  user.premiumUntil = from + PREMIUM.days * 86400e3;

  db.history[user.id] = db.history[user.id] || [];
  db.history[user.id].unshift({
    amt: -PREMIUM.price,
    title: 'Newchat Premium',
    sub: PREMIUM.days + ' дней · ' + PREMIUM.slots + ' слотов',
    time: now()
  });
  save();

  const until = new Date(user.premiumUntil).toLocaleDateString('ru');
  serviceMessage(user.id, `Премиум активен до ${until}. Теперь у вас ${PREMIUM.slots} слотов под юзернеймы.`);
  send(res, 200, { state: fullState(user) });
});

/* ---------- Юзернеймы и биржа ---------- */

route('POST', '/api/usernames/claim', async (req, res, body, user) => {
  const username = normUsername(body.username);
  const mine = myUsernames(user.id);
  const limit = slotLimit(user);

  if (username.length < 3) return send(res, 400, { error: 'Минимум 3 символа' });
  if (db.usernames[username]) return send(res, 400, { error: 'Этот юзернейм уже занят' });
  if (mine.length >= limit) {
    return send(res, 400, {
      error: isPremium(user) ? 'Все слоты заняты' : `Занято ${limit} из ${limit}. Премиум даёт ${PREMIUM.slots} слотов.`
    });
  }

  db.usernames[username] = { owner: user.id, main: false, forSale: false, price: 0 };
  save();
  send(res, 200, { usernames: myUsernames(user.id) });
});

route('POST', '/api/usernames/sell', async (req, res, body, user) => {
  const username = normUsername(body.username);
  const price = Math.round(Number(body.price));
  const rec = db.usernames[username];

  if (!rec || rec.owner !== user.id) return send(res, 403, { error: 'Это не ваш юзернейм' });
  if (!(price > 0)) return send(res, 400, { error: 'Укажите цену' });
  if (rec.main) return send(res, 400, { error: 'Основной юзернейм продать нельзя' });

  rec.forSale = true;
  rec.price = price;
  save();
  send(res, 200, { usernames: myUsernames(user.id) });
});

route('POST', '/api/usernames/buy', async (req, res, body, user) => {
  const username = normUsername(body.username);
  const rec = db.usernames[username];

  if (!rec || !rec.forSale) return send(res, 404, { error: 'Лот не найден' });
  if (rec.owner === user.id) return send(res, 400, { error: 'Это ваш лот' });
  if (user.frozen) return send(res, 403, { error: 'Кошелёк заморожен' });
  if (user.balance < rec.price) return send(res, 400, { error: 'Недостаточно средств' });
  if (myUsernames(user.id).length >= slotLimit(user)) {
    return send(res, 400, { error: `Нет свободных слотов. Премиум даёт ${PREMIUM.slots}.` });
  }

  const seller = db.users[rec.owner];
  const price = rec.price;
  /* Комиссия площадки — на неё живёт сервис */
  const fee = Math.round(price * FEE_PERCENT / 100);
  const toSeller = price - fee;

  user.balance -= price;
  seller.balance += toSeller;

  db.history[user.id] = db.history[user.id] || [];
  db.history[user.id].unshift({ amt: -price, title: 'Покупка @' + username, sub: 'биржа юзернеймов', time: now() });
  db.history[seller.id] = db.history[seller.id] || [];
  db.history[seller.id].unshift({
    amt: toSeller,
    title: 'Продажа @' + username,
    sub: fee ? `комиссия ${FEE_PERCENT}% — ${fee.toLocaleString('ru')} ₽` : 'биржа юзернеймов',
    time: now()
  });

  rec.owner = user.id;
  rec.forSale = false;
  rec.price = 0;
  rec.main = false;
  save();

  serviceMessage(seller.id,
    `Юзернейм @${username} продан за ${price.toLocaleString('ru')} ₽.` +
    (fee ? ` Комиссия ${FEE_PERCENT}% — ${fee.toLocaleString('ru')} ₽.` : '') +
    ` На кошелёк зачислено ${toSeller.toLocaleString('ru')} ₽. Тратить внутри можно сразу, вывод — после проверки.`);
  push(seller.id, { type: 'state' });

  send(res, 200, { state: fullState(user) });
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
    payments: YK_ENABLED,
    rules: {
      minWithdraw: RULES.minWithdraw,
      holdDays: RULES.holdDays,
      maxPerDay: RULES.maxPerDay
    }
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
  'POST /api/auth/telegram/start',
  'POST /api/auth/telegram/check',
  'POST /telegram/webhook',
  'POST /yookassa/webhook',
  'POST /api/admin/payouts',
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
  const key = req.method + ' ' + url;
  const handler = routes[key];

  if (!handler) return send(res, 404, { error: 'Не найдено' });

  let user = null;
  if (!OPEN_ROUTES.includes(key)) {
    const token = (req.headers.authorization || '').replace('Bearer ', '');
    user = userByToken(token);
    if (!user) return send(res, 401, { error: 'Требуется вход' });
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
