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

/* ================= ХРАНИЛИЩЕ ================= */

let db = {
  users: {},      // userId -> { id, phone, name, username, cover, ava, status, verified, dev, balance, trust, createdAt }
  usernames: {},  // username -> { owner, main, forSale, price }
  chats: {},      // chatId -> { id, members:[a,b], service, msgs:[] }
  history: {},    // userId -> [операции кошелька]
  reports: [],    // жалобы
  verifyRequests: [],
  tokens: {},     // token -> userId
  codes: {}       // phone -> { code, expires }
};

function load() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      db = Object.assign(db, JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')));
      console.log('База загружена');
    }
  } catch (e) {
    console.error('Не удалось прочитать базу:', e.message);
  }
}

let saveTimer = null;
function save() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    try {
      fs.writeFileSync(DATA_FILE, JSON.stringify(db));
    } catch (e) {
      console.error('Не удалось сохранить базу:', e.message);
    }
  }, 200);
}

const uid = () => crypto.randomBytes(9).toString('hex');
const now = () => Date.now();

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
      phone: user.phone, balance: user.balance, trust: user.trust
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

  const code = String(Math.floor(10000 + Math.random() * 90000));
  db.codes[phone] = { code, expires: now() + 10 * 60 * 1000 };
  save();

  console.log(`Код для +7${phone}: ${code}`);

  /* Пока SMS-шлюз не подключён, код возвращается прямо в ответе.
     Когда подключишь SMS-сервис — отправляй тут и убери devCode. */
  send(res, 200, { ok: true, devCode: code });
});

route('POST', '/api/auth/verify', async (req, res, body) => {
  const phone = normPhone(body.phone);
  const code = String(body.code || '').replace(/\D/g, '');
  const rec = db.codes[phone];

  if (!rec || rec.expires < now()) return send(res, 400, { error: 'Код истёк, запросите новый' });
  if (rec.code !== code) return send(res, 400, { error: 'Неверный код' });

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

route('POST', '/api/wallet/topup', async (req, res, body, user) => {
  const cur = RATES[body.currency] ? body.currency : 'rub';
  const amount = Number(body.amount);
  if (!(amount > 0)) return send(res, 400, { error: 'Введите сумму' });

  const rub = Math.round(amount * RATES[cur]);
  user.balance += rub;
  db.history[user.id] = db.history[user.id] || [];
  db.history[user.id].unshift({
    amt: rub,
    title: 'Пополнение · ' + (cur === 'rub' ? '₽' : cur === 'usd' ? '$' : 'AED'),
    sub: cur === 'rub' ? 'с карты' : amount + ' → конвертация в рубли',
    time: now()
  });
  save();
  send(res, 200, { balance: user.balance, history: db.history[user.id] });
});

route('POST', '/api/wallet/withdraw', async (req, res, body, user) => {
  const card = String(body.card || '').replace(/\D/g, '');
  const amount = Math.round(Number(body.amount));
  if (card.length < 16) return send(res, 400, { error: 'Введите номер карты полностью' });
  if (!(amount >= 100)) return send(res, 400, { error: 'Минимальная сумма — 100 ₽' });
  if (amount > user.balance) return send(res, 400, { error: 'Недостаточно средств' });

  user.balance -= amount;
  db.history[user.id] = db.history[user.id] || [];
  db.history[user.id].unshift({
    amt: -amount,
    title: 'Вывод на карту •• ' + card.slice(-4),
    sub: 'комиссия 1%',
    time: now()
  });
  save();
  send(res, 200, { balance: user.balance, history: db.history[user.id], last4: card.slice(-4) });
});

/* ---------- Юзернеймы и биржа ---------- */

route('POST', '/api/usernames/claim', async (req, res, body, user) => {
  const username = normUsername(body.username);
  const mine = myUsernames(user.id);
  const limit = 3;

  if (username.length < 3) return send(res, 400, { error: 'Минимум 3 символа' });
  if (db.usernames[username]) return send(res, 400, { error: 'Этот юзернейм уже занят' });
  if (mine.length >= limit) return send(res, 400, { error: 'Все слоты заняты' });

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
  if (user.balance < rec.price) return send(res, 400, { error: 'Недостаточно средств' });
  if (myUsernames(user.id).length >= 3) return send(res, 400, { error: 'Нет свободных слотов' });

  const seller = db.users[rec.owner];
  const price = rec.price;

  user.balance -= price;
  seller.balance += price;

  db.history[user.id] = db.history[user.id] || [];
  db.history[user.id].unshift({ amt: -price, title: 'Покупка @' + username, sub: 'биржа юзернеймов', time: now() });
  db.history[seller.id] = db.history[seller.id] || [];
  db.history[seller.id].unshift({ amt: price, title: 'Продажа @' + username, sub: 'биржа юзернеймов', time: now() });

  rec.owner = user.id;
  rec.forSale = false;
  rec.price = 0;
  rec.main = false;
  save();

  serviceMessage(seller.id, `Юзернейм @${username} продан за ${price.toLocaleString('ru')} ₽. Деньги зачислены на кошелёк.`);
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

/* ---------- Служебное ---------- */

route('GET', '/api/health', async (req, res) => {
  send(res, 200, { ok: true, users: Object.keys(db.users).length });
});

/* ================= СЕРВЕР ================= */

const OPEN_ROUTES = ['POST /api/auth/request', 'POST /api/auth/verify', 'GET /api/health'];

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

load();
server.listen(PORT, () => console.log('Newchat-сервер запущен на порту ' + PORT));
