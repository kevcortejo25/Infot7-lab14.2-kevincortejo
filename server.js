require("dotenv").config();

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const path = require("path");
const fs = require("fs");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const cookieParser = require("cookie-parser");
const mongoose = require("mongoose");
const { Redis } = require("@upstash/redis");

const app = express();
const server = http.createServer(app);
let wss = null;

const PORT = process.env.PORT || 3001;
const JWT_SECRET = process.env.JWT_SECRET || "local_secret_key";
const DATA_FILE = path.join(__dirname, "local-data.json");
const MONGODB_DB = process.env.MONGODB_DB || "infinity-chat";
const hasMongoPlaceholder = [process.env.MONGODB_DIRECT_URI, process.env.MONGODB_URI]
  .some(uri => uri && uri.includes("<db_password>"));
const ATLAS_DIRECT_HOSTS = "ac-doq5i7d-shard-00-00.xo02jzk.mongodb.net:27017,ac-doq5i7d-shard-00-01.xo02jzk.mongodb.net:27017,ac-doq5i7d-shard-00-02.xo02jzk.mongodb.net:27017";
const ATLAS_REPLICA_SET = "atlas-301nex-shard-0";
const MONGODB_URIS = getMongoUris();
const hasRedisCredentials = Boolean(process.env.UPSTASH_REDIS_REST_URL && process.env.UPSTASH_REDIS_REST_TOKEN);
const redis = hasRedisCredentials
  ? new Redis({
      url: process.env.UPSTASH_REDIS_REST_URL,
      token: process.env.UPSTASH_REDIS_REST_TOKEN
    })
  : null;

app.use(express.json({ limit: "10mb" }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, "public")));

function getMongoUris() {
  const uris = [process.env.MONGODB_DIRECT_URI];
  const srvUri = process.env.MONGODB_URI || "";
  const srvMatch = srvUri.match(/^mongodb\+srv:\/\/([^@]+)@cluster0\.xo02jzk\.mongodb\.net\/?\??(.*)$/);

  if (srvMatch) {
    const credentials = srvMatch[1];
    const appNameMatch = srvUri.match(/[?&]appName=([^&]+)/);
    const appName = appNameMatch ? appNameMatch[1] : "Cluster0";
    uris.push(
      `mongodb://${credentials}@${ATLAS_DIRECT_HOSTS}/${MONGODB_DB}?ssl=true&replicaSet=${ATLAS_REPLICA_SET}&authSource=admin&retryWrites=true&w=majority&appName=${appName}`
    );
  }

  uris.push(process.env.MONGODB_URI);

  return Array.from(new Set(uris.filter(uri => uri && !uri.includes("<db_password>"))));
}

app.get("/api/health", (req, res) => {
  res.json({
    success: true,
    storage: storageMode,
    mongoConfigured: MONGODB_URIS.length > 0,
    mongoDatabase: MONGODB_DB,
    mongoNeedsPassword: hasMongoPlaceholder,
    redisConfigured: hasRedisCredentials
  });
});

const userSchema = new mongoose.Schema({ _id: String }, { strict: false, collection: "users" });
const messageSchema = new mongoose.Schema({ _id: String }, { strict: false, collection: "messages" });
const groupSchema = new mongoose.Schema({ _id: String }, { strict: false, collection: "groups" });

const UserModel = mongoose.model("User", userSchema);
const MessageModel = mongoose.model("Message", messageSchema);
const GroupModel = mongoose.model("Group", groupSchema);

let storageMode = "local";

function loadLocalData() {
  try {
    if (!fs.existsSync(DATA_FILE)) {
      fs.writeFileSync(DATA_FILE, JSON.stringify({ users: [], messages: [], groups: [] }, null, 2));
    }
    return JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
  } catch {
    return { users: [], messages: [], groups: [] };
  }
}

function saveLocalData(data) {
  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
}

async function loadMongoData() {
  const [users, messages, groups] = await Promise.all([
    UserModel.find({}).lean(),
    MessageModel.find({}).lean(),
    GroupModel.find({}).lean()
  ]);

  return {
    users: users.map(({ __v, ...user }) => user),
    messages: messages.map(({ __v, ...message }) => message),
    groups: groups.map(({ __v, ...group }) => group)
  };
}

async function persistMongoCollection(Model, items) {
  await Model.deleteMany({});
  if (items.length) {
    await Model.insertMany(items, { ordered: false });
  }
}

async function persistData(data) {
  if (storageMode === "mongo") {
    await Promise.all([
      persistMongoCollection(UserModel, data.users),
      persistMongoCollection(MessageModel, data.messages),
      persistMongoCollection(GroupModel, data.groups)
    ]);
    return;
  }

  saveLocalData(data);
}

function saveData(data) {
  persistData(data).catch(error => {
    console.error("[DATA] Failed to persist data:", error.message);
  });
}

async function connectMongo() {
  let lastError = null;

  for (const uri of MONGODB_URIS) {
    try {
      await mongoose.connect(uri, {
        dbName: MONGODB_DB,
        serverSelectionTimeoutMS: 10000
      });
      storageMode = "mongo";
      return true;
    } catch (error) {
      lastError = error;
      await mongoose.disconnect().catch(() => {});
    }
  }

  if (lastError) {
    console.warn("[MONGODB] Connection failed:", lastError.message);
    console.warn("[MONGODB] Check Atlas Database Access username/password and Network Access allowlist.");
  } else if (hasMongoPlaceholder) {
    console.warn("[MONGODB] MONGODB_URI still contains <db_password>. Replace it with the real Atlas database user password.");
  }

  return false;
}

async function loadData() {
  if (!MONGODB_URIS.length) {
    return loadLocalData();
  }

  const mongoConnected = await connectMongo();
  if (!mongoConnected) {
    storageMode = "local";
    return loadLocalData();
  }

  const data = await loadMongoData();

  if (!data.users.length && !data.messages.length && !data.groups.length) {
    const localData = loadLocalData();
    if (localData.users.length || localData.messages.length || localData.groups.length) {
      await persistData(localData);
      return localData;
    }
  }

  return data;
}

async function connectRedis() {
  if (!redis) return false;

  await redis.ping();
  await redis.del("infinity-chat:online");
  return true;
}

function trackUserOnline(username) {
  if (!redis) return;
  redis.sadd("infinity-chat:online", username).catch(error => {
    console.error("[REDIS] Failed to mark user online:", error.message);
  });
}

function trackUserOffline(username) {
  if (!redis) return;
  redis.srem("infinity-chat:online", username).catch(error => {
    console.error("[REDIS] Failed to mark user offline:", error.message);
  });
}

let db = { users: [], messages: [], groups: [] };
const onlineUsers = new Map();

function normalizeData() {
  const seenUsers = new Set();
  const uniqueUsers = [];

  db.users.forEach(user => {
    user.username = String(user.username || "").trim().toLowerCase();
    if (!user.username || seenUsers.has(user.username)) return;
    seenUsers.add(user.username);
    uniqueUsers.push(user);
  });

  db.users = uniqueUsers;
  db.groups = db.groups.map(group => ({
    ...group,
    admin: group.admin || group.creator,
    members: Array.from(new Set((group.members || [])
      .map(member => String(member).trim().toLowerCase())
      .filter(member => seenUsers.has(member))))
  })).filter(group => group.members.length > 0);

  db.messages = db.messages.filter(message => seenUsers.has(message.username));
  saveData(db);
}

function scheduleNormalizeData() {
  try {
    normalizeData();
  } catch (error) {
    console.error("[DATA] Failed to normalize data:", error.message);
  }
}

function createToken(user) {
  return jwt.sign(
    { id: user._id, username: user.username, avatar: user.avatar, bio: user.bio || "" },
    JWT_SECRET,
    { expiresIn: "7d" }
  );
}

function auth(req, res, next) {
  let token = req.cookies.token;

  if (!token && req.headers.authorization?.startsWith("Bearer ")) {
    token = req.headers.authorization.split(" ")[1];
  }

  if (!token) return res.status(401).json({ error: "No token provided." });

  try {
    req.user = jwt.verify(token, JWT_SECRET);
    next();
  } catch {
    res.status(403).json({ error: "Invalid token." });
  }
}

function broadcast(data) {
  if (!wss) return;

  const message = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

function sendToUsers(usernames, data) {
  const message = JSON.stringify(data);
  usernames.forEach(username => {
    const sockets = onlineUsers.get(username);
    if (!sockets) return;

    sockets.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });
  });
}

function getUserPublic(user) {
  return {
    username: user.username,
    avatar: user.avatar,
    bio: user.bio || "",
    online: onlineUsers.has(user.username)
  };
}

function normalizeMembers(members) {
  if (!Array.isArray(members)) return [];
  return members
    .map(member => String(member).trim().toLowerCase())
    .filter(Boolean);
}

function privateRoomId(userA, userB) {
  return ["dm", ...[userA, userB].sort()].join(":");
}

function getRoomAccess(roomId, username) {
  if (roomId === "lounge") {
    return { ok: true, members: db.users.map(user => user.username), title: "Lounge Chat" };
  }

  if (roomId?.startsWith("dm:")) {
    const members = roomId.split(":").slice(1);
    return {
      ok: members.length === 2 && members.includes(username),
      members,
      title: members.find(member => member !== username) || "Direct Message"
    };
  }

  const group = db.groups.find(item => item._id === roomId);
  if (!group) return { ok: false, members: [] };

  return {
    ok: group.members.includes(username),
    members: group.members,
    title: group.name,
    group
  };
}

function getOnlineList() {
  return Array.from(onlineUsers.keys()).map(username => {
    const user = db.users.find(u => u.username === username);
    return {
      username,
      avatar: user?.avatar || "",
      bio: user?.bio || ""
    };
  });
}

app.post("/api/auth/signup", async (req, res) => {
  try {
    const { username, password, avatar } = req.body;

    if (!username || !password) {
      return res.status(400).json({ error: "Username and password are required." });
    }

    const cleanUsername = username.trim().toLowerCase();

    if (db.users.find(u => u.username === cleanUsername)) {
      return res.status(400).json({ error: "Username already exists." });
    }

    const hashedPassword = await bcrypt.hash(password, 10);

    const user = {
      _id: "u_" + Date.now(),
      username: cleanUsername,
      password: hashedPassword,
      avatar: avatar || `https://api.dicebear.com/9.x/bottts/svg?seed=${cleanUsername}`,
      bio: "Infinity Chat user",
      createdAt: new Date().toISOString()
    };

    db.users.push(user);
    saveData(db);

    const token = createToken(user);

    res.cookie("token", token, {
      httpOnly: true,
      maxAge: 7 * 24 * 60 * 60 * 1000
    });

    res.json({
      success: true,
      token,
      user: {
        username: user.username,
        avatar: user.avatar,
        bio: user.bio
      }
    });
  } catch {
    res.status(500).json({ error: "Signup failed." });
  }
});

app.post("/api/auth/login", async (req, res) => {
  try {
    const { username, password } = req.body;
    const cleanUsername = username.trim().toLowerCase();

    const user = db.users.find(u => u.username === cleanUsername);
    if (!user) return res.status(400).json({ error: "Invalid username or password." });

    const match = await bcrypt.compare(password, user.password);
    if (!match) return res.status(400).json({ error: "Invalid username or password." });

    const token = createToken(user);

    res.cookie("token", token, {
      httpOnly: true,
      maxAge: 7 * 24 * 60 * 60 * 1000
    });

    res.json({
      success: true,
      token,
      user: {
        username: user.username,
        avatar: user.avatar,
        bio: user.bio
      }
    });
  } catch {
    res.status(500).json({ error: "Login failed." });
  }
});

app.get("/api/auth/me", auth, (req, res) => {
  const user = db.users.find(u => u.username === req.user.username);
  res.json({
    success: true,
    user: {
      username: user.username,
      avatar: user.avatar,
      bio: user.bio
    }
  });
});

app.post("/api/auth/logout", (req, res) => {
  res.clearCookie("token");
  res.json({ success: true });
});

app.get("/api/users", auth, (req, res) => {
  const users = db.users.map(getUserPublic);

  res.json({ success: true, users });
});

app.put("/api/profile", auth, (req, res) => {
  const { avatar, bio } = req.body;
  const user = db.users.find(u => u.username === req.user.username);

  if (!user) return res.status(404).json({ error: "User not found." });

  if (avatar) user.avatar = avatar;
  user.bio = bio || "";

  saveData(db);

  res.json({
    success: true,
    user: {
      username: user.username,
      avatar: user.avatar,
      bio: user.bio
    }
  });
});

app.get("/api/messages/:roomId", auth, (req, res) => {
  const roomId = req.params.roomId || "lounge";
  const access = getRoomAccess(roomId, req.user.username);

  if (!access.ok) {
    return res.status(403).json({ error: "You do not have access to this chat." });
  }

  const messages = db.messages
    .filter(m => m.roomId === roomId)
    .slice(-50);

  res.json({ success: true, messages });
});

app.post("/api/groups", auth, (req, res) => {
  const { name, members } = req.body;

  if (!name || !name.trim()) {
    return res.status(400).json({ error: "Group name is required." });
  }

  const requestedMembers = normalizeMembers(members);
  const validMembers = requestedMembers.filter(member =>
    member !== req.user.username && db.users.some(user => user.username === member)
  );

  const group = {
    _id: "g_" + Date.now(),
    name: name.trim(),
    admin: req.user.username,
    creator: req.user.username,
    members: Array.from(new Set([req.user.username, ...validMembers])),
    avatar: `https://api.dicebear.com/9.x/initials/svg?seed=${encodeURIComponent(name)}`,
    createdAt: new Date().toISOString()
  };

  db.groups.push(group);
  saveData(db);

  sendToUsers(group.members, { type: "groups_updated" });

  res.json({ success: true, group });
});

app.get("/api/groups", auth, (req, res) => {
  const groups = db.groups.filter(g => g.members.includes(req.user.username));
  res.json({ success: true, groups });
});

app.delete("/api/groups/:groupId", auth, (req, res) => {
  const group = db.groups.find(g => g._id === req.params.groupId);

  if (!group) return res.status(404).json({ error: "Group not found." });
  if ((group.admin || group.creator) !== req.user.username) {
    return res.status(403).json({ error: "Only the group admin can delete this group." });
  }

  const members = [...group.members];
  db.groups = db.groups.filter(g => g._id !== group._id);
  db.messages = db.messages.filter(message => message.roomId !== group._id);
  saveData(db);

  sendToUsers(members, { type: "group_deleted", roomId: group._id });
  res.json({ success: true });
});

app.delete("/api/groups/:groupId/members/:username", auth, (req, res) => {
  const group = db.groups.find(g => g._id === req.params.groupId);
  const member = req.params.username.trim().toLowerCase();

  if (!group) return res.status(404).json({ error: "Group not found." });
  if ((group.admin || group.creator) !== req.user.username) {
    return res.status(403).json({ error: "Only the group admin can remove members." });
  }
  if (member === req.user.username) {
    return res.status(400).json({ error: "The admin cannot remove themselves." });
  }

  group.members = group.members.filter(username => username !== member);
  saveData(db);

  sendToUsers([...group.members, member], { type: "groups_updated", roomId: group._id });
  res.json({ success: true, group });
});

function setupWebSocketServer() {
  wss = new WebSocket.Server({ server });

  wss.on("connection", (ws, req) => {
    const params = new URLSearchParams(req.url.split("?")[1]);
    const token = params.get("token");

    let user;

    try {
      user = jwt.verify(token, JWT_SECRET);
    } catch {
      ws.close();
      return;
    }

    ws.username = user.username;

    if (!onlineUsers.has(user.username)) {
      onlineUsers.set(user.username, new Set());
    }

    onlineUsers.get(user.username).add(ws);
    trackUserOnline(user.username);

    broadcast({
      type: "online_list",
      users: getOnlineList()
    });

    ws.send(JSON.stringify({
      type: "history",
      roomId: "lounge",
      messages: db.messages.filter(m => m.roomId === "lounge").slice(-50)
    }));

    ws.on("message", raw => {
      try {
        const data = JSON.parse(raw.toString());

        if (!data.message || !data.message.trim()) return;

        const currentUser = db.users.find(u => u.username === user.username);
        const roomId = data.roomId || "lounge";
        const access = getRoomAccess(roomId, user.username);

        if (!access.ok) return;

        const msg = {
          _id: "m_" + Date.now(),
          type: "chat",
          roomId,
          roomTitle: access.title,
          username: user.username,
          avatar: currentUser?.avatar || user.avatar,
          message: data.message.trim(),
          timestamp: new Date().toISOString()
        };

        db.messages.push(msg);
        saveData(db);

        sendToUsers(access.members, msg);
      } catch {
        console.log("Invalid message.");
      }
    });

    ws.on("close", () => {
      const set = onlineUsers.get(user.username);

      if (set) {
        set.delete(ws);

        if (set.size === 0) {
          onlineUsers.delete(user.username);
          trackUserOffline(user.username);
        }
      }

      broadcast({
        type: "online_list",
        users: getOnlineList()
      });
    });
  });
}

async function startServer() {
  try {
    console.log("[SYSTEM] Loading data...");
    db = await loadData();
    console.log(`[SYSTEM] Data loaded from ${storageMode}.`);
    scheduleNormalizeData();

    console.log("[SYSTEM] Connecting Redis...");
    const redisConnected = await connectRedis().catch(error => {
      console.error("[REDIS] Connection failed:", error.message);
      return false;
    });

    console.log("[SYSTEM] Starting HTTP server...");
    await new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(PORT, () => {
        server.off("error", reject);
        resolve();
      });
    });

    setupWebSocketServer();

    console.log(storageMode === "mongo"
      ? `[MONGODB] Connected to "${MONGODB_DB}" database.`
      : "[LOCAL DB] Loaded local-data.json");
    console.log(redisConnected
      ? "[REDIS] Connected to Upstash Redis."
      : "[SYSTEM] Redis credentials not found or unavailable. Using in-memory presence.");
    console.log(`[SYSTEM] Server listening on http://localhost:${PORT}`);
  } catch (error) {
    if (error.code === "EADDRINUSE") {
      console.error(`[SYSTEM] Port ${PORT} is already in use. Stop the existing Node server or change PORT in .env.`);
      process.exitCode = 1;
      return;
    }

    console.error("[SYSTEM] Failed to start server:", error.message);
    process.exitCode = 1;
  }
}

startServer();
