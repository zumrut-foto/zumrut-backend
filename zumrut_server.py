import asyncio
import json
import os
import sqlite3
import hashlib
from datetime import datetime

import websockets

DB_PATH = "zumrut.db"
connected = {}  # username -> websocket


def hash_password(password):
    return hashlib.sha256(password.encode('utf-8')).hexdigest()


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY,
        password_hash TEXT,
        status TEXT DEFAULT 'available'
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS messages(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sender TEXT, target TEXT, content TEXT, ts TEXT
    )""")
    # Eski surumden kalan veritabanlarinda 'target' kolonu olmayabilir, ekle.
    c.execute("PRAGMA table_info(messages)")
    existing_cols = {row[1] for row in c.fetchall()}
    if "target" not in existing_cols:
        c.execute("ALTER TABLE messages ADD COLUMN target TEXT")
    conn.commit()
    conn.close()


async def broadcast(payload, only_users=None):
    """only_users=None -> herkese gonder. Aksi halde sadece verilen kullanici adlarina gonder."""
    dead = []
    targets = connected.items() if only_users is None else [
        (u, ws) for u, ws in connected.items() if u in only_users
    ]
    for user, ws in targets:
        try:
            await ws.send(json.dumps(payload))
        except Exception:
            dead.append(user)
    for u in dead:
        connected.pop(u, None)


async def send_user_list():
    users_list = list(connected.keys())
    await broadcast({
        "type": "user_list",
        "users": users_list
    })


async def handler(websocket):
    username = None
    try:
        async for raw in websocket:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            action = data.get("type")

            if action == "register":
                u_name = (data.get("username") or "").strip()
                p_word = data.get("password") or ""

                if not u_name or not p_word:
                    await websocket.send(json.dumps({"type": "auth_res", "success": False, "msg": "Kullanıcı adı ve şifre gerekli!"}))
                    continue

                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT username FROM users WHERE username=?", (u_name,))
                if c.fetchone():
                    await websocket.send(json.dumps({"type": "auth_res", "success": False, "msg": "Bu kullanıcı adı zaten alınmış!"}))
                else:
                    c.execute("INSERT INTO users(username, password_hash) VALUES(?,?)",
                              (u_name, hash_password(p_word)))
                    conn.commit()
                    await websocket.send(json.dumps({"type": "auth_res", "success": True, "msg": "Kayıt başarılı! Şimdi giriş yapabilirsiniz."}))
                conn.close()

            elif action == "login":
                u_name = (data.get("username") or "").strip()
                p_word = data.get("password") or ""

                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT password_hash FROM users WHERE username=?", (u_name,))
                row = c.fetchone()
                conn.close()

                if row and row[0] == hash_password(p_word):
                    if u_name in connected:
                        await websocket.send(json.dumps({"type": "auth_res", "success": False, "msg": "Bu hesap zaten şu an çevrimiçi!"}))
                    else:
                        username = u_name
                        connected[username] = websocket
                        await websocket.send(json.dumps({"type": "auth_res", "success": True, "msg": "Giriş başarılı!"}))
                        await send_user_list()
                        await broadcast({
                            "type": "presence",
                            "user": username,
                            "status": "online"
                        })
                else:
                    await websocket.send(json.dumps({"type": "auth_res", "success": False, "msg": "Kullanıcı adı veya şifre hatalı!"}))

            elif action == "chat":
                if not username:
                    continue
                message = data.get("message", "")
                target = data.get("target")
                ts = datetime.utcnow().strftime("%H:%M")

                conn = sqlite3.connect(DB_PATH)
                conn.execute(
                    "INSERT INTO messages(sender, target, content, ts) VALUES(?,?,?,?)",
                    (username, target, message, ts)
                )
                conn.commit()
                conn.close()

                out = {
                    "type": "chat",
                    "sender": username,
                    "message": message,
                    "target": target,
                    "ts": ts
                }

                if target:
                    # Ozel mesaj: sadece gonderen ve alici gorur
                    recipients = {username, target}
                    await broadcast(out, only_users=recipients)
                else:
                    await broadcast(out)

    finally:
        if username:
            connected.pop(username, None)
            await send_user_list()
            await broadcast({
                "type": "presence",
                "user": username,
                "status": "offline"
            })


async def main():
    init_db()
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"Zümrüt Sunucusu Aktif: Port {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
