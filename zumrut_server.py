import asyncio
import json
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
        fullname TEXT,
        password_hash TEXT,
        status TEXT DEFAULT 'available'
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS messages(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sender TEXT, channel TEXT, content TEXT,
        msg_type TEXT, ts TEXT
    )""")
    conn.commit()
    conn.close()


async def broadcast(payload):
    dead = []
    for user, ws in connected.items():
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
            data = json.loads(raw)
            action = data.get("action")

            if action == "register":
                u_name = data["username"].strip()
                f_name = data["fullname"].strip()
                p_word = data["password"]

                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT username FROM users WHERE username=?", (u_name,))
                if c.fetchone():
                    await websocket.send(json.dumps({"type": "auth_res", "success": False, "msg": "Bu kullanıcı adı zaten alınmış!"}))
                else:
                    c.execute("INSERT INTO users(username, fullname, password_hash) VALUES(?,?,?)",
                              (u_name, f_name, hash_password(p_word)))
                    conn.commit()
                    await websocket.send(json.dumps({"type": "auth_res", "success": True, "msg": "Kayıt başarılı! Şimdi giriş yapabilirsiniz."}))
                conn.close()

            elif action == "login":
                u_name = data["username"].strip()
                p_word = data["password"]

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

            elif action == "message":
                if not username:
                    continue
                ts = datetime.utcnow().strftime("%H:%M")
                conn = sqlite3.connect(DB_PATH)
                conn.execute(
                    "INSERT INTO messages(sender,channel,content,msg_type,ts) VALUES(?,?,?,?,?)",
                    (username, data.get("channel", "genel"), data["content"], data.get("msg_type", "text"), ts)
                )
                conn.commit()
                conn.close()

                await broadcast({
                    "type": "message",
                    "sender": username,
                    "channel": data.get("channel", "genel"),
                    "content": data["content"],
                    "msg_type": data.get("msg_type", "text"),
                    "ts": ts
                })

            elif action == "status":
                if not username:
                    continue
                conn = sqlite3.connect(DB_PATH)
                conn.execute("UPDATE users SET status=? WHERE username=?", (data["status"], username))
                conn.commit()
                conn.close()
                await broadcast({
                    "type": "status",
                    "user": username,
                    "status": data["status"]
                })

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
    async with websockets.serve(handler, "0.0.0.0", 8765):
        print("Zümrüt Sunucusu Aktif: Port 8765")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
