import asyncio
import json
import sqlite3
import hashlib
import os
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
        sender TEXT, target TEXT, content TEXT, ts TEXT
    )""")
    conn.commit()
    conn.close()


async def broadcast_user_list():
    users_list = list(connected.keys())
    payload = json.dumps({"type": "user_list", "users": users_list})
    for ws in list(connected.values()):
        try:
            await ws.send(payload)
        except Exception:
            pass


async def handler(websocket):
    username = None
    try:
        async for raw in websocket:
            data = json.loads(raw)
            msg_type = data.get("type")

            if msg_type == "register":
                u_name = data.get("username", "").strip()
                p_word = data.get("password", "").strip()

                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT username FROM users WHERE username=?", (u_name,))
                if c.fetchone():
                    await websocket.send(json.dumps({"type": "auth_res", "success": False, "msg": "Bu kullanıcı adı zaten alınmış!"}))
                else:
                    c.execute("INSERT INTO users(username, fullname, password_hash) VALUES(?,?,?)",
                              (u_name, u_name, hash_password(p_word)))
                    conn.commit()
                    await websocket.send(json.dumps({"type": "auth_res", "success": True, "msg": "Kayıt başarılı!"}))
                conn.close()

            elif msg_type == "login":
                u_name = data.get("username", "").strip()
                p_word = data.get("password", "").strip()

                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT password_hash FROM users WHERE username=?", (u_name,))
                row = c.fetchone()
                
                # Kullanıcı yoksa otomatik kayıt et (Kolay erişim için)
                if not row:
                    c.execute("INSERT INTO users(username, fullname, password_hash) VALUES(?,?,?)",
                              (u_name, u_name, hash_password(p_word)))
                    conn.commit()
                    row = (hash_password(p_word),)

                conn.close()

                if row and row[0] == hash_password(p_word):
                    username = u_name
                    connected[username] = websocket
                    await websocket.send(json.dumps({"type": "auth_res", "success": True, "msg": "Giriş başarılı!"}))
                    await broadcast_user_list()
                else:
                    await websocket.send(json.dumps({"type": "auth_res", "success": False, "msg": "Şifre hatalı!"}))

            elif msg_type == "chat":
                if not username:
                    continue
                sender = username
                target = data.get("target")
                content = data.get("message")
                ts = datetime.utcnow().strftime("%H:%M")

                out_payload = json.dumps({
                    "type": "chat",
                    "sender": sender,
                    "target": target,
                    "message": content,
                    "ts": ts
                })

                if target:
                    # Özel mesaj (DM)
                    if target in connected:
                        await connected[target].send(out_payload)
                    if sender in connected:
                        await connected[sender].send(out_payload)
                else:
                    # Genel Mesaj
                    for ws in list(connected.values()):
                        try:
                            await ws.send(out_payload)
                        except Exception:
                            pass

    except Exception as e:
        print(f"Hata: {e}")
    finally:
        if username and username in connected:
            connected.pop(username, None)
            await broadcast_user_list()


async def main():
    init_db()
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(handler, "0.0.0.0", port):
        print(f"Zümrüt Sunucusu Aktif: Port {port}")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
