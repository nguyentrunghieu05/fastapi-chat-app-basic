import asyncio
import json
import redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends
from sqlalchemy.orm import Session
from passlib.context import CryptContext
from typing import List
from database import SessionLocal, engine, Base
from models import User, Message

# Khởi tạo DB
Base.metadata.create_all(bind=engine)

app = FastAPI()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Cấu hình Redis
# Đảm bảo Redis Server đang chạy trên cổng 6379 (mặc định)
REDIS_URL = "redis://localhost:6379"
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
REDIS_CHANNEL = "chat_messages"

# Dependency DB
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ----------------------
# REST API Endpoints
# ----------------------

@app.get("/")
def read_root():
    return {"message": "Chat API is running 🚀"}

@app.post("/register")
def register(username: str, password: str, email: str = None, db: Session = Depends(get_db)):
    hashed_password = pwd_context.hash(password)
    user = User(username=username, password=hashed_password, email=email)
    db.add(user)
    db.commit()
    db.refresh(user)
    return {"msg": "User created", "user_id": user.id}

@app.post("/login")
def login(username: str, password: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.username == username).first()
    if not user or not pwd_context.verify(password, user.password):
        return {"error": "Invalid credentials"}
    return {"msg": "Login successful", "user_id": user.id}

@app.get("/messages")
def get_messages(db: Session = Depends(get_db)):
    messages = db.query(Message).all()
    return [{"user": m.user.username, "content": m.content, "created_at": m.created_at} for m in messages]

# ----------------------
# WebSocket Management with Redis Pub/Sub
# ----------------------

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        # PubSub instance phải được tạo trong môi trường bất đồng bộ
        self.pubsub = r.pubsub()
        self.pubsub.subscribe(REDIS_CHANNEL)
        asyncio.create_task(self.listener())

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    # Lắng nghe các tin nhắn từ kênh Redis và phát sóng chúng đến các client
    async def listener(self):
        while True:
            # Sử dụng asyncio.to_thread để chạy blocking I/O trong thread pool
            message = await asyncio.to_thread(self.pubsub.get_message, ignore_subscribe_messages=True)
            if message:
                data = json.loads(message['data'])
                # Phát sóng tin nhắn đến tất cả các client
                for connection in self.active_connections:
                    try:
                        await connection.send_text(json.dumps(data))
                    except WebSocketDisconnect:
                        self.active_connections.remove(connection)
                    except Exception as e:
                        print(f"Error broadcasting message: {e}")
                        self.active_connections.remove(connection)
            # Tránh lặp vô hạn và giải phóng tài nguyên CPU
            await asyncio.sleep(0.01)

manager = ConnectionManager()

@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str, db: Session = Depends(get_db)):
    await manager.connect(websocket)
    try:
        # Kiểm tra nếu người dùng chưa tồn tại, hãy tạo một người dùng mới
        user = db.query(User).filter(User.username == username).first()
        if not user:
            print(f"Người dùng '{username}' không tồn tại. Đang tạo người dùng mới.")
            # Sử dụng một mật khẩu mặc định vì chúng ta không cần đăng nhập
            user = User(username=username, password="anonymous_user")
            db.add(user)
            db.commit()
            db.refresh(user)
            
        while True:
            data = await websocket.receive_text()
            new_msg = Message(user_id=user.id, content=data)
            db.add(new_msg)
            db.commit()
            db.refresh(new_msg)
            
            # Sử dụng asyncio.to_thread để chạy blocking I/O trong thread pool
            await asyncio.to_thread(r.publish, REDIS_CHANNEL, json.dumps({
                "username": username,
                "message": data
            }))
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        # Thông báo rằng người dùng đã rời khỏi
        await asyncio.to_thread(r.publish, REDIS_CHANNEL, json.dumps({
            "username": "System",
            "message": f"{username} left the chat"
        }))
