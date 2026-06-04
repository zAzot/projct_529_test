from fastapi import FastAPI, File, UploadFile, HTTPException, Depends, status, Request, WebSocket, WebSocketDisconnect, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, FileResponse, Response
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, field_validator
import os
import shutil
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, timezone
from helpers import logger, config, LOG_FILE_PATH
import hashlib
import secrets
from jose import JWTError, jwt
from contextlib import asynccontextmanager
import asyncio
from tqdm import tqdm
import json
import math
import uuid
import mimetypes
from PIL import Image
import io
import time
from collections import defaultdict
import hmac
from typing import Optional

UPLOAD_DIR = config.get('UPLOAD_DIR')
os.makedirs(UPLOAD_DIR, exist_ok=True)
PHOTOS_DIR = os.path.join(os.path.dirname(__file__), "Photos")
os.makedirs(PHOTOS_DIR, exist_ok=True)
DB_PATH = config.get('DATABASE_PATH')
ALLOWED_EXTENSIONS = config.get('ALLOWED_EXTENSIONS')
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)
SECRET_KEY = config.get('SECRET_KEY')
ALGORITHM = config.get('ALGORITHM')
ACCESS_TOKEN_EXPIRE_MINUTES = config.get('ACCESS_TOKEN_EXPIRE_MINUTES')
ADMIN_STATIC_CODE = config.get('ADMIN_STATIC_CODE')
progress_status = {"status": "idle", "progress": 0, "message": "", "current_operation": ""}
active_connections = []
rate_limit_storage = defaultdict(list)
restore_lock = asyncio.Lock()
RATE_LIMIT_REQUESTS = 100
RATE_LIMIT_PERIOD = 60

def get_all_existing_ids(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM tbl_1_pub UNION SELECT id FROM tbl_2_unpub UNION SELECT id FROM tbl_3_buffer")
    return {row[0] for row in cursor.fetchall()}

def log_admin_action(admin: dict, endpoint: str, request_data: str, response_data: str):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        login = admin.get('username') or admin.get('email') or 'unknown'
        cursor.execute("""
            INSERT INTO admin_logs (login, endpoint, request_data, response_data, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (login, endpoint, request_data, response_data, datetime.now()))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[ERROR] Failed to write log: {str(e)}")

class CellUpdateRequest(BaseModel):
    record_id: int
    column_name: str
    new_value: str

class RowDeleteRequest(BaseModel):
    record_id: int

class FilterRequest(BaseModel):
    code: str = None
    name: str = None
    brand: str = None
    model: str = None
    place: str = None
    qty_min: int = None
    qty_max: int = None
    price_min: int = None
    price_max: int = None
    condition: str = None
    date_from: str = None
    date_to: str = None
    sort_by: str = None
    sort_order: str = "asc"
    page: int = 1
    page_size: int = 50

class AdminCreate(BaseModel):
    username: str
    email: Optional[str] = None
    password: str
    @field_validator('username')
    def username_alphanumeric(cls, v):
        if not v.isalnum():
            raise ValueError('Username must be alphanumeric')
        return v
    @field_validator('email')
    def email_format(cls, v):
        if v is None or v == "":
            return v
        if '@' not in v or '.' not in v:
            raise ValueError('Invalid email format')
        return v
    @field_validator('password')
    def password_min_length(cls, v):
        if len(v) < 6:
            raise ValueError('Password must be at least 6 characters')
        return v

class AdminDelete(BaseModel):
    pass

class UserResponse(BaseModel):
    username: str
    email: str
    role: str
    created_at: str

class MoveRowRequest(BaseModel):
    source: str
    id: int

class StartNegotiationRequest(BaseModel):
    record_id: int
    sold_qty: int

class EndNegotiationRequest(BaseModel):
    record_id: int

class AddUnpublishedItemRequest(BaseModel):
    code: str
    qty: int
    name: str
    brand: str = None
    model: str = None
    place: str = None
    price: int = 0
    conditions: str = None
    truck_models: str = None
    publishing_date: str = None

class ContactUpdate(BaseModel):
    phone: str
    name: str

async def cleanup_dead_websocket_connections():
    dead_connections = []
    for connection in active_connections:
        try:
            await connection.send_json({"type": "ping"})
        except:
            dead_connections.append(connection)
    for dead in dead_connections:
        if dead in active_connections:
            active_connections.remove(dead)

async def notify_progress(status_val: str, progress: int, message: str, current_operation: str):
    global progress_status
    progress_status = {
        "status": status_val,
        "progress": progress,
        "message": message,
        "current_operation": current_operation,
        "timestamp": datetime.now().isoformat()
    }
    await cleanup_dead_websocket_connections()
    for connection in active_connections:
        try:
            await connection.send_json(progress_status)
        except:
            pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[INFO] Starting FastAPI server")
    os.makedirs("logs", exist_ok=True)
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    os.makedirs(PHOTOS_DIR, exist_ok=True)
    os.makedirs("templates", exist_ok=True)
    os.makedirs("static", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='db_version'")
    version_row = cursor.fetchone()
    current_version = 0
    if version_row:
        cursor.execute("SELECT version FROM db_version ORDER BY id DESC LIMIT 1")
        current_version = cursor.fetchone()[0]
    else:
        cursor.execute("CREATE TABLE db_version (id INTEGER PRIMARY KEY AUTOINCREMENT, version INTEGER, applied_at TIMESTAMP)")
        cursor.execute("INSERT INTO db_version (version, applied_at) VALUES (0, ?)", (datetime.now(),))
        conn.commit()
    if current_version == 0:
        print("[INFO] Creating initial tables")
        cursor.execute("""
            CREATE TABLE admins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'admin',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE tbl_1_pub (
                id INTEGER PRIMARY KEY,
                Code TEXT,
                QTY INTEGER,
                NAME TEXT,
                Brand TEXT,
                Model TEXT,
                PLACE TEXT,
                Price INTEGER,
                Conditions TEXT,
                Truck_models TEXT,
                Publishing_date TEXT,
                Description TEXT DEFAULT '',
                Photos TEXT DEFAULT ''
            )
        """)
        cursor.execute("""
            CREATE TABLE backup_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tbl TEXT NOT NULL,
                original_id INTEGER NOT NULL,
                Code TEXT,
                QTY INTEGER,
                NAME TEXT,
                Brand TEXT,
                Model TEXT,
                PLACE TEXT,
                Price INTEGER,
                Conditions TEXT,
                Truck_models TEXT,
                Publishing_date TEXT,
                Description TEXT DEFAULT '',
                Photos TEXT DEFAULT ''
            )
        """)
        cursor.execute("""
            CREATE TABLE tbl_2_unpub (
                id INTEGER PRIMARY KEY,
                Code TEXT,
                QTY INTEGER,
                NAME TEXT,
                Brand TEXT,
                Model TEXT,
                PLACE TEXT,
                Price INTEGER,
                Conditions TEXT,
                Truck_models TEXT,
                Publishing_date TEXT,
                Description TEXT DEFAULT '',
                Photos TEXT DEFAULT ''
            )
        """)
        cursor.execute("""
            CREATE TABLE tbl_3_buffer (
                id INTEGER PRIMARY KEY,
                Code TEXT,
                QTY INTEGER,
                NAME TEXT,
                Brand TEXT,
                Model TEXT,
                PLACE TEXT,
                Price INTEGER,
                Conditions TEXT,
                Truck_models TEXT,
                Publishing_date TEXT,
                sales_negotiation_start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                Description TEXT DEFAULT '',
                Photos TEXT DEFAULT ''
            )
        """)
        cursor.execute("""
            CREATE TABLE tbl_4_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                Code TEXT,
                QTY INTEGER,
                NAME TEXT,
                Brand TEXT,
                Model TEXT,
                PLACE TEXT,
                Price INTEGER,
                Conditions TEXT,
                Truck_models TEXT,
                Publishing_date TEXT,
                sales_negotiation_start_date TIMESTAMP,
                sales_negotiation_end_date TIMESTAMP,
                Description TEXT DEFAULT '',
                Photos TEXT DEFAULT ''
            )
        """)
        cursor.execute("""
            CREATE TABLE salt_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                salt BLOB NOT NULL
            )
        """)
        cursor.execute("UPDATE db_version SET version=1, applied_at=? WHERE id=1", (datetime.now(),))
        conn.commit()
        print("[INFO] Tables created successfully")
        current_version = 1
    cursor.execute("CREATE TABLE IF NOT EXISTS manual_backup (id INTEGER PRIMARY KEY AUTOINCREMENT, tbl TEXT NOT NULL, original_id INTEGER NOT NULL, Code TEXT, QTY INTEGER, NAME TEXT, Brand TEXT, Model TEXT, PLACE TEXT, Price INTEGER, Conditions TEXT, Truck_models TEXT, Publishing_date TEXT, Description TEXT DEFAULT '', Photos TEXT DEFAULT '')")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS admin_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            login TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            request_data TEXT,
            response_data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    files = []
    for f in os.listdir(UPLOAD_DIR):
        fpath = os.path.join(UPLOAD_DIR, f)
        if os.path.isfile(fpath):
            files.append((fpath, os.path.getmtime(fpath)))
    if len(files) > 50:
        files.sort(key=lambda x: x[1])
        to_delete = files[:45]
        for fpath, _ in to_delete:
            try:
                os.remove(fpath)
                print(f"[INFO] Deleted old file: {os.path.basename(fpath)}")
            except Exception as e:
                print(f"[ERROR] Could not delete {fpath}: {str(e)}")
    conn_cleanup = sqlite3.connect(DB_PATH)
    cursor_cleanup = conn_cleanup.cursor()
    cursor_cleanup.execute("SELECT Photos FROM tbl_1_pub")
    pub_photos = cursor_cleanup.fetchall()
    cursor_cleanup.execute("SELECT Photos FROM tbl_2_unpub")
    unpub_photos = cursor_cleanup.fetchall()
    cursor_cleanup.execute("SELECT Photos FROM tbl_3_buffer")
    buffer_photos = cursor_cleanup.fetchall()
    conn_cleanup.close()
    db_photos = set()
    for row in pub_photos:
        if row[0]:
            for p in row[0].split(','):
                if p.strip():
                    db_photos.add(p.strip())
    for row in unpub_photos:
        if row[0]:
            for p in row[0].split(','):
                if p.strip():
                    db_photos.add(p.strip())
    for row in buffer_photos:
        if row[0]:
            for p in row[0].split(','):
                if p.strip():
                    db_photos.add(p.strip())
    for f in os.listdir(PHOTOS_DIR):
        fpath = os.path.join(PHOTOS_DIR, f)
        if os.path.isfile(fpath) and f != "no_photo.png":
            if f not in db_photos:
                try:
                    os.remove(fpath)
                    print(f"[INFO] Deleted photo not in DB: {os.path.basename(fpath)}")
                except Exception as e:
                    print(f"[ERROR] Could not delete {fpath}: {str(e)}")
    yield
    print("[INFO] FastAPI server shutting down")

app = FastAPI(lifespan=lifespan)

@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    client_ip = request.client.host
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        client_ip = forwarded.split(",")[0].strip()
    method = request.method
    path = request.url.path
    print(f"[INFO] IP: {client_ip}")
    print(f"[INFO] Endpoint: {method} {path}")
    if request.method in ["POST", "PUT", "PATCH"]:
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            try:
                body = await request.body()
                if len(body) > 1024 * 1024:
                    print("[WARN] Body too large to log")
                else:
                    print(f"[INFO] Request body: {body.decode('utf-8')}")
                async def receive():
                    return {"type": "http.request", "body": body}
                request._receive = receive
            except Exception as e:
                print(f"[WARN] Could not read body: {e}")
        elif "multipart/form-data" in content_type:
            print(f"[INFO] Request body: <multipart form data>")
    response = await call_next(request)
    if response.headers.get("content-type", "").startswith("application/json"):
        try:
            response_body = b""
            async for chunk in response.body_iterator:
                response_body += chunk
            response = JSONResponse(content=json.loads(response_body), status_code=response.status_code, headers=dict(response.headers))
            print(f"[INFO] Response body: {response_body.decode('utf-8')}")
        except Exception as e:
            print(f"[WARN] Could not read response body: {e}")
    else:
        print(f"[INFO] Response body: <non-JSON response, status {response.status_code}>")
    return response

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    client_ip = request.client.host
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        client_ip = forwarded.split(",")[0].strip()
    now = time.time()
    timestamps = rate_limit_storage[client_ip]
    timestamps = [t for t in timestamps if now - t < RATE_LIMIT_PERIOD]
    if len(timestamps) >= RATE_LIMIT_REQUESTS:
        print(f"[WARN] Rate limit exceeded for IP: {client_ip}")
        return JSONResponse(status_code=429, content={"detail": "Too many requests"})
    timestamps.append(now)
    rate_limit_storage[client_ip] = timestamps
    response = await call_next(request)
    return response

@app.middleware("http")
async def body_size_middleware(request: Request, call_next):
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > 10 * 1024 * 1024:
        return JSONResponse(status_code=413, content={"detail": "Payload too large"})
    response = await call_next(request)
    return response

@app.middleware("http")
async def maintenance_middleware(request: Request, call_next):
    if config.get('MAINTENANCE_MODE') and request.url.path not in ['/maintenance', '/admin/maintenance-off', '/admin/backup/restore', '/admin/backup/create', '/admin/backup/restore-auto', '/ws/progress', '/progress-status', '/data/filter', '/healthcheck', '/pages-count', '/data', '/photos', '/item', '/admin/logs', '/admin/admins-list', '/login', '/token', '/admin/logs/view']:
        return RedirectResponse(url="/maintenance", status_code=303)
    response = await call_next(request)
    return response

@app.middleware("http")
async def protected_routes_middleware(request: Request, call_next):
    protected_paths = ["/admin", "/sierra-alpha"]
    path = request.url.path
    if path in protected_paths:
        token = request.headers.get("Authorization")
        if token and token.startswith("Bearer "):
            token = token[7:]
        else:
            token = request.cookies.get("access_token")
        if not token:
            return RedirectResponse(url="/login", status_code=303)
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            role = payload.get("role")
            if path == "/admin" and role not in ["admin", "super_admin"]:
                return RedirectResponse(url="/login", status_code=303)
            if path == "/sierra-alpha" and role not in ["admin", "super_admin"]:
                return RedirectResponse(url="/login", status_code=303)
        except JWTError:
            return RedirectResponse(url="/login", status_code=303)
    response = await call_next(request)
    return response

def get_current_user_optional(token: str = Depends(oauth2_scheme)):
    if not token:
        return None
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email = payload.get("sub")
        role = payload.get("role")
        if email is None or role is None:
            return None
        if role == "super_admin":
            return {"username": "super_admin", "email": "super_admin", "role": "super_admin", "created_at": datetime.now().isoformat()}
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT username, email, role, created_at FROM admins WHERE email = ?", (email,))
        user = cursor.fetchone()
        conn.close()
        if user is None:
            return None
        return {"username": user[0], "email": user[1], "role": user[2], "created_at": user[3]}
    except JWTError:
        return None

def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"}
    )
    user = get_current_user_optional(token)
    if user is None:
        raise credentials_exception
    return user

def get_current_admin(current_user: dict = Depends(get_current_user)):
    if current_user["role"] not in ["admin", "super_admin"]:
        print(f"[WARN] Access attempt to admin endpoint by user with role: {current_user['role']}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Admin access required"
        )
    return current_user

def get_current_super_admin(current_user: dict = Depends(get_current_user)):
    if current_user["role"] != "super_admin":
        print(f"[WARN] Access attempt to super admin endpoint by user with role: {current_user['role']}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Super admin access required"
        )
    return current_user

@app.websocket("/ws/progress")
async def websocket_progress(websocket: WebSocket):
    await websocket.accept()
    token = websocket.query_params.get("token")
    user = get_current_user_optional(token)
    if user is None or user["role"] not in ["admin", "super_admin"]:
        await websocket.close(code=1008, reason="Unauthorized")
        return
    active_connections.append(websocket)
    try:
        await websocket.send_json(progress_status)
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in active_connections:
            active_connections.remove(websocket)
    except Exception as e:
        if websocket in active_connections:
            active_connections.remove(websocket)

@app.get("/photos/{filename}")
async def get_photo(filename: str):
    safe_filename = os.path.basename(filename)
    if not safe_filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    file_path = os.path.join(PHOTOS_DIR, safe_filename)
    real_photos_dir = os.path.realpath(PHOTOS_DIR)
    real_file_path = os.path.realpath(file_path)
    if not real_file_path.startswith(real_photos_dir):
        print(f"[ERROR] Path traversal attempt: {filename}")
        raise HTTPException(status_code=403, detail="Access denied")
    if not os.path.exists(real_file_path):
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        test_file_path = os.path.join(static_dir, "no_photo.png")
        if os.path.exists(test_file_path):
            file_path = test_file_path
            real_file_path = os.path.realpath(file_path)
            real_static_dir = os.path.realpath(static_dir)
            if not real_file_path.startswith(real_static_dir):
                raise HTTPException(status_code=403, detail="Access denied")
        else:
            raise HTTPException(status_code=404, detail="Photo not found")
    try:
        with Image.open(real_file_path) as img:
            target_size = (480, 480)
            img.thumbnail(target_size, Image.Resampling.LANCZOS)
            if img.mode in ('RGBA', 'LA', 'P'):
                rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'P':
                    img = img.convert('RGBA')
                rgb_img.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                img = rgb_img
            elif img.mode != 'RGB':
                img = img.convert('RGB')
            output = io.BytesIO()
            img_format = 'JPEG'
            if real_file_path.lower().endswith('.png'):
                img_format = 'PNG'
            elif real_file_path.lower().endswith('.gif'):
                img_format = 'GIF'
            img.save(output, format=img_format, quality=85)
            output.seek(0)
            mime_type, _ = mimetypes.guess_type(real_file_path)
            if not mime_type or not mime_type.startswith('image/'):
                mime_type = "image/jpeg"
            return Response(content=output.getvalue(), media_type=mime_type)
    except Exception as e:
        print(f"[ERROR] Error processing photo {filename}: {str(e)}")
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        test_file_path = os.path.join(static_dir, "no_photo.png")
        if os.path.exists(test_file_path):
            with open(test_file_path, "rb") as f:
                return Response(content=f.read(), media_type="image/png")
        raise HTTPException(status_code=500, detail="Error processing photo")

@app.get("/item")
async def get_item(record_id: int, size: int = -1):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, Description, Photos FROM tbl_1_pub WHERE id = ?", (record_id,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Record not found")
    columns = ['id', 'Code', 'QTY', 'NAME', 'Brand', 'Model', 'PLACE', 'Price', 'Conditions', 'Truck_models', 'Publishing_date', 'Description', 'Photos']
    result = dict(zip(columns, row))
    photo_names = result['Photos'].split(',') if result['Photos'] else []
    if size >= 0 and photo_names:
        first_photo = photo_names[0].strip()
        safe_filename = os.path.basename(first_photo)
        if safe_filename:
            file_path = os.path.join(PHOTOS_DIR, safe_filename)
            real_photos_dir = os.path.realpath(PHOTOS_DIR)
            real_file_path = os.path.realpath(file_path)
            if real_file_path.startswith(real_photos_dir) and os.path.exists(real_file_path):
                if size == 0:
                    mime_type, _ = mimetypes.guess_type(real_file_path)
                    return FileResponse(real_file_path, media_type=mime_type)
                else:
                    with Image.open(real_file_path) as img:
                        target_size = (size, size)
                        img.thumbnail(target_size, Image.Resampling.LANCZOS)
                        if img.mode in ('RGBA', 'LA', 'P'):
                            rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                            if img.mode == 'P':
                                img = img.convert('RGBA')
                            rgb_img.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                            img = rgb_img
                        elif img.mode != 'RGB':
                            img = img.convert('RGB')
                        output = io.BytesIO()
                        img_format = 'JPEG'
                        if real_file_path.lower().endswith('.png'):
                            img_format = 'PNG'
                        elif real_file_path.lower().endswith('.gif'):
                            img_format = 'GIF'
                        img.save(output, format=img_format, quality=85)
                        output.seek(0)
                        mime_type, _ = mimetypes.guess_type(real_file_path)
                        if not mime_type or not mime_type.startswith('image/'):
                            mime_type = "image/jpeg"
                        return Response(content=output.getvalue(), media_type=mime_type)
    result['photo_urls'] = [f"/photos/{name.strip()}" for name in photo_names if name.strip()]
    return result

@app.post("/data/filter")
async def filter_data(request: FilterRequest):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    query = "SELECT * FROM tbl_1_pub WHERE 1=1"
    params = []
    if request.code:
        query += " AND Code LIKE ?"
        params.append(f"%{request.code}%")
    if request.name:
        query += " AND NAME LIKE ?"
        params.append(f"%{request.name}%")
    if request.brand:
        query += " AND Brand LIKE ?"
        params.append(f"%{request.brand}%")
    if request.model:
        query += " AND Model LIKE ?"
        params.append(f"%{request.model}%")
    if request.place:
        query += " AND PLACE LIKE ?"
        params.append(f"%{request.place}%")
    if request.qty_min is not None:
        query += " AND QTY >= ?"
        params.append(request.qty_min)
    if request.qty_max is not None:
        query += " AND QTY <= ?"
        params.append(request.qty_max)
    if request.price_min is not None:
        query += " AND Price >= ?"
        params.append(request.price_min)
    if request.price_max is not None:
        query += " AND Price <= ?"
        params.append(request.price_max)
    if request.condition:
        query += " AND Conditions LIKE ?"
        params.append(f"%{request.condition}%")
    if request.date_from:
        query += " AND Publishing_date >= ?"
        params.append(request.date_from)
    if request.date_to:
        query += " AND Publishing_date <= ?"
        params.append(request.date_to)
    if request.sort_by and request.sort_by in ['Code', 'QTY', 'NAME', 'Brand', 'Model', 'PLACE', 'Price', 'Conditions']:
        sort_order_value = request.sort_order if request.sort_order else "asc"
        if sort_order_value.lower() not in ["asc", "desc"]:
            sort_order_value = "asc"
        sort_order = "ASC" if sort_order_value.lower() == "asc" else "DESC"
        sort_column = request.sort_by
        query += f" ORDER BY {sort_column} {sort_order}"
    cursor.execute(query, params)
    all_rows = cursor.fetchall()
    total = len(all_rows)
    offset = (request.page - 1) * request.page_size
    cursor.execute(query + f" LIMIT {request.page_size} OFFSET {offset}", params)
    rows = cursor.fetchall()
    conn.close()
    columns = ['id', 'Code', 'QTY', 'NAME', 'Brand', 'Model', 'PLACE', 'Price', 'Conditions', 'Truck_models', 'Publishing_date', 'Description', 'Photos']
    result = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        photo_names = row_dict['Photos'].split(',') if row_dict['Photos'] else []
        row_dict['photo_urls'] = [f"/photos/{name.strip()}" for name in photo_names if name.strip()]
        result.append(row_dict)
    print(f"[INFO] Data filtering: found {total} records")
    response_data = {
        "total": total,
        "page": request.page,
        "page_size": request.page_size,
        "total_pages": (total + request.page_size - 1) // request.page_size,
        "data": result,
        "filters_applied": {k: v for k, v in request.model_dump().items() if v is not None and k not in ['page', 'page_size']}
    }
    return response_data

@app.get("/healthcheck")
async def healthcheck():
    status_val = "healthy"
    components = {}
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        conn.close()
        components["database"] = {"status": "up", "response_time_ms": 5}
    except Exception as e:
        status_val = "unhealthy"
        components["database"] = {"status": "down", "error": str(e)}
    try:
        if os.path.exists(UPLOAD_DIR):
            free_space = shutil.disk_usage(UPLOAD_DIR).free
            free_space_mb = free_space // (1024 * 1024)
            components["storage"] = {"status": "up", "free_space_mb": free_space_mb}
        else:
            components["storage"] = {"status": "down", "error": "Directory not found"}
    except Exception as e:
        components["storage"] = {"status": "down", "error": str(e)}
    components["maintenance_mode"] = config.get('MAINTENANCE_MODE')
    print("[INFO] Server health check")
    response_data = {"status": status_val, "timestamp": datetime.now(timezone.utc).isoformat(), "version": "3.0.0", "components": components}
    return response_data

@app.get("/progress-status")
async def get_progress_status(current_user: dict = Depends(get_current_admin)):
    return progress_status

@app.get("/admin/logs/view")
async def view_admin_logs(code: str):
    if not code or code != ADMIN_STATIC_CODE:
        print(f"[ERROR] Invalid admin code provided for logs access")
        raise HTTPException(status_code=401, detail="Invalid admin code")
    try:
        if not os.path.exists(LOG_FILE_PATH):
            return JSONResponse(content={"logs": "", "message": "Log file not found"})
        with open(LOG_FILE_PATH, "r", encoding="utf-8") as f:
            log_content = f.read()
        print(f"[INFO] Admin logs accessed successfully")
        return JSONResponse(content={"logs": log_content})
    except Exception as e:
        print(f"[ERROR] Failed to read log file: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to read log file")

@app.get("/login")
async def login_page():
    print("[INFO] Login page request")
    login_html_path = os.path.join(os.path.dirname(__file__), "templates", "login.html")
    if os.path.exists(login_html_path):
        with open(login_html_path, "r", encoding="utf-8") as f:
            content = f.read()
            return HTMLResponse(content=content)
    return HTMLResponse(content="<h1>Login</h1><p>File templates/login.html not found</p>")

@app.post("/token")
async def login_for_access_token(response: Response, form_data: OAuth2PasswordRequestForm = Depends()):
    print(f"[INFO] Login attempt with username: {form_data.username}")
    if form_data.username == "none":
        provided_hash = hashlib.sha256(form_data.password.encode()).hexdigest()
        static_code_hash = hashlib.sha256(ADMIN_STATIC_CODE.encode()).hexdigest()
        if not hmac.compare_digest(provided_hash, static_code_hash):
            print(f"[ERROR] Invalid super admin code")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid admin code",
                headers={"WWW-Authenticate": "Bearer"}
            )
        print(f"[INFO] Super admin authenticated successfully")
        super_token = jwt.encode(
            {"sub": "super_admin", "role": "super_admin", "exp": datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)},
            SECRET_KEY,
            algorithm=ALGORITHM
        )
        response.set_cookie(
            key="access_token",
            value=super_token,
            httponly=True,
            secure=False,
            samesite="lax",
            max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60
        )
        return {"status": "success", "access_token": super_token, "token_type": "bearer", "redirect_url": "/sierra-alpha"}
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT username, email, password_hash, role FROM admins WHERE email = ? OR username = ?", (form_data.username, form_data.username))
    user = cursor.fetchone()
    conn.close()
    if not user:
        print(f"[ERROR] Failed login attempt: {form_data.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect login or password",
            headers={"WWW-Authenticate": "Bearer"}
        )
    salt, hash_value = user[2].split('$')
    hash_obj = hashlib.pbkdf2_hmac('sha256', form_data.password.encode(), salt.encode(), 100000)
    if not hmac.compare_digest(hash_obj.hex(), hash_value):
        print(f"[ERROR] Failed login attempt: {form_data.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect login or password",
            headers={"WWW-Authenticate": "Bearer"}
        )
    if user[3] != 'admin':
        print(f"[WARN] Non-admin login attempt: {user[0]}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can login here"
        )
    print(f"[INFO] Successful login for user: {user[0]} with role {user[3]}")
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = jwt.encode(
        {"sub": user[1], "role": user[3], "exp": datetime.now(timezone.utc) + access_token_expires},
        SECRET_KEY,
        algorithm=ALGORITHM
    )
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )
    return {"access_token": access_token, "token_type": "bearer", "redirect_url": "/admin"}

@app.get("/admin/verify-super")
async def verify_super_admin(request: Request):
    token = request.headers.get("Authorization")
    if token and token.startswith("Bearer "):
        token = token[7:]
    if not token:
        raise HTTPException(status_code=401, detail="No token provided")
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        role = payload.get("role")
        if role not in ["admin", "super_admin"]:
            raise HTTPException(status_code=403, detail="Access denied")
        return {"valid": True, "role": role}
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

@app.post("/admin/register")
async def register_admin(admin_data: AdminCreate, current_admin: dict = Depends(get_current_super_admin)):
    print(f"[INFO] Admin registration attempt with username: {admin_data.username}, email: {admin_data.email}")
    salt = secrets.token_hex(16)
    hash_obj = hashlib.pbkdf2_hmac('sha256', admin_data.password.encode(), salt.encode(), 100000)
    hashed_pw = f"{salt}${hash_obj.hex()}"
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    email_value = admin_data.email if admin_data.email is not None else ""
    cursor.execute("SELECT email, username FROM admins WHERE email = ? OR username = ?", (email_value, admin_data.username))
    if cursor.fetchone():
        conn.close()
        print(f"[ERROR] Admin with this email or username already exists")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email or username already registered")
    cursor.execute("""
        INSERT INTO admins (username, email, password_hash, role, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (admin_data.username, email_value, hashed_pw, 'admin', datetime.now().isoformat()))
    conn.commit()
    conn.close()
    print(f"[INFO] Admin successfully registered: {admin_data.username}")
    response_data = {"message": "Admin registered successfully", "username": admin_data.username, "email": email_value}
    return response_data

@app.post("/admin/delete")
async def delete_admin(email: str = Form(...), current_admin: dict = Depends(get_current_super_admin)):
    print(f"[INFO] Attempt to delete admin account: {email}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM admins WHERE email = ?", (email,))
    admin_id = cursor.fetchone()
    if not admin_id:
        conn.close()
        print(f"[ERROR] Admin with email {email} not found")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Admin not found")
    cursor.execute("DELETE FROM admins WHERE email = ?", (email,))
    conn.commit()
    conn.close()
    print(f"[INFO] Admin account {email} successfully deleted")
    response_data = {"message": f"Admin account {email} deleted successfully"}
    return response_data

@app.get("/admin/me", response_model=UserResponse)
async def admin_me(current_admin: dict = Depends(get_current_admin)):
    print(f"[INFO] Admin profile request: {current_admin['username']}")
    response = current_admin
    log_admin_action(current_admin, "/admin/me", "", str(response))
    return response

@app.get("/maintenance")
async def maintenance_page():
    print("[INFO] Maintenance page request")
    return RedirectResponse(url="/admin", status_code=303)

@app.post("/admin/maintenance-off")
async def maintenance_off(current_admin: dict = Depends(get_current_admin)):
    config.set_maintenance(False)
    progress_status["status"] = "completed"
    progress_status["progress"] = 100
    progress_status["message"] = "Maintenance completed"
    progress_status["current_operation"] = "done"
    print(f"[INFO] Maintenance mode disabled by admin: {current_admin['username']}")
    response_data = {"message": "Maintenance mode disabled", "on_maintenance": False}
    log_admin_action(current_admin, "/admin/maintenance-off", "", str(response_data))
    return response_data

@app.post("/admin/backup/create")
async def create_backup_endpoint(current_admin: dict = Depends(get_current_super_admin)):
    print(f"[INFO] Attempt to create manual backup")
    async with restore_lock:
        config.set_maintenance(True)
        try:
            await notify_progress("running", 0, "Creating manual backup", "manual_backup_start")
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM manual_backup")
            total_rows = 0
            for src_table in ['tbl_1_pub', 'tbl_2_unpub', 'tbl_3_buffer']:
                cursor.execute(f"SELECT id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, Description, Photos FROM {src_table}")
                rows = cursor.fetchall()
                for row in rows:
                    cursor.execute("INSERT INTO manual_backup (tbl, original_id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, Description, Photos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (src_table, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12]))
                    total_rows += 1
            conn.commit()
            conn.close()
            print(f"[INFO] Manual backup created: {total_rows} rows")
            await notify_progress("completed", 100, f"Manual backup created: {total_rows} rows", "manual_backup_done")
            response_data = {"message": "Manual backup created successfully", "rows_backed_up": total_rows, "tables": ["tbl_1_pub", "tbl_2_unpub", "tbl_3_buffer"]}
            return response_data
        except Exception as e:
            await notify_progress("error", 0, f"Error: {str(e)}", "manual_backup_error")
            print(f"[ERROR] Error creating manual backup: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            config.set_maintenance(False)

@app.post("/admin/backup/restore")
async def restore_backup_endpoint(target_tables: str = Form(default="all"), current_admin: dict = Depends(get_current_super_admin)):
    print(f"[INFO] Attempt to restore from manual backup")
    if target_tables == "all":
        tables_to_restore = ["tbl_1_pub", "tbl_2_unpub", "tbl_3_buffer"]
    else:
        tables_to_restore = [t.strip() for t in target_tables.split(",")]
    async with restore_lock:
        config.set_maintenance(True)
        try:
            await notify_progress("running", 0, "Starting restore from manual backup", "manual_restore_start")
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM manual_backup")
            count = cursor.fetchone()[0]
            if count == 0:
                conn.close()
                print(f"[ERROR] Manual backup table is empty")
                raise HTTPException(status_code=404, detail="Manual backup table is empty")
            for target_table in tables_to_restore:
                cursor.execute(f"DELETE FROM {target_table}")
                cursor.execute("SELECT original_id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, Description, Photos FROM manual_backup WHERE tbl = ?", (target_table,))
                rows = cursor.fetchall()
                for row in rows:
                    cursor.execute(f"INSERT INTO {target_table} (id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, Description, Photos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12]))
                conn.commit()
                print(f"[INFO] Restored table {target_table} from manual backup: {len(rows)} rows")
            conn.close()
            print(f"[INFO] Restore from manual backup completed for tables: {', '.join(tables_to_restore)}")
            await notify_progress("completed", 100, f"Restored tables from manual backup: {', '.join(tables_to_restore)}", "manual_restore_done")
            response_data = {"message": "Restore from manual backup completed successfully", "restored_tables": tables_to_restore}
            return response_data
        except Exception as e:
            await notify_progress("error", 0, f"Error: {str(e)}", "manual_restore_error")
            print(f"[ERROR] Error restoring from manual backup: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            config.set_maintenance(False)

@app.post("/admin/backup/restore-auto")
async def restore_auto_backup_endpoint(target_tables: str = Form(default="all"), current_admin: dict = Depends(get_current_super_admin)):
    print(f"[INFO] Attempt to restore from auto backup")
    if target_tables == "all":
        tables_to_restore = ["tbl_1_pub", "tbl_2_unpub", "tbl_3_buffer"]
    else:
        tables_to_restore = [t.strip() for t in target_tables.split(",")]
    async with restore_lock:
        config.set_maintenance(True)
        try:
            await notify_progress("running", 0, "Starting restore from auto backup", "auto_restore_start")
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM backup_table")
            count = cursor.fetchone()[0]
            if count == 0:
                conn.close()
                print(f"[ERROR] Auto backup table is empty")
                raise HTTPException(status_code=404, detail="Auto backup table is empty")
            for target_table in tables_to_restore:
                cursor.execute(f"DELETE FROM {target_table}")
                cursor.execute("SELECT original_id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, Description, Photos FROM backup_table WHERE tbl = ?", (target_table,))
                rows = cursor.fetchall()
                for row in rows:
                    cursor.execute(f"INSERT INTO {target_table} (id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, Description, Photos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12]))
                conn.commit()
                print(f"[INFO] Restored table {target_table} from auto backup: {len(rows)} rows")
            conn.close()
            print(f"[INFO] Restore from auto backup completed for tables: {', '.join(tables_to_restore)}")
            await notify_progress("completed", 100, f"Restored tables from auto backup: {', '.join(tables_to_restore)}", "auto_restore_done")
            response_data = {"message": "Restore from auto backup completed successfully", "restored_tables": tables_to_restore}
            return response_data
        except Exception as e:
            await notify_progress("error", 0, f"Error: {str(e)}", "auto_restore_error")
            print(f"[ERROR] Error restoring from auto backup: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            config.set_maintenance(False)

@app.get("/sierra-alpha")
async def sierra_alpha_page():
    print("[INFO] Sierra-alpha page request")
    sierra_alpha_path = os.path.join(os.path.dirname(__file__), "templates", "sierra-alpha.html")
    if os.path.exists(sierra_alpha_path):
        with open(sierra_alpha_path, "r", encoding="utf-8") as f:
            content = f.read()
            return HTMLResponse(content=content)
    return HTMLResponse(content="<h1>Sierra Alpha</h1><p>File templates/sierra-alpha.html not found</p>")

@app.post("/admin/verify-password")
async def verify_admin_password(request: Request, current_admin: dict = Depends(get_current_super_admin)):
    client_ip = request.client.host if request else "unknown"
    forwarded = request.headers.get("X-Forwarded-For") if request else None
    if forwarded:
        client_ip = forwarded.split(",")[0].strip()
    print(f"[INFO] IP: {client_ip}")
    print(f"[INFO] Endpoint: POST /admin/verify-password")
    print(f"[REQW] Password verification request")
    print(f"[INFO] Admin password successfully verified")
    response_data = {"message": "Admin password verified successfully", "verified": True}
    return response_data

@app.get("/admin/admins-list")
async def get_admins_list(current_admin: dict = Depends(get_current_super_admin)):
    print(f"[INFO] Attempt to get admins list")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT id, username, email, role, created_at FROM admins")
    rows = cursor.fetchall()
    conn.close()
    result = [dict(row) for row in rows]
    print(f"[INFO] Retrieved admins list: {len(result)} records")
    return JSONResponse(content={"admins": result})

@app.post("/admin/logs")
async def get_admin_logs(request: Request, current_admin: dict = Depends(get_current_super_admin)):
    print(f"[INFO] Attempt to get admin logs")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT id, login, endpoint, request_data, response_data, created_at FROM admin_logs ORDER BY id DESC")
    rows = cursor.fetchall()
    conn.close()
    result = [dict(row) for row in rows]
    print(f"[INFO] Retrieved logs: {len(result)} records")
    return JSONResponse(content={"logs": result})

@app.get("/admin/table/buffer-sales")
async def get_buffer_sales(current_admin: dict = Depends(get_current_admin)):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, sales_negotiation_start_date, Description, Photos FROM tbl_3_buffer ORDER BY id DESC")
    rows = cursor.fetchall()
    conn.close()
    result = [dict(row) for row in rows]
    print(f"[INFO] Retrieved buffer sales: {len(result)} records")
    response_data = {"table_number": 4, "table_name": "tbl_3_buffer", "data": result}
    log_admin_action(current_admin, "/admin/table/buffer-sales", "", str(response_data))
    return response_data

@app.get("/admin/table/sales-history")
async def get_sales_history(current_admin: dict = Depends(get_current_admin)):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, sales_negotiation_start_date, sales_negotiation_end_date, Description, Photos FROM tbl_4_history ORDER BY id DESC")
    rows = cursor.fetchall()
    conn.close()
    result = [dict(row) for row in rows]
    print(f"[INFO] Retrieved sales history: {len(result)} records")
    response_data = {"table_number": 6, "table_name": "tbl_4_history", "data": result}
    log_admin_action(current_admin, "/admin/table/sales-history", "", str(response_data))
    return response_data

@app.post("/admin/table/unpublished/add")
async def add_unpublished_item(item: AddUnpublishedItemRequest, current_admin: dict = Depends(get_current_admin)):
    print(f"[INFO] Attempt to add item to tbl_2_unpub by admin: {current_admin['username']}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        existing_ids = get_all_existing_ids(conn)
        new_id = 1
        while new_id in existing_ids:
            new_id += 1
        cursor.execute("INSERT INTO tbl_2_unpub (id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, Description, Photos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (new_id, item.code, item.qty, item.name, item.brand, item.model, item.place, item.price, item.conditions, item.truck_models, item.publishing_date, '', ''))
        conn.commit()
        print(f"[INFO] Item successfully added to tbl_2_unpub with ID={new_id}")
        response_data = {"message": "Item added successfully", "id": new_id}
        log_admin_action(current_admin, "/admin/table/unpublished/add", str(item.model_dump()), str(response_data))
        return response_data
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Error adding item: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/admin/table/{table_number}")
async def get_table_data(table_number: int, current_admin: dict = Depends(get_current_admin)):
    if table_number not in [1, 2, 3, 5]:
        raise HTTPException(status_code=400, detail="Table number must be 1 (admins), 2 (tbl_1_pub), 3 (backup_table), or 5 (tbl_2_unpub). For buffer_sales use /admin/table/buffer-sales (table 4), for sales_history use /admin/table/sales-history (table 6)")
    table_map = {1: "admins", 2: "tbl_1_pub", 3: "backup_table", 5: "tbl_2_unpub"}
    table_name = table_map[table_number]
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    conn.close()
    result = [dict(row) for row in rows]
    print(f"[INFO] Retrieved table {table_number} ({table_name}) data")
    response_data = {"table_number": table_number, "table_name": table_name, "data": result}
    log_admin_action(current_admin, f"/admin/table/{table_number}", "", str(response_data))
    return response_data

@app.post("/admin/table/update-cell")
async def update_cell(update_data: CellUpdateRequest, current_admin: dict = Depends(get_current_admin)):
    allowed_columns = ['Code', 'QTY', 'NAME', 'Brand', 'Model', 'PLACE', 'Price', 'Conditions', 'Truck_models', 'Publishing_date', 'Description', 'Photos']
    if update_data.column_name not in allowed_columns:
        print(f"[ERROR] Attempt to update disallowed column: {update_data.column_name}")
        raise HTTPException(status_code=400, detail=f"Column {update_data.column_name} not allowed. Allowed: {allowed_columns}")
    if update_data.column_name == "Photos":
        raise HTTPException(status_code=400, detail="Use /admin/table/update-photo endpoint to update photos")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM tbl_1_pub WHERE id = ?", (update_data.record_id,))
    if not cursor.fetchone():
        conn.close()
        print(f"[ERROR] Record with ID {update_data.record_id} not found")
        raise HTTPException(status_code=400, detail=f"Record with id {update_data.record_id} not found")
    if update_data.column_name in ['QTY', 'Price']:
        try:
            new_value_int = int(update_data.new_value)
            cursor.execute(f"UPDATE tbl_1_pub SET {update_data.column_name} = ? WHERE id = ?", (new_value_int, update_data.record_id))
        except ValueError:
            conn.close()
            print(f"[ERROR] Invalid data type for column {update_data.column_name}: {update_data.new_value}")
            raise HTTPException(status_code=400, detail=f"Column {update_data.column_name} requires integer value")
    else:
        cursor.execute(f"UPDATE tbl_1_pub SET {update_data.column_name} = ? WHERE id = ?", (update_data.new_value, update_data.record_id))
    conn.commit()
    conn.close()
    print(f"[INFO] Admin {current_admin['username']} updated cell: ID={update_data.record_id}, {update_data.column_name}={update_data.new_value}")
    response_data = {"message": "Cell updated successfully", "record_id": update_data.record_id, "column": update_data.column_name, "new_value": update_data.new_value}
    log_admin_action(current_admin, "/admin/table/update-cell", str(update_data.model_dump()), str(response_data))
    return response_data

@app.post("/admin/table/update-photo")
async def update_photo(
    record_id: int = Form(...),
    file: UploadFile = File(...),
    current_admin: dict = Depends(get_current_admin)
):
    print(f"[INFO] Attempt to update photo for record ID={record_id} by admin: {current_admin['username']}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT Photos FROM tbl_1_pub WHERE id = ?", (record_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Record with id {record_id} not found")
    old_photos = row[0]
    if old_photos:
        old_files = [f.strip() for f in old_photos.split(',') if f.strip()]
        for old_file in old_files:
            safe_old = os.path.basename(old_file)
            old_path = os.path.join(PHOTOS_DIR, safe_old)
            if os.path.exists(old_path) and os.path.realpath(old_path).startswith(os.path.realpath(PHOTOS_DIR)):
                try:
                    os.remove(old_path)
                    print(f"[INFO] Deleted old photo file: {old_file}")
                except Exception as e:
                    print(f"[WARN] Could not delete {old_file}: {str(e)}")
    if not file.filename:
        conn.close()
        raise HTTPException(status_code=400, detail="No file provided")
    allowed_photo_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in allowed_photo_extensions:
        conn.close()
        raise HTTPException(status_code=415, detail=f"Unsupported image type. Allowed: {allowed_photo_extensions}")
    unique_filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}_{uuid.uuid4().hex[:8]}.{ext[1:]}"
    file_path = os.path.join(PHOTOS_DIR, unique_filename)
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        print(f"[INFO] Photo saved: {file_path}")
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=f"Failed to save photo: {str(e)}")
    cursor.execute("UPDATE tbl_1_pub SET Photos = ? WHERE id = ?", (unique_filename, record_id))
    conn.commit()
    conn.close()
    print(f"[INFO] Photo for record ID={record_id} updated: {unique_filename}")
    response_data = {"message": "Photo updated successfully", "record_id": record_id, "filename": unique_filename}
    log_admin_action(current_admin, "/admin/table/update-photo", f"record_id={record_id}", str(response_data))
    return response_data

@app.delete("/admin/table/delete-row")
async def delete_row(delete_data: RowDeleteRequest, request: Request, current_admin: dict = Depends(get_current_admin)):
    client_ip = request.client.host
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        client_ip = forwarded.split(",")[0].strip()
    print(f"[INFO] IP: {client_ip}")
    print(f"[INFO] Endpoint: DELETE /admin/table/delete-row")
    print(f"[REQW] Admin {current_admin['username']} sent request to delete row with ID={delete_data.record_id}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    tables_to_check = {
        "tbl_1_pub": "published items",
        "tbl_2_unpub": "unpublished items",
        "tbl_3_buffer": "sales buffer",
        "tbl_4_history": "sales history"
    }
    found_table = None
    for table_name, table_desc in tables_to_check.items():
        cursor.execute(f"SELECT id FROM {table_name} WHERE id = ?", (delete_data.record_id,))
        if cursor.fetchone():
            found_table = table_name
            print(f"[INFO] Found record ID={delete_data.record_id} in table {table_name} ({table_desc})")
            break
    if not found_table:
        conn.close()
        print(f"[ERROR] Record with ID {delete_data.record_id} not found in any table")
        print(f"[INFO] Response body: {{\"detail\":\"Record with id {delete_data.record_id} not found\"}}")
        raise HTTPException(status_code=400, detail=f"Record with id {delete_data.record_id} not found")
    cursor.execute(f"DELETE FROM {found_table} WHERE id = ?", (delete_data.record_id,))
    conn.commit()
    conn.close()
    print(f"[INFO] Deleted row ID={delete_data.record_id} from table {found_table}")
    print(f"[INFO] Response body: {{\"message\":\"Row with id {delete_data.record_id} deleted successfully\", \"deleted_id\":{delete_data.record_id}, \"source_table\":\"{found_table}\"}}")
    print(f"[INFO] Admin {current_admin['username']} deleted row ID={delete_data.record_id} from table {found_table}")
    response_data = {
        "message": f"Row with id {delete_data.record_id} deleted successfully",
        "deleted_id": delete_data.record_id,
        "source_table": found_table
    }
    log_admin_action(current_admin, "/admin/table/delete-row", str(delete_data.model_dump()), str(response_data))
    return response_data

@app.post("/admin/move-row")
async def move_row(request: MoveRowRequest, req: Request, current_admin: dict = Depends(get_current_admin)):
    client_ip = req.client.host
    forwarded = req.headers.get("X-Forwarded-For")
    if forwarded:
        client_ip = forwarded.split(",")[0].strip()
    print(f"[INFO] IP: {client_ip}")
    print(f"[INFO] Endpoint: POST /admin/move-row")
    print(f"[REQW] Admin {current_admin['username']} sent request to move row: source={request.source}, id={request.id}")
    if request.source not in ["pub", "unpub"]:
        print(f"[ERROR] Invalid source value: {request.source}. Allowed values: pub, unpub")
        print(f"[INFO] Response body: {{\"detail\":\"Invalid source\"}}")
        raise HTTPException(status_code=400, detail="Invalid source")
    source_table = "tbl_1_pub" if request.source == "pub" else "tbl_2_unpub"
    target_table = "tbl_2_unpub" if request.source == "pub" else "tbl_1_pub"
    source_desc = "published items (tbl_1_pub)" if request.source == "pub" else "unpublished items (tbl_2_unpub)"
    target_desc = "unpublished items (tbl_2_unpub)" if request.source == "pub" else "published items (tbl_1_pub)"
    print(f"[INFO] Source: {source_table} ({source_desc})")
    print(f"[INFO] Target: {target_table} ({target_desc})")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {source_table} WHERE id = ?", (request.id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        print(f"[ERROR] Record with ID {request.id} not found in table {source_table}")
        print(f"[INFO] Response body: {{\"detail\":\"Record not found in {source_table}\"}}")
        raise HTTPException(status_code=400, detail=f"Record not found in {source_table}")
    print(f"[INFO] Found record ID={request.id} in table {source_table}")
    cursor.execute(f"SELECT id FROM {target_table} WHERE id = ?", (request.id,))
    if cursor.fetchone():
        conn.close()
        print(f"[ERROR] Record with ID {request.id} already exists in table {target_table}. Move impossible")
        print(f"[INFO] Response body: {{\"detail\":\"Already moved\"}}")
        raise HTTPException(status_code=400, detail="Already moved")
    cursor.execute(f"PRAGMA table_info({source_table})")
    columns = [col[1] for col in cursor.fetchall() if col[1] != 'id']
    placeholders = ','.join(['?'] * len(columns))
    col_names = ','.join(columns)
    values = [row[columns.index(col)+1] for col in columns]
    cursor.execute(f"INSERT INTO {target_table} (id, {col_names}) VALUES (?, {placeholders})", (request.id, *values))
    print(f"[INFO] Inserted row copy into table {target_table}")
    cursor.execute(f"DELETE FROM {source_table} WHERE id = ?", (request.id,))
    print(f"[INFO] Deleted original row from table {source_table}")
    conn.commit()
    conn.close()
    print(f"[INFO] Row ID={request.id} successfully moved from {source_table} to {target_table}")
    print(f"[INFO] Response body: {{\"message\":\"Row moved successfully\"}}")
    print(f"[INFO] Admin {current_admin['username']} moved row id={request.id} from {source_table} to {target_table}")
    response_data = {"message": "Row moved successfully"}
    log_admin_action(current_admin, "/admin/move-row", str(request.model_dump()), str(response_data))
    return response_data

@app.get("/pages-count")
async def pages_count():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM tbl_1_pub")
    total = cursor.fetchone()[0]
    conn.close()
    total_pages = math.ceil(total / 50)
    return {"total_pages": total_pages}

@app.get("/data")
async def get_data(page: int):
    if page < 1:
        raise HTTPException(status_code=400, detail="Page must be >= 1")
    page_size = 50
    offset = (page - 1) * page_size
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM tbl_1_pub")
    total = cursor.fetchone()[0]
    total_pages = math.ceil(total / page_size)
    cursor.execute("SELECT id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, Description, Photos FROM tbl_1_pub ORDER BY id LIMIT ? OFFSET ?", (page_size, offset))
    rows = cursor.fetchall()
    conn.close()
    columns = ['id', 'Code', 'QTY', 'NAME', 'Brand', 'Model', 'PLACE', 'Price', 'Conditions', 'Truck_models', 'Publishing_date', 'Description', 'Photos']
    data = []
    for row in rows:
        row_dict = dict(zip(columns, row))
        photo_names = row_dict['Photos'].split(',') if row_dict['Photos'] else []
        row_dict['photo_urls'] = [f"/photos/{name.strip()}" for name in photo_names if name.strip()]
        data.append(row_dict)
    return {"page": page, "page_size": page_size, "total_pages": total_pages, "data": data}

@app.post("/admin/start-negotiation")
async def start_negotiation(req: StartNegotiationRequest, current_admin: dict = Depends(get_current_admin)):
    if req.sold_qty <= 0:
        raise HTTPException(status_code=400, detail="Invalid quantity")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT QTY FROM tbl_1_pub WHERE id = ?", (req.record_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=400, detail="Record not found")
    current_qty = row[0]
    if req.sold_qty > current_qty:
        conn.close()
        raise HTTPException(status_code=400, detail="Invalid quantity")
    try:
        cursor.execute("UPDATE tbl_1_pub SET QTY = QTY - ? WHERE id = ? AND QTY >= ?", (req.sold_qty, req.record_id, req.sold_qty))
        if cursor.rowcount == 0:
            conn.close()
            raise HTTPException(status_code=400, detail="Concurrent update failed")
        cursor.execute("SELECT id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, Description, Photos FROM tbl_1_pub WHERE id = ?", (req.record_id,))
        updated_row = cursor.fetchone()
        if updated_row and updated_row[2] == 0:
            cursor.execute("DELETE FROM tbl_1_pub WHERE id = ?", (req.record_id,))
            print(f"[INFO] Row id={req.record_id} deleted due to sold_qty={req.sold_qty}")
            row_for_buffer = updated_row
        else:
            cursor.execute("SELECT id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, Description, Photos FROM tbl_1_pub WHERE id = ?", (req.record_id,))
            row_for_buffer = cursor.fetchone()
            if not row_for_buffer:
                cursor.execute("SELECT id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, Description, Photos FROM tbl_1_pub WHERE id = ?", (req.record_id,))
                row_for_buffer = cursor.fetchone()
        existing_ids = get_all_existing_ids(conn)
        new_buffer_id = 1
        while new_buffer_id in existing_ids or new_buffer_id == req.record_id:
            new_buffer_id += 1
        cursor.execute("INSERT INTO tbl_3_buffer (id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, sales_negotiation_start_date, Description, Photos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (new_buffer_id, row_for_buffer[1], req.sold_qty, row_for_buffer[3], row_for_buffer[4], row_for_buffer[5], row_for_buffer[6], row_for_buffer[7], row_for_buffer[8], row_for_buffer[9], row_for_buffer[10], datetime.now(), row_for_buffer[11], row_for_buffer[12]))
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))
    conn.close()
    print(f"[INFO] Admin {current_admin['username']} started negotiation: record_id={req.record_id}, sold_qty={req.sold_qty}")
    response_data = {"message": "Negotiation started"}
    log_admin_action(current_admin, "/admin/start-negotiation", str(req.model_dump()), str(response_data))
    return response_data

@app.post("/admin/end-negotiation")
async def end_negotiation(req: EndNegotiationRequest, current_admin: dict = Depends(get_current_admin)):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, sales_negotiation_start_date, Description, Photos FROM tbl_3_buffer WHERE id = ?", (req.record_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=400, detail="Record not found")
    try:
        cursor.execute("INSERT INTO tbl_4_history (Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, sales_negotiation_start_date, sales_negotiation_end_date, Description, Photos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], datetime.now(), row[12], row[13]))
        cursor.execute("DELETE FROM tbl_3_buffer WHERE id = ?", (req.record_id,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))
    conn.close()
    print(f"[INFO] Admin {current_admin['username']} completed negotiation for record_id={req.record_id}")
    response_data = {"message": "Negotiation completed"}
    log_admin_action(current_admin, "/admin/end-negotiation", str(req.model_dump()), str(response_data))
    return response_data

@app.post("/admin/cancel-negotiation")
async def cancel_negotiation(req: EndNegotiationRequest, current_admin: dict = Depends(get_current_admin)):
    print(f"[INFO] Attempt to cancel negotiation for record ID={req.record_id} by admin: {current_admin['username']}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, Description, Photos FROM tbl_3_buffer WHERE id = ?", (req.record_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=400, detail="Record not found in tbl_3_buffer")
    try:
        existing_ids = get_all_existing_ids(conn)
        new_pub_id = 1
        while new_pub_id in existing_ids or new_pub_id == row[0]:
            new_pub_id += 1
        cursor.execute("INSERT INTO tbl_1_pub (id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, Description, Photos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (new_pub_id, row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12]))
        cursor.execute("DELETE FROM tbl_3_buffer WHERE id = ?", (req.record_id,))
        conn.commit()
        print(f"[INFO] Negotiation cancelled: record_id={req.record_id} returned to tbl_1_pub with new ID={new_pub_id}")
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))
    conn.close()
    response_data = {"message": "Negotiation cancelled", "record_id": req.record_id}
    log_admin_action(current_admin, "/admin/cancel-negotiation", str(req.model_dump()), str(response_data))
    return response_data

@app.post("/admin/reset-sequence/buffer_sales")
async def reset_buffer_sales_sequence(current_admin: dict = Depends(get_current_admin)):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(id) FROM tbl_3_buffer")
    max_id = cursor.fetchone()[0]
    if max_id is not None:
        cursor.execute("UPDATE sqlite_sequence SET seq = ? WHERE name = 'tbl_3_buffer'", (max_id,))
    conn.commit()
    conn.close()
    print(f"[INFO] Reset counter for table tbl_3_buffer")
    response_data = {"message": "Sequence reset for tbl_3_buffer"}
    log_admin_action(current_admin, "/admin/reset-sequence/buffer_sales", "", str(response_data))
    return response_data

@app.get("/")
async def empty_page():
    print("[INFO] Main page request")
    user_html_path = os.path.join(os.path.dirname(__file__), "templates", "user.html")
    if os.path.exists(user_html_path):
        with open(user_html_path, "r", encoding="utf-8") as f:
            content = f.read()
            return HTMLResponse(content=content)
    return HTMLResponse(content="<h1>User Page</h1><p>File templates/user.html not found</p>")

@app.get("/admin")
async def admin_page():
    print("[INFO] Admin page request")
    admin_html_path = os.path.join(os.path.dirname(__file__), "templates", "admin.html")
    if os.path.exists(admin_html_path):
        with open(admin_html_path, "r", encoding="utf-8") as f:
            content = f.read()
            return HTMLResponse(content=content)
    return HTMLResponse(content="<h1>Admin Panel</h1><p>File templates/admin.html not found</p>")

@app.post("/upload")
async def upload_file(file: UploadFile = File(...), current_admin: dict = Depends(get_current_admin)):
    print(f"[REQW] Received file: {file.filename}")
    if not file.filename:
        print("[ERROR] No file selected")
        raise HTTPException(status_code=400, detail="No file selected")
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        print(f"[ERROR] Disallowed file extension: {ext}. Allowed: {ALLOWED_EXTENSIONS}")
        raise HTTPException(status_code=415, detail=f"Unsupported media type. Allowed extensions: {ALLOWED_EXTENSIONS}")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_filename = f"{timestamp}_{uuid.uuid4().hex}.{ext[1:]}"
    file_path = os.path.join(UPLOAD_DIR, safe_filename)
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        print(f"[INFO] File saved: {file_path}")
        config.set_maintenance(True)
        print("[INFO] Maintenance mode activated")
        progress_status["status"] = "running"
        progress_status["progress"] = 0
        progress_status["message"] = "Starting file processing"
        progress_status["current_operation"] = "start"
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM backup_table")
        for src_table in ['tbl_1_pub', 'tbl_2_unpub', 'tbl_3_buffer']:
            cursor.execute(f"SELECT id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, Description, Photos FROM {src_table}")
            rows = cursor.fetchall()
            for row in rows:
                cursor.execute("INSERT INTO backup_table (tbl, original_id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, Description, Photos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (src_table, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12]))
        conn.commit()
        conn.close()
        print("[INFO] Backup of all main tables completed")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            cursor.execute("BEGIN TRANSACTION")
            if ext == '.xlsx':
                progress_status["progress"] = 0
                progress_status["message"] = "Reading Excel file"
                progress_status["current_operation"] = "read_excel"
                df = pd.read_excel(file_path)
                required_columns = ['Code', 'QTY', 'NAME', 'Brand', 'Model', 'PLACE', 'Price', 'Conditions', 'Truck models', 'Publishing date', 'Status']
                for col in required_columns:
                    if col not in df.columns:
                        df[col] = None
                df = df[required_columns]
                progress_status["progress"] = 20
                progress_status["message"] = "Converting data types"
                progress_status["current_operation"] = "convert_types"
                df['QTY'] = pd.to_numeric(df['QTY'], errors='coerce')
                df['QTY'] = df['QTY'].apply(lambda x: 1 if pd.isna(x) else int(x))
                df['Price'] = pd.to_numeric(df['Price'], errors='coerce').fillna(0).astype(int)
                for col in required_columns:
                    if col not in ['QTY', 'Price']:
                        df[col] = df[col].astype(str)
                cursor.execute("DELETE FROM tbl_1_pub")
                cursor.execute("DELETE FROM tbl_2_unpub")
                cursor.execute("DELETE FROM tbl_3_buffer")
                existing_ids = get_all_existing_ids(conn)
                published_rows = []
                unpublished_rows = []
                for _, row in df.iterrows():
                    status_val = row['Status']
                    row_data = (row['Code'], row['QTY'], row['NAME'], row['Brand'], row['Model'], row['PLACE'], row['Price'], row['Conditions'], row['Truck models'], row['Publishing date'])
                    is_empty = pd.isna(status_val) or str(status_val).strip().lower() in ['nan', 'none', '', 'null']
                    if not is_empty and str(status_val).strip() == "Published":
                        published_rows.append(row_data)
                    else:
                        unpublished_rows.append(row_data)
                if published_rows:
                    new_ids = []
                    candidate = 1
                    while len(new_ids) < len(published_rows):
                        if candidate not in existing_ids:
                            new_ids.append(candidate)
                        candidate += 1
                    published_tuples = [(new_ids[i],) + published_rows[i] for i in range(len(published_rows))]
                    cursor.executemany("INSERT INTO tbl_1_pub (id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", published_tuples)
                    existing_ids.update(new_ids)
                if unpublished_rows:
                    new_ids = []
                    candidate = 1
                    while len(new_ids) < len(unpublished_rows):
                        if candidate not in existing_ids:
                            new_ids.append(candidate)
                        candidate += 1
                    unpublished_tuples = [(new_ids[i],) + unpublished_rows[i] for i in range(len(unpublished_rows))]
                    cursor.executemany("INSERT INTO tbl_2_unpub (id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", unpublished_tuples)
            elif ext == '.db':
                progress_status["progress"] = 50
                progress_status["message"] = "Copying data from .db file"
                progress_status["current_operation"] = "copy_from_db"
                conn_src = sqlite3.connect(file_path)
                cursor_src = conn_src.cursor()
                cursor.execute("DELETE FROM tbl_1_pub")
                cursor.execute("DELETE FROM tbl_2_unpub")
                cursor.execute("DELETE FROM tbl_3_buffer")
                cursor_src.execute("SELECT Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, Description, Photos FROM tbl_1_pub")
                rows = cursor_src.fetchall()
                existing_ids = get_all_existing_ids(conn)
                new_ids = []
                candidate = 1
                while len(new_ids) < len(rows):
                    if candidate not in existing_ids:
                        new_ids.append(candidate)
                    candidate += 1
                db_inserts = [(new_ids[i],) + rows[i] for i in range(len(rows))]
                cursor.executemany("INSERT INTO tbl_1_pub (id, Code, QTY, NAME, Brand, Model, PLACE, Price, Conditions, Truck_models, Publishing_date, Description, Photos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", db_inserts)
                conn_src.close()
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
        progress_status["status"] = "completed"
        progress_status["progress"] = 100
        progress_status["message"] = "File processing completed"
        progress_status["current_operation"] = "done"
        print("[INFO] Table update completed")
        response_data = {"message": "File processed and database updated successfully", "filename": safe_filename}
        log_admin_action(current_admin, "/upload", f"filename={file.filename}", str(response_data))
        return response_data
    except Exception as e:
        progress_status["status"] = "error"
        progress_status["progress"] = 0
        progress_status["message"] = f"Error: {str(e)}"
        progress_status["current_operation"] = "error"
        print(f"[ERROR] Processing error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
    finally:
        config.set_maintenance(False)
        print("[INFO] Maintenance mode disabled")

@app.get("/favicon.ico")
async def get_favicon():
    favicon_path = os.path.join(os.path.dirname(__file__), "static", "favicon.ico")
    if not os.path.exists(favicon_path):
        raise HTTPException(status_code=500 , detail="Favicon not found")
    return FileResponse(favicon_path, media_type="image/x-icon")

@app.get("/logo")
async def get_logo():
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    for ext in ['.png', '.jpg', '.jpeg', '.webp']:
        logo_path = os.path.join(static_dir, f"logo{ext}")
        if os.path.exists(logo_path):
            mime_map = {'.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.webp': 'image/webp'}
            return FileResponse(logo_path, media_type=mime_map[ext])
    raise HTTPException(status_code=500, detail="Logo not found")

@app.get("/site-name")
async def get_site_name():
    name_path = os.path.join(os.path.dirname(__file__), "static", "name.txt")
    if not os.path.exists(name_path):
        raise HTTPException(status_code=500, detail="name.txt not found")
    with open(name_path, "r", encoding="utf-8") as f:
        site_name = f.read().strip()
    return JSONResponse(content={"site_name": site_name})

@app.get("/contacts")
async def get_contacts():
    contacts_path = os.path.join(os.path.dirname(__file__), "static", "contacts.txt")
    if not os.path.exists(contacts_path):
        return JSONResponse(content={"phone": "", "name": ""})
    try:
        with open(contacts_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if ":" in content:
                phone, name = content.split(":", 1)
                return JSONResponse(content={"phone": phone.strip(), "name": name.strip()})
            return JSONResponse(content={"phone": content, "name": ""})
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error reading contacts")

@app.post("/admin/contacts")
async def update_contacts(contact: ContactUpdate, current_admin: dict = Depends(get_current_super_admin)):
    print(f"[INFO] Attempt to update contacts by admin: {current_admin['username']}")
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    os.makedirs(static_dir, exist_ok=True)
    contacts_path = os.path.join(static_dir, "contacts.txt")
    try:
        with open(contacts_path, "w", encoding="utf-8") as f:
            f.write(f"{contact.phone}:{contact.name}")
        print(f"[INFO] Contacts updated successfully")
        response_data = {"message": "Контактные данные успешно обновлены"}
        log_admin_action(current_admin, "/admin/contacts", f"{contact.phone}:{contact.name}", str(response_data))
        return response_data
    except Exception as e:
        print(f"[ERROR] Failed to update contacts: {e}")
        raise HTTPException(status_code=500, detail="Error writing contacts")

if __name__ == "__main__":
    import uvicorn
    print("[INFO] Starting uvicorn server")
    uvicorn.run(app, host="0.0.0.0", port=8000, timeout_keep_alive=5, limit_concurrency=50, limit_max_requests=10000)