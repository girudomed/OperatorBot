# main.py    
import os
import io
import time
import json
import base64
import hmac
import hashlib
from datetime import datetime, timezone, timedelta
from flask import Flask, request, render_template_string, send_file, session, redirect, url_for
from dotenv import load_dotenv
import qrcode
from pathlib import Path
import utils.httpx_proxy_patch  # noqa: F401
from supabase import create_client, Client

# Московское время (UTC+3)
MOSCOW_TZ = timezone(timedelta(hours=3))

def get_moscow_time():
    """Получить текущее время в Москве"""
    return datetime.now(MOSCOW_TZ)

def get_moscow_timestamp():
    """Получить timestamp московского времени"""
    return int(get_moscow_time().timestamp())

# Загрузка переменных окружения
load_dotenv()

SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")
QR_SECRET = os.environ.get("QR_SECRET")

if not (SUPABASE_URL and SUPABASE_KEY and QR_SECRET):
    raise Exception("Укажите все переменные окружения: NEXT_PUBLIC_SUPABASE_URL, NEXT_PUBLIC_SUPABASE_ANON_KEY, QR_SECRET")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "your-secret-key-here-change-in-production")

@app.route('/health')
def health():
    return "200", 200

# Получение списка филиалов из таблицы 'branches' через Supabase API
def get_branches():
    try:
        res = supabase.table("branches").select("*").execute()
        if res.data:
            print(f"Получено филиалов: {len(res.data)}", flush=True)
            return res.data
        print("Филиалы не найдены", flush=True)
        return []
    except Exception as e:
        import traceback; traceback.print_exc()
        print("Ошибка при получении филиалов через Supabase API:", e, flush=True)
        return []

# Генерация подписи для QR (HMAC-SHA256)
def generate_signature(branch_id, time_window):
    """Генерация подписи для QR-кода"""
    try:
        msg = f"{branch_id}:{time_window}".encode()
        secret = QR_SECRET.encode()
        return hmac.new(secret, msg, hashlib.sha256).hexdigest()
    except Exception as e:
        print(f"Ошибка генерации подписи: {e}", flush=True)
        return None

# Генерация base64-кодированной строки для QR
def generate_qr_payload(branch_id, branch_name):
    """Генерация данных для QR-кода с улучшенной логикой времени"""
    try:
        timestamp = get_moscow_timestamp()
        time_window = timestamp // 30  # Окно 30 секунд
        expires = timestamp + 120  # Увеличиваем время жизни до 2 минут
        signature = generate_signature(branch_id, time_window)
        
        if not signature:
            print("Ошибка генерации подписи QR-кода", flush=True)
            return None
        
        payload = {
            "branch_id": branch_id,
            "branch_name": branch_name,
            "timestamp": time_window,
            "expires": expires,
            "signature": signature,
            "generated_at": timestamp  # Добавляем время генерации для отладки
        }
        
        json_str = json.dumps(payload, ensure_ascii=False)
        base64_str = base64.urlsafe_b64encode(json_str.encode()).decode()
        
        print(f"QR-код сгенерирован: branch_id={branch_id}, time_window={time_window}, expires={expires}", flush=True)
        return base64_str
        
    except Exception as e:
        print(f"Ошибка генерации QR-кода: {e}", flush=True)
        return None

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        
        if username == "Admin" and password == "master940nw":
            session["authenticated"] = True
            return redirect(url_for("index"))
        else:
            return render_template_string(
                """
                <!doctype html>
                <html>
                <head>
                    <title>Авторизация</title>
                    <meta charset="utf-8">
                    <style>
                        @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700&display=swap');
                        body { font-family: 'Montserrat', Arial, sans-serif; background: #f4f4f4; padding: 2em; }
                        .container { background: #fff; padding: 2em; max-width: 400px; margin: auto; border-radius: 16px; box-shadow: 0 0 10px #bbb; }
                        input, button { padding: 0.6em 1em; border-radius: 8px; border: 1px solid #ccc; margin-bottom: 1em; width: 100%; font-family: 'Montserrat', Arial, sans-serif; }
                        .error { color: #dc3545; margin-bottom: 1em; }
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h2>Авторизация</h2>
                        <div class="error">Неверный логин или пароль</div>
                        <form method="post">
                            <input type="text" name="username" placeholder="Логин" required>
                            <input type="password" name="password" placeholder="Пароль" required>
                            <button type="submit">Войти</button>
                        </form>
                    </div>
                </body>
                </html>
                """
            )
    
    return render_template_string(
        """
        <!doctype html>
        <html>
        <head>
            <title>Авторизация</title>
            <meta charset="utf-8">
            <style>
                @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700&display=swap');
                body { font-family: 'Montserrat', Arial, sans-serif; background: #f4f4f4; padding: 2em; }
                .container { background: #fff; padding: 2em; max-width: 400px; margin: auto; border-radius: 16px; box-shadow: 0 0 10px #bbb; }
                input, button { padding: 0.6em 1em; border-radius: 8px; border: 1px solid #ccc; margin-bottom: 1em; width: 100%; font-family: 'Montserrat', Arial, sans-serif; }
            </style>
        </head>
        <body>
            <div class="container">
                <h2>Авторизация</h2>
                <form method="post">
                    <input type="text" name="username" placeholder="Логин" required>
                    <input type="password" name="password" placeholder="Пароль" required>
                    <button type="submit">Войти</button>
                </form>
            </div>
        </body>
        </html>
        """
    )

@app.route("/logout")
def logout():
    session.pop("authenticated", None)
    return redirect(url_for("login"))

def require_auth():
    if not session.get("authenticated"):
        return redirect(url_for("login"))
    return None

@app.route("/", methods=["GET"])
def index():
    auth_check = require_auth()
    if auth_check:
        return auth_check
    
    branches = get_branches()
    return render_template_string(
        """
        <!doctype html>
        <html>
        <head>
            <title>QR для сотрудников</title>
            <meta charset="utf-8">
            <style>
                @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700&display=swap');
                body { font-family: 'Montserrat', Arial, sans-serif; background: #f4f4f4; padding: 2em; }
                .container { background: #fff; padding: 2em; max-width: 400px; margin: auto; border-radius: 16px; box-shadow: 0 0 10px #bbb; }
                select, button { padding: 0.6em 1em; border-radius: 8px; border: 1px solid #ccc; margin-bottom: 1em; width: 100%; font-family: 'Montserrat', Arial, sans-serif; }
                img { display: block; margin: 1.5em auto; }
            </style>
        </head>
        <body>
            <div class="container">
                <h2>Генерация QR-кода</h2>
                <form method="get" action="/qr">
                    <label>Выберите филиал:</label>
                    <select name="branch_id" required>
                        {% for branch in branches %}
                            <option value="{{branch['id']}}">{{branch['name']}}</option>
                        {% endfor %}
                    </select>
                    <button type="submit">Показать QR</button>
                </form>
                <div style="margin-top: 1em; text-align: center;">
                    <a href="/logout" style="color: #666; text-decoration: none; font-size: 0.9em;">Выйти</a>
                </div>
            </div>
        </body>
        </html>
        """, branches=branches
    )

@app.route("/qr", methods=["GET"])
def qr():
    auth_check = require_auth()
    if auth_check:
        return auth_check
    
    branch_id = request.args.get("branch_id", type=int)
    if not branch_id:
        return "Не выбран филиал", 400
    branches = get_branches()
    branch = next((b for b in branches if b["id"] == branch_id), None)
    if not branch:
        return "Филиал не найден", 404
    qr_data = generate_qr_payload(branch_id, branch["name"])
    qr_full = f"/qr_{qr_data}"

    # Получаем все пункты философии из Supabase через API
    philosophy_points = []
    try:
        res = supabase.table("philosophy").select("text").order("id", desc=False).execute()
        if res.data:
            philosophy_points = [row["text"] for row in res.data]
        else:
            philosophy_points = ["Философия клиники «Гирудомед»: нет данных."]
    except Exception as e:
        philosophy_points = ["Философия клиники «Гирудомед»: ошибка загрузки из Supabase."]

    # Генерируем картинку QR
    qr_img = qrcode.make(qr_full)
    img_io = io.BytesIO()
    qr_img.save(img_io, 'PNG')
    img_io.seek(0)

    # HTML для отображения QR-кода с автообновлением и философией
    return render_template_string(
        """
        <!doctype html>
        <html lang="ru">
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
            <title>QR-код для {{branch['name']}}</title>
            <style>
                @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700&display=swap');
                html,body {
                    height: 100%;
                    margin: 0;
                    padding: 0;
                    font-family: 'Montserrat', 'Inter', 'Segoe UI', Arial, sans-serif;
                    background: linear-gradient(135deg, #0e1f13 0%, #1a3a24 100%);
                    color: #eaf7ef;
                    min-height: 100vh;
                }
                body {
                    display: flex;
                    flex-direction: column;
                    min-height: 100vh;
                }
                .container {
                    background: rgba(24, 44, 32, 0.97);
                    border-radius: 2.2rem;
                    box-shadow: 0 8px 40px 0 rgba(34, 139, 34, 0.18), 0 1.5px 8px 0 rgba(0,0,0,0.10);
                    max-width: 630px;
                    margin: auto;
                    padding: 2.2rem 1.2rem 1.2rem 1.2rem;
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    gap: 1.2rem;
                    min-height: 92vh;
                    justify-content: center;
                }
                .philosophy-title {
                    font-size: 1.15rem;
                    font-weight: 700;
                    color: #b6f5c6;
                    letter-spacing: 0.5px;
                    margin-bottom: 0.3rem;
                    text-shadow: 0 1px 8px #1e4d2b44;
                }
                .philosophy-block {
                    font-size: 1.01em;
                    font-weight: 500;
                    line-height: 1.45;
                    color: #1a3a24;
                    background: #ffffff;
                    border: 1.5px solid #4ecb7a;
                    border-radius: 1.1rem;
                    margin-bottom: 0.2rem;
                    padding: 1rem 0.8rem;
                    width: 100%;
                    max-width: 510px;
                    box-sizing: border-box;
                    box-shadow: 0 2px 12px rgba(76, 203, 122, 0.10);
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    text-align: center;
                    overflow-wrap: break-word;
                    word-break: break-word;
                    hyphens: auto;
                    white-space: pre-wrap;
                    transition: all 0.4s ease-in-out;
                    min-height: auto;
                }
                h2 {
                    font-size: 1.13rem;
                    font-weight: 600;
                    color: #eaf7ef;
                    margin: 0.3rem 0 0.5rem 0;
                    letter-spacing: 0.01em;
                }
                .qr-box {
                    background: #fff;
                    border-radius: 1.2rem;
                    box-shadow: 0 2px 16px rgba(34, 139, 34, 0.10);
                    padding: 0.7rem;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    margin-bottom: 0.2rem;
                    transition: transform 0.4s ease-in-out;
                }
                .qr-box img {
                    width: 60vw;
                    max-width: 260px;
                    min-width: 120px;
                    height: auto;
                    display: block;
                }
                .timer {
                    font-size: 1.01rem;
                    color: #b6f5c6;
                    margin: 0.5rem 0 0.1rem 0;
                    letter-spacing: 0.02em;
                }
                .expired { color: #dc3545; }
                .back-link {
                    display: inline-block;
                    margin-top: 0.7rem;
                    color: #b6f5c6;
                    text-decoration: none;
                    font-weight: 500;
                    font-size: 1.5rem;
                    transition: color 0.2s;
                }
                .back-link:hover { color: #4ecb7a; }
                @media (max-width: 600px) {
                    .container { padding: 0.7rem 0.2rem; min-height: 99vh; }
                    .philosophy-block { font-size: 0.98em; }
                    .qr-box img { width: 90vw; max-width: 98vw; }
                }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="philosophy-title">Философия клиники «Гирудомед»</div>
                <div class="philosophy-block" id="philosophy-text"></div>
                <h2>Филиал: {{branch['name']}}</h2>
                <div class="qr-box">
                    <img id="qr-image" src="/qr_image?branch_id={{branch['id']}}&t=" alt="QR-код" />
                </div>
                <div class="timer">
                    <span id="timer">Обновление через: <span id="countdown">30</span> сек</span>
                </div>
                <div style="font-size:0.97rem;color:#b6f5c6;margin-top:0.1rem;">QR-код действителен 1 минуту</div>
                <a class="back-link" href="/">←</a>
            </div>
            <script>
                // Философия из Python
                const philosophyPoints = {{ philosophy_points|tojson }};
                let philosophyBlock = document.getElementById('philosophy-text');
                
                function getRandomPhilosophy() {
                    const randomIndex = Math.floor(Math.random() * philosophyPoints.length);
                    const pointNumber = randomIndex + 1;
                    
                    // Плавная смена текста с адаптацией размера
                    philosophyBlock.style.opacity = '0.6';
                    philosophyBlock.style.transform = 'scale(0.98)';
                    
                    setTimeout(() => {
                        philosophyBlock.textContent = `${pointNumber}. ${philosophyPoints[randomIndex]}`;
                        
                        // Возвращаем нормальное состояние
                        philosophyBlock.style.opacity = '1';
                        philosophyBlock.style.transform = 'scale(1)';
                    }, 200);
                }
                
                getRandomPhilosophy();
                setInterval(getRandomPhilosophy, 15000);

                let countdown = 30;
                let timerElement = document.getElementById('countdown');
                let qrImage = document.getElementById('qr-image');
                let branchId = {{branch['id']}};
                
                function updateQR() {
                    qrImage.src = '/qr_image?branch_id=' + branchId + '&t=' + Date.now();
                    countdown = 30;
                }
                
                function updateTimer() {
                    timerElement.textContent = countdown;
                    countdown--;
                    if (countdown < 0) {
                        updateQR();
                    }
                }
                
                setInterval(updateTimer, 1000);
                setInterval(updateQR, 30000);
            </script>
        </body>
        </html>
        """, branch=branch, qr_data=qr_data, philosophy_points=philosophy_points
    )

@app.route("/qr_image")
def qr_image():
    auth_check = require_auth()
    if auth_check:
        return auth_check
    
    # Поддержка старого формата (data) и нового (branch_id)
    data = request.args.get("data", "")
    branch_id = request.args.get("branch_id", type=int)
    
    if data:
        # Старый формат - используем готовые данные
        qr_full = f"/qr_{data}"
    elif branch_id:
        # Новый формат - генерируем QR по branch_id
        branches = get_branches()
        branch = next((b for b in branches if b["id"] == branch_id), None)
        if not branch:
            return "Филиал не найден", 404
        qr_data = generate_qr_payload(branch_id, branch["name"])
        qr_full = f"/qr_{qr_data}"
    else:
        return "Нет данных для QR", 400
    
    qr_img = qrcode.make(qr_full)
    img_io = io.BytesIO()
    qr_img.save(img_io, 'PNG')
    img_io.seek(0)
    return send_file(img_io, mimetype='image/png')

if __name__ == "__main__":
    app.run("0.0.0.0", 8080, debug=True)
