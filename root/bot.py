import logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
import requests
import os
import re
import sys
import asyncio
import hashlib
import glob
import time
from aiogram import types
from asyncio import sleep
from aiogram.filters import StateFilter
from aiogram.exceptions import TelegramBadRequest
import sqlite3
import uuid
from aiogram.fsm.state import State, StatesGroup
from collections import deque
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, FSInputFile, BotCommand, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.fsm.context import FSMContext
from aiogram.client.default import DefaultBotProperties
import subprocess
from datetime import datetime, timedelta, timezone
import psutil
import platform
import socket
import json
import shutil
import yookassa
from yookassa import Configuration, Payment

# Настройка ЮKassa
YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID")
YOOKASSA_SECRET_KEY = os.getenv("YOOKASSA_SECRET_KEY")
Configuration.configure(YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY)

# Новое состояние для платежей
class PaymentStates(StatesGroup):
    waiting_for_amount = State()
    waiting_for_payment_confirmation = State()

class SetEmoji(StatesGroup):
    waiting_for_emoji = State()

class RenameProfile(StatesGroup):
    waiting_for_new_name = State()
    waiting_for_rename_approve = State()

class VPNSetup(StatesGroup):
    entering_user_id = State()
    entering_client_name_manual = State()
    choosing_option = State()
    entering_client_name = State()
    entering_days = State()
    deleting_client = State()
    list_for_delete = State()
    choosing_config_type = State()
    choosing_protocol = State()
    choosing_wg_type = State()
    confirming_rename = State()

class AdminAnnounce(StatesGroup):
    waiting_for_text = State()

# Инициализация базы данных с таблицей балансов и платежей
def init_db(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            profile_name TEXT,
            balance REAL DEFAULT 0.0
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            payment_id TEXT PRIMARY KEY,
            user_id INTEGER,
            amount REAL,
            status TEXT,
            created_at TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

# Сохранение имени профиля и инициализация баланса
def save_profile_name(user_id, new_profile_name, db_path="/root/vpn.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE id=?", (user_id,))
    res = cur.fetchone()
    if res:
        cur.execute("UPDATE users SET profile_name=? WHERE id=?", (new_profile_name, user_id))
    else:
        cur.execute("INSERT INTO users (id, profile_name, balance) VALUES (?, ?, 0.0)", (user_id, new_profile_name))
    conn.commit()
    conn.close()

# Получение баланса пользователя
def get_user_balance(user_id, db_path="/root/vpn.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT balance FROM users WHERE id=?", (user_id,))
    res = cur.fetchone()
    conn.close()
    return res[0] if res else 0.0

# Обновление баланса пользователя
def update_user_balance(user_id, amount, db_path="/root/vpn.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("UPDATE users SET balance = balance + ? WHERE id=?", (amount, user_id))
    conn.commit()
    conn.close()

# Сохранение информации о платеже
def save_payment(payment_id, user_id, amount, status, db_path="/root/vpn.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO payments (payment_id, user_id, amount, status, created_at) VALUES (?, ?, ?, ?, ?)",
        (payment_id, user_id, amount, status, datetime.now(timezone.utc))
    )
    conn.commit()
    conn.close()

# Обновление статуса платежа
def update_payment_status(payment_id, status, db_path="/root/vpn.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("UPDATE payments SET status=? WHERE payment_id=?", (status, payment_id))
    conn.commit()
    conn.close()

# Получение статуса платежа
def get_payment_info(payment_id, db_path="/root/vpn.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT user_id, amount, status FROM payments WHERE payment_id=?", (payment_id,))
    res = cur.fetchone()
    conn.close()
    return {"user_id": res[0], "amount": res[1], "status": res[2]} if res else None

# Получение списка всех пользователей и их балансов
def get_all_users_balances(db_path="/root/vpn.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT id, profile_name, balance FROM users")
    users = cur.fetchall()
    conn.close()
    return users

# Инициализация базы данных
DB_PATH = "/root/vpn.db"
init_db(DB_PATH)

cancel_markup = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="❌ Отмена")]],
    resize_keyboard=True,
    one_time_keyboard=True
)

USERS_FILE = "users.txt"
LAST_MENUS_FILE = "last_menus.json"
PENDING_FILE = "pending_users.json"
EMOJI_FILE = "user_emojis.json"
MAX_MENUS_PER_USER = 3
MAX_BOT_MENUS = 1

load_dotenv()
FILEVPN_NAME = os.getenv("FILEVPN_NAME")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")

if not FILEVPN_NAME:
    raise RuntimeError("FILEVPN_NAME не задан в .env")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан в .env")
if not ADMIN_ID:
    raise RuntimeError("ADMIN_ID не задан в .env")
if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET_KEY:
    raise RuntimeError("YOOKASSA_SHOP_ID или YOOKASSA_SECRET_KEY не заданы в .env")
ADMIN_ID = int(ADMIN_ID)

ITEMS_PER_PAGE = 5
AUTHORIZED_USERS = [ADMIN_ID]
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

print(f"=== BOT START ===")
print(f"BOT_TOKEN starts with: {BOT_TOKEN[:8]}...")
print(f"ADMIN_ID: {ADMIN_ID} ({type(ADMIN_ID)})")
print(f"YOOKASSA_SHOP_ID: {YOOKASSA_SHOP_ID[:8]}...")
print(f"==================")

# Создание платежа через ЮKassa
def create_payment(user_id, amount):
    idempotence_key = str(uuid.uuid4())
    payment = Payment.create({
        "amount": {
            "value": f"{amount:.2f}",
            "currency": "RUB"
        },
        "confirmation": {
            "type": "redirect",
            "return_url": "https://your-bot-domain.com/return"  # Замените на ваш URL
        },
        "capture": True,
        "description": f"Пополнение баланса для пользователя {user_id}",
        "metadata": {"user_id": user_id}
    }, idempotence_key)
    save_payment(payment.id, user_id, amount, payment.status)
    return payment

# Проверка статуса платежа
async def check_payment_status(payment_id):
    payment = Payment.find_one(payment_id)
    return payment.status

# Модифицированное меню пользователя с балансом и кнопкой пополнения
def create_user_menu(client_name, back_callback="main_menu", is_admin=False, user_id=None):
    balance = get_user_balance(user_id) if user_id else 0.0
    keyboard = [
        [InlineKeyboardButton(text="📥 Получить OpenVPN", callback_data=f"select_openvpn_{client_name}")],
        [InlineKeyboardButton(text="📥 Получить WireGuard", callback_data=f"get_wg_{client_name}")],
        [InlineKeyboardButton(text="📥 Получить Amnezia", callback_data=f"get_amnezia_{client_name}")],
        [InlineKeyboardButton(text="📥 Получить VLESS", callback_data=f"get_vless_{client_name}")],
        [InlineKeyboardButton(text=f"💰 Баланс: {balance:.2f} RUB", callback_data="show_balance")],
        [InlineKeyboardButton(text="➕ Пополнить баланс", callback_data="top_up_balance")],
    ]
    if is_admin:
        keyboard.append([InlineKeyboardButton(text="⚙️ Установить эмодзи", callback_data=f"set_emoji_{client_name}")])
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# Функция для отображения меню
async def show_menu(chat_id, text, reply_markup):
    try:
        msg = await bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode="HTML")
        set_last_menu_id(chat_id, msg.message_id)
        return msg
    except Exception as e:
        print(f"[show_menu] Ошибка: {e}")

# Сохранение ID последнего меню
def set_last_menu_id(user_id, msg_id):
    data = {}
    if os.path.exists(LAST_MENUS_FILE):
        with open(LAST_MENUS_FILE, "r") as f:
            data = json.load(f)
    user_id = str(user_id)
    data[user_id] = [msg_id]
    with open(LAST_MENUS_FILE, "w") as f:
        json.dump(data, f)

# Получение ID последних меню
def get_last_menu_ids(user_id):
    if not os.path.exists(LAST_MENUS_FILE):
        return []
    try:
        with open(LAST_MENUS_FILE, "r") as f:
            data = json.load(f)
        return data.get(str(user_id), [])
    except Exception:
        return []

# Удаление последних меню
async def delete_last_menus(user_id):
    if not os.path.exists(LAST_MENUS_FILE):
        return
    with open(LAST_MENUS_FILE, "r") as f:
        data = json.load(f)
    ids = data.get(str(user_id), [])
    for mid in ids:
        try:
            await bot.delete_message(user_id, mid)
        except Exception:
            pass
    data[str(user_id)] = []
    with open(LAST_MENUS_FILE, "w") as f:
        json.dump(data, f)

# Добавление в очередь ожидания
def add_pending(user_id, username, fullname):
    pending = {}
    if os.path.exists(PENDING_FILE):
        with open(PENDING_FILE, "r") as f:
            pending = json.load(f)
    pending[str(user_id)] = {"username": username, "fullname": fullname}
    with open(PENDING_FILE, "w") as f:
        json.dump(pending, f)

# Удаление из очереди ожидания
def remove_pending(user_id):
    if not os.path.exists(PENDING_FILE):
        return
    with open(PENDING_FILE, "r") as f:
        pending = json.load(f)
    pending.pop(str(user_id), None)
    with open(PENDING_FILE, "w") as f:
        json.dump(pending, f)

# Проверка статуса ожидания
def is_pending(user_id):
    if not os.path.exists(PENDING_FILE):
        return False
    try:
        with open(PENDING_FILE, "r") as f:
            pending = json.load(f)
    except Exception:
        pending = {}
    return str(user_id) in pending

# Безопасная отправка сообщения
async def safe_send_message(chat_id, text, **kwargs):
    print(f"[SAFE_SEND] chat_id={chat_id}, text={text[:50]}, kwargs={kwargs}")
    try:
        await bot.send_message(chat_id, text, **kwargs)
        print(f"[SAFE_SEND] success to {chat_id}!")
    except Exception as e:
        print(f"[Ошибка отправки сообщения] chat_id={chat_id}: {e}")

# Проверка регистрации пользователя
def user_registered(user_id):
    return bool(get_profile_name(user_id))

APPROVED_FILE = "approved_users.txt"

# Установка эмодзи пользователя
def set_user_emoji(user_id, emoji):
    data = {}
    if os.path.exists(EMOJI_FILE):
        with open(EMOJI_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    data[str(user_id)] = emoji
    with open(EMOJI_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

# Получение эмодзи пользователя
def get_user_emoji(user_id):
    if not os.path.exists(EMOJI_FILE):
        return ""
    with open(EMOJI_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get(str(user_id), "")

# Проверка одобренного пользователя
def is_approved_user(user_id):
    user_id = str(user_id)
    if not os.path.exists(APPROVED_FILE):
        return False
    with open(APPROVED_FILE, "r") as f:
        approved = [line.strip() for line in f]
    return user_id in approved

# Одобрение пользователя
def approve_user(user_id):
    user_id = str(user_id)
    if not is_approved_user(user_id):
        with open(APPROVED_FILE, "a") as f:
            f.write(user_id + "\n")

# Сохранение ID пользователя
def save_user_id(user_id):
    try:
        user_id = str(user_id)
        if not os.path.exists(USERS_FILE):
            with open(USERS_FILE, "w") as f:
                f.write(f"{user_id}\n")
        else:
            with open(USERS_FILE, "r+") as f:
                users = set(line.strip() for line in f)
                if user_id not in users:
                    f.write(f"{user_id}\n")
    except Exception as e:
        print(f"[save_user_id] Ошибка при сохранении user_id: {e}")

# Удаление ID пользователя
def remove_user_id(user_id):
    if not os.path.exists(USERS_FILE):
        return
    try:
        with open(USERS_FILE, "r") as f:
            lines = [line.strip() for line in f if line.strip().isdigit()]
        updated = [line for line in lines if line != str(user_id)]
        with open(USERS_FILE, "w") as f:
            for uid in updated:
                f.write(f"{uid}\n")
    except Exception as e:
        print(f"[remove_user_id] Не удалось обновить {USERS_FILE}: {e}")

# Удаление одобренного пользователя
def remove_approved_user(user_id):
    if not os.path.exists(APPROVED_FILE):
        return
    try:
        with open(APPROVED_FILE, "r") as f:
            lines = [line.strip() for line in f]
        updated = [line for line in lines if line != str(user_id)]
        with open(APPROVED_FILE, "w") as f:
            for uid in updated:
                f.write(f"{uid}\n")
    except Exception as e:
        print(f"[remove_approved_user] Не удалось обновить {APPROVED_FILE}: {e}")

# Получение имени профиля
def get_profile_name(user_id, db_path="/root/vpn.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT profile_name FROM users WHERE id=?", (user_id,))
    res = cur.fetchone()
    conn.close()
    return res[0] if res else None

# Получение ID пользователя по имени
def get_user_id_by_name(client_name, db_path="/root/vpn.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE profile_name=?", (client_name,))
    res = cur.fetchone()
    conn.close()
    return res[0] if res else None

# Получение информации о сертификате
def get_cert_expiry_info(client_name):
    try:
        result = subprocess.run(
            ["/etc/openvpn/easyrsa3/easyrsa", "--batch", "show-cert", client_name],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return None
        expiry_match = re.search(r"notAfter=([^\n]+)", result.stdout)
        if not expiry_match:
            return None
        expiry_date = datetime.strptime(expiry_match.group(1), "%b %d %H:%M:%S %Y %Z")
        days_left = (expiry_date - datetime.now()).days
        return {"days_left": days_left}
    except Exception:
        return None

# Выполнение скрипта
async def execute_script(option: str, client_name: str = "", days: str = ""):
    cmd = ["/root/antizapret/client.sh", option]
    if client_name:
        cmd.append(client_name)
    if days:
        cmd.append(days)
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        return {
            "returncode": process.returncode,
            "stdout": stdout.decode(),
            "stderr": stderr.decode(),
        }
    except Exception as e:
        return {"returncode": 1, "stdout": "", "stderr": str(e)}

# Проверка существования клиента
async def client_exists(vpn_type: str, client_name: str) -> bool:
    clients = await get_clients(vpn_type)
    return client_name in clients

# Получение списка клиентов
async def get_clients(vpn_type: str):
    option = "3" if vpn_type == "openvpn" else "6"
    result = await execute_script(option)
    if result["returncode"] == 0:
        clients = [
            c.strip()
            for c in result["stdout"].split("\n")
            if c.strip()
            and not c.startswith("OpenVPN client names:")
            and not c.startswith("WireGuard/AmneziaWG client names:")
            and not c.startswith("OpenVPN - List clients")
            and not c.startswith("WireGuard/AmneziaWG - List clients")
        ]
        return clients
    return []

# Получение онлайн-пользователей из лога
def get_online_users_from_log():
    users = {}
    try:
        with open("/var/log/openvpn-status.log", "r") as f:
            for line in f:
                if line.startswith("CLIENT_LIST"):
                    parts = line.strip().split(",")
                    if len(parts) > 3:
                        users[parts[1]] = "OpenVPN"
    except Exception:
        pass
    return users

# Получение онлайн-пользователей WireGuard
def get_online_wg_peers():
    peers = {}
    try:
        result = subprocess.run(["wg", "show", "wg0"], capture_output=True, text=True)
        lines = result.stdout.split("\n")
        current_peer = None
        for line in lines:
            line = line.strip()
            if line.startswith("peer:"):
                current_peer = line.split()[1]
            elif line.startswith("endpoint:") and current_peer:
                for base in ["/root/antizapret/client/wireguard", "/root/antizapret/client/amneziawg"]:
                    for root, _, files in os.walk(base):
                        for fname in files:
                            if fname.endswith(".conf"):
                                path = os.path.join(root, fname)
                                try:
                                    with open(path, encoding="utf-8") as cf:
                                        content = cf.read()
                                        if current_peer in content:
                                            client_name = fname.split("-")[1].split(".")[0]
                                            peers[client_name] = "WG" if "wireguard" in root else "Amnezia"
                                            break
                                except Exception:
                                    pass
                            if current_peer in peers:
                                break
                        if current_peer in peers:
                            break
    except Exception as e:
        print(f"[ERROR] wg show: {e}")
    return peers

# Создание клавиатуры подтверждения
def create_confirmation_keyboard(client_name: str, vpn_type: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Подтвердить", callback_data=f"confirm_{vpn_type}_{client_name}"
                ),
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_delete"),
            ]
        ]
    )

# Создание клавиатуры списка клиентов
def create_client_list_keyboard(clients, page: int, total_pages: int, vpn_type: str, action: str):
    start = (page - 1) * ITEMS_PER_PAGE
    end = start + ITEMS_PER_PAGE
    keyboard = []
    for client in clients[start:end]:
        keyboard.append(
            [InlineKeyboardButton(text=client, callback_data=f"{action}_{vpn_type}_{client}")]
        )
    nav_buttons = []
    if page > 1:
        nav_buttons.append(
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"page_{action}_{vpn_type}_{page-1}")
        )
    if page < total_pages:
        nav_buttons.append(
            InlineKeyboardButton(text="Вперед ➡️", callback_data=f"page_{action}_{vpn_type}_{page+1}")
        )
    if nav_buttons:
        keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton(text="⬅️ Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# Создание меню OpenVPN конфигурации
def create_openvpn_config_menu(client_name):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Обычный VPN", callback_data=f"send_ovpn_vpn_default_{client_name}"
                ),
                InlineKeyboardButton(
                    text="Antizapret", callback_data=f"send_ovpn_antizapret_default_{client_name}"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Назад", callback_data=f"cancel_openvpn_config_{client_name}"
                )
            ],
        ]
    )

# Создание меню WireGuard конфигурации
def create_wireguard_config_menu(client_name):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Обычный VPN", callback_data=f"send_wg_vpn_wg_{client_name}"
                ),
                InlineKeyboardButton(
                    text="Antizapret", callback_data=f"send_wg_antizapret_wg_{client_name}"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Назад", callback_data=f"cancel_openvpn_config_{client_name}"
                )
            ],
        ]
    )

# Создание главного меню
def create_main_menu():
    keyboard = [
        [InlineKeyboardButton(text="Управление пользователями", callback_data="users_menu")],
        [InlineKeyboardButton(text="Добавить/Удалить", callback_data="add_del_menu")],
        [InlineKeyboardButton(text="Пересоздать файлы VPN", callback_data="7")],
        [InlineKeyboardButton(text="Создать резерв", callback_data="8")],
        [InlineKeyboardButton(text="Заявки на получение доступа", callback_data="admin_pending_list")],
        [InlineKeyboardButton(text="Управление сервером VPN", callback_data="server_manage_menu")],
        [InlineKeyboardButton(text="Сделать объявление", callback_data="announce_menu")],
        [InlineKeyboardButton(text="В сети", callback_data="who_online")],
        [InlineKeyboardButton(text="💰 Балансы пользователей", callback_data="view_all_balances")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# Создание меню управления сервером
def create_server_manage_menu():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Перезагрузить бота", callback_data="restart_bot")],
            [InlineKeyboardButton(text="Перезагрузить сервер", callback_data="reboot_server")],
            [InlineKeyboardButton(text="Вернуться", callback_data="main_menu")],
        ]
    )

# Создание меню вкладок пользователей
def make_users_tab_keyboard(active_tab: str):
    tabs = [
        ("Все", "users_tab_all"),
        ("В сети", "users_tab_online"),
        ("Скоро ББ", "users_tab_expiring"),
    ]
    buttons = []
    for title, cb in tabs:
        text = f"» {title} «" if cb == active_tab else title
        buttons.append(InlineKeyboardButton(text=text, callback_data=cb))
    return InlineKeyboardMarkup(inline_keyboard=[buttons])

# Получение внешнего IP
def get_external_ip():
    try:
        response = requests.get("https://api.ipify.org", timeout=10)
        if response.status_code == 200:
            return response.text
        return "IP не найден"
    except requests.Timeout:
        return "Ошибка: запрос превысил время ожидания."
    except requests.ConnectionError:
        return "Ошибка: нет подключения к интернету."
    except requests.RequestException as e:
        return f"Ошибка при запросе: {e}"
SERVER_IP = get_external_ip()

# Получение информации о сервере
def get_server_info():
    ip = SERVER_IP
    uptime_seconds = int(psutil.boot_time())
    uptime = datetime.now() - datetime.fromtimestamp(uptime_seconds)
    cpu = psutil.cpu_percent()
    mem = psutil.virtual_memory().percent
    hostname = socket.gethostname()
    os_version = platform.platform()
    return f"""<b>💻 Сервер:</b> <code>{hostname}</code>
<b>🌐 IP:</b> <code>{ip}</code>
<b>🕒 Аптайм:</b> <code>{str(uptime).split('.')[0]}</code>
<b>🧠 RAM:</b> <code>{mem}%</code>
<b>⚡ CPU:</b> <code>{cpu}%</code>
<b>🛠 ОС:</b> <code>{os_version}</code>
"""

# Обработчик команды /start
@dp.message(Command("start"))
async def start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    await delete_last_menus(user_id)

    for mid in get_last_menu_ids(user_id):
        try:
            await bot.delete_message(user_id, mid)
        except Exception:
            pass

    if user_id == ADMIN_ID:
        info = get_server_info()
        msg = await message.answer(
            info + "\n<b>Главное меню администратора:</b>",
            reply_markup=create_main_menu(),
            parse_mode="HTML"
        )
        set_last_menu_id(user_id, msg.message_id)
        await state.set_state(VPNSetup.choosing_option)
        return

    if is_approved_user(user_id):
        save_user_id(user_id)
        client_name = get_profile_name(user_id)
        if not await client_exists("openvpn", client_name):
            result = await execute_script("1", client_name, "30")
            if result["returncode"] != 0:
                msg = await message.answer("❌ Ошибка при регистрации клиента. Свяжитесь с администратором.")
                set_last_menu_id(user_id, msg.message_id)
                return
        msg = await message.answer(
            f"Привет, <b>твой VPN-аккаунт активирован!</b>\n\n"
            f"💰 Ваш баланс: {get_user_balance(user_id):.2f} RUB\n"
            "Выбери действие ниже:",
            reply_markup=create_user_menu(client_name, user_id=user_id)
        )
        set_last_menu_id(user_id, msg.message_id)
        return

    if is_pending(user_id):
        msg = await message.answer("Ваша заявка на доступ уже на рассмотрении.")
        set_last_menu_id(user_id, msg.message_id)
        return

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Отправить заявку на доступ", callback_data="send_request")]
    ])
    msg = await message.answer(
        "У вас нет доступа к VPN. Чтобы получить доступ — отправьте заявку на одобрение администратором:", 
        reply_markup=markup
    )
    set_last_menu_id(user_id, msg.message_id)

# Обработчик отправки заявки
@dp.callback_query(lambda c: c.data == "send_request")
async def send_request(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if is_pending(user_id):
        await callback.answer("Ваша заявка уже на рассмотрении", show_alert=True)
        return
    add_pending(user_id, callback.from_user.username, callback.from_user.full_name)
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve_{user_id}")],
        [InlineKeyboardButton(text="✏️ Одобрить с изменением имени", callback_data=f"approve_rename_{user_id}")],
        [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{user_id}")]
    ])
    await safe_send_message(
        ADMIN_ID,
        f"🔔 <b>Новая заявка:</b>\nID: <code>{user_id}</code>\nUsername: @{callback.from_user.username or '-'}\nИмя: {callback.from_user.full_name or '-'}",
        reply_markup=markup,
        parse_mode="HTML"
    )
    await callback.message.edit_text("Заявка отправлена, ждите одобрения администратора.")
    await callback.answer("Заявка отправлена!", show_alert=True)

# Обработчик просмотра балансов всех пользователей
@dp.callback_query(lambda c: c.data == "view_all_balances")
async def view_all_balances(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав!", show_alert=True)
        return
    users = get_all_users_balances()
    if not users:
        await callback.message.edit_text("Нет зарегистрированных пользователей.")
        await callback.answer()
        return
    text = "💰 <b>Балансы пользователей:</b>\n\n"
    for user_id, profile_name, balance in users:
        text += f"ID: <code>{user_id}</code>, Имя: {profile_name}, Баланс: {balance:.2f} RUB\n"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()

# Обработчик кнопки "Пополнить баланс"
@dp.callback_query(lambda c: c.data == "top_up_balance")
async def top_up_balance(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await delete_last_menus(user_id)
    try:
        await callback.message.delete()
    except Exception:
        pass
    msg = await bot.send_message(
        user_id,
        "💸 Введите сумму для пополнения (в RUB, минимум 100):",
        reply_markup=cancel_markup
    )
    await state.set_state(PaymentStates.waiting_for_amount)
    await state.update_data(input_message_id=msg.message_id)
    await callback.answer()

# Обработчик ввода суммы пополнения
@dp.message(PaymentStates.waiting_for_amount)
async def process_payment_amount(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    input_msg_id = data.get("input_message_id")

    # Удаляем сообщение с запросом суммы
    if input_msg_id:
        try:
            await bot.delete_message(user_id, input_msg_id)
        except Exception:
            pass

    # Проверка отмены
    if message.text == "❌ Отмена":
        await state.clear()
        client_name = get_profile_name(user_id)
        await show_menu(
            user_id,
            f"Меню пользователя <b>{client_name}</b>:",
            create_user_menu(client_name, user_id=user_id)
        )
        return

    # Проверка корректности суммы
    try:
        amount = float(message.text.strip())
        if amount < 100:
            warn = await message.answer("❌ Минимальная сумма пополнения — 100 RUB.", reply_markup=cancel_markup)
            await asyncio.sleep(1.5)
            try:
                await warn.delete()
            except:
                pass
            msg = await bot.send_message(
                user_id,
                "💸 Введите сумму для пополнения (в RUB, минимум 100):",
                reply_markup=cancel_markup
            )
            await state.update_data(input_message_id=msg.message_id)
            return
    except ValueError:
        warn = await message.answer("❌ Введите корректную сумму (число).", reply_markup=cancel_markup)
        await asyncio.sleep(1.5)
        try:
            await warn.delete()
        except:
            pass
        msg = await bot.send_message(
            user_id,
            "💸 Введите сумму для пополнения (в RUB, минимум 100):",
            reply_markup=cancel_markup
        )
        await state.update_data(input_message_id=msg.message_id)
        return

    # Удаляем сообщение пользователя
    try:
        await message.delete()
    except:
        pass

    # Создаем платеж через ЮKassa
    try:
        payment = create_payment(user_id, amount)
        confirmation_url = payment.confirmation.confirmation_url
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить", url=confirmation_url)],
            [InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_payment")]
        ])
        msg = await bot.send_message(
            user_id,
            f"💸 Для пополнения баланса на {amount:.2f} RUB перейдите по ссылке для оплаты:",
            reply_markup=markup
        )
        await state.set_state(PaymentStates.waiting_for_payment_confirmation)
        await state.update_data(payment_id=payment.id, amount=amount, message_id=msg.message_id)
    except Exception as e:
        await bot.send_message(user_id, f"❌ Ошибка при создании платежа: {str(e)}")
        await state.clear()
        client_name = get_profile_name(user_id)
        await show_menu(
            user_id,
            f"Меню пользователя <b>{client_name}</b>:",
            create_user_menu(client_name, user_id=user_id)
        )

# Обработчик отмены платежа
@dp.callback_query(lambda c: c.data == "cancel_payment")
async def cancel_payment(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await delete_last_menus(user_id)
    await state.clear()
    client_name = get_profile_name(user_id)
    await show_menu(
        user_id,
        f"Меню пользователя <b>{client_name}</b>:",
        create_user_menu(client_name, user_id=user_id)
    )
    await callback.message.delete()
    await callback.answer("Платеж отменен")

# Обработчик проверки статуса платежа
@dp.callback_query(lambda c: c.data.startswith("check_payment_"))
async def check_payment(callback: types.CallbackQuery, state: FSMContext):
    payment_id = callback.data.split("_")[2]
    user_id = callback.from_user.id
    data = await state.get_data()
    amount = data.get("amount")
    
    try:
        status = await check_payment_status(payment_id)
        update_payment_status(payment_id, status)
        
        if status == "succeeded":
            update_user_balance(user_id, amount)
            await callback.message.edit_text(
                f"✅ Платеж на {amount:.2f} RUB успешно завершен!\nТекущий баланс: {get_user_balance(user_id):.2f} RUB",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ В меню", callback_data=f"back_to_user_menu_{get_profile_name(user_id)}")]
                ])
            )
            await notify_admin_payment(user_id, amount)
            await state.clear()
        elif status == "canceled":
            await callback.message.edit_text(
                "❌ Платеж был отменен.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ В меню", callback_data=f"back_to_user_menu_{get_profile_name(user_id)}")]
                ])
            )
            await state.clear()
        else:
            await callback.message.edit_text(
                f"⏳ Платеж находится в статусе: {status}. Пожалуйста, подождите.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Проверить снова", callback_data=f"check_payment_{payment_id}")]
                ])
            )
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка при проверке платежа: {str(e)}")
        await state.clear()
    await callback.answer()

# Обработчик возврата в меню пользователя
@dp.callback_query(lambda c: c.data.startswith("back_to_user_menu_"))
async def back_to_user_menu(callback: types.CallbackQuery):
    client_name = callback.data.split("_")[-1]
    user_id = callback.from_user.id
    await delete_last_menus(user_id)
    await show_menu(
        user_id,
        f"Меню пользователя <b>{client_name}</b>:",
        create_user_menu(client_name, user_id=user_id)
    )
    await callback.answer()

# Уведомление админа о пополнении
async def notify_admin_payment(user_id, amount):
    client_name = get_profile_name(user_id)
    try:
        await bot.send_message(
            ADMIN_ID,
            f"💸 Пополнение баланса\nПользователь: <code>{user_id}</code> ({client_name})\nСумма: {amount:.2f} RUB",
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка при отправке уведомления админу о пополнении: {e}")

# Обработчик кнопки "Показать баланс"
@dp.callback_query(lambda c: c.data == "show_balance")
async def show_balance(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    balance = get_user_balance(user_id)
    client_name = get_profile_name(user_id)
    await callback.message.edit_text(
        f"💰 Ваш баланс: {balance:.2f} RUB",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"back_to_user_menu_{client_name}")]
        ]),
        parse_mode="HTML"
    )
    await callback.answer()

# Функция для периодической проверки незавершенных платежей
async def check_pending_payments():
    while True:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT payment_id, user_id, amount FROM payments WHERE status IN ('pending', 'waiting_for_capture')")
        payments = cur.fetchall()
        conn.close()
        
        for payment_id, user_id, amount in payments:
            try:
                status = await check_payment_status(payment_id)
                update_payment_status(payment_id, status)
                if status == "succeeded":
                    update_user_balance(user_id, amount)
                    client_name = get_profile_name(user_id)
                    await safe_send_message(
                        user_id,
                        f"✅ Платеж на {amount:.2f} RUB успешно завершен!\nТекущий баланс: {get_user_balance(user_id):.2f} RUB",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="⬅️ В меню", callback_data=f"back_to_user_menu_{client_name}")]
                        ])
                    )
                    await notify_admin_payment(user_id, amount)
                elif status == "canceled":
                    await safe_send_message(
                        user_id,
                        "❌ Платеж был отменен.",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="⬅️ В меню", callback_data=f"back_to_user_menu_{get_profile_name(user_id)}")]
                        ])
                    )
            except Exception as e:
                print(f"Ошибка при проверке платежа {payment_id}: {e}")
        await asyncio.sleep(60)  # Проверять каждые 60 секунд

# Остальной код остается без изменений
# ... (все остальные функции и обработчики из исходного кода)

async def main():
    print("✅ Бот успешно запущен!")
    asyncio.create_task(check_pending_payments())  # Запускаем фоновую проверку платежей
    asyncio.create_task(notify_expiring_users())
    await set_bot_commands()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
