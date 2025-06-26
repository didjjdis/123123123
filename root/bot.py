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
import json
import glob
import time
import uuid
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, FSInputFile, BotCommand, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties
import subprocess
from datetime import datetime, timedelta, timezone
import psutil
import platform
import socket
import sqlite3
from dotenv import load_dotenv
from yookassa import Configuration, Payment
from db import init_db, get_profile_name, save_profile_name

# --- Настройка ЮKassa ---
load_dotenv()
YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID")
YOOKASSA_SECRET_KEY = os.getenv("YOOKASSA_SECRET_KEY")

if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET_KEY:
    raise RuntimeError("YOOKASSA_SHOP_ID или YOOKASSA_SECRET_KEY не заданы в .env")

Configuration.configure(YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY)

# --- Состояния ---
class Payment(StatesGroup):
    waiting_for_amount = State()

class SetEmoji(StatesGroup):
    waiting_for_emoji = State()

class RenameProfile(StatesGroup):
    waiting_for_new_name = State()
    waiting_for_rename_approve = State()

class SetEmojiState(StatesGroup):
    waiting_for_emoji = State()

class AdminAnnounce(StatesGroup):
    waiting_for_text = State()

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

# --- Инициализация базы данных ---
DB_PATH = "vpn.db"
def init_db_with_balances(db_path="vpn.db"):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            profile_name TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS balances (
            user_id INTEGER PRIMARY KEY,
            balance REAL DEFAULT 0.0
        )
    """)
    conn.commit()
    conn.close()

# --- Функции для работы с балансом ---
def get_balance(user_id, db_path="vpn.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT balance FROM balances WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else 0.0

def update_balance(user_id, amount, db_path="vpn.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO balances (user_id, balance) VALUES (?, ?)", 
                (user_id, amount))
    conn.commit()
    conn.close()

def get_all_balances(db_path="vpn.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT user_id, balance FROM balances")
    balances = cur.fetchall()
    conn.close()
    return balances

init_db_with_balances(DB_PATH)

# --- Настройки бота ---
cancel_markup = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="❌ Отмена")]],
    resize_keyboard=True,
    one_time_keyboard=True
)

USERS_FILE = "users.txt"
LAST_MENUS_FILE = "last_menus.json"
PENDING_FILE = "pending_users.json"
APPROVED_FILE = "approved_users.txt"
EMOJI_FILE = "user_emojis.json"
MAX_BOT_MENUS = 1
MAX_MENUS_PER_USER = 3
ITEMS_PER_PAGE = 5
AUTHORIZED_USERS = [int(os.getenv("ADMIN_ID"))]

FILEVPN_NAME = os.getenv("FILEVPN_NAME")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

if not FILEVPN_NAME or not BOT_TOKEN or not ADMIN_ID:
    raise RuntimeError("FILEVPN_NAME, BOT_TOKEN или ADMIN_ID не заданы в .env")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

BOT_DESCRIPTION = """
👴🕶️ БичиVPN — bi4i.ru
⚡ VPN-бот для своих:
— 🧑‍💻 Ебейший VPN который обходит только заблокированные сервисы
— 🕳️ Генерация конфигов OpenVPN прям в Боте
— 🧾 Статистика на хуй никому не нужная
— 🪣 МБ будет Vless позже)
Как получить VPN?
🪪 Жми /start, отправляй заявку, жди одобрения!
🟣https://bi4i.ru/install/ Инструкция по установке и подключению
"""

BOT_SHORT_DESCRIPTION = "👴🕶️ БичиVPN — приватный VPN за минуту! bi4i.ru"
BOT_ABOUT = "Бот для пользования услугами VPN от БичиVPN."

# --- Функции для работы с файлами и пользователями ---
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

def add_pending(user_id, username, fullname):
    pending = {}
    if os.path.exists(PENDING_FILE):
        with open(PENDING_FILE, "r") as f:
            pending = json.load(f)
    pending[str(user_id)] = {"username": username, "fullname": fullname}
    with open(PENDING_FILE, "w") as f:
        json.dump(pending, f)

def remove_pending(user_id):
    if not os.path.exists(PENDING_FILE):
        return
    with open(PENDING_FILE, "r") as f:
        pending = json.load(f)
    pending.pop(str(user_id), None)
    with open(PENDING_FILE, "w") as f:
        json.dump(pending, f)

def is_pending(user_id):
    if not os.path.exists(PENDING_FILE):
        return False
    try:
        with open(PENDING_FILE, "r") as f:
            pending = json.load(f)
    except Exception:
        pending = {}
    return str(user_id) in pending

def is_approved_user(user_id):
    user_id = str(user_id)
    if not os.path.exists(APPROVED_FILE):
        return False
    with open(APPROVED_FILE, "r") as f:
        approved = [line.strip() for line in f]
    return user_id in approved

def approve_user(user_id):
    user_id = str(user_id)
    if not is_approved_user(user_id):
        with open(APPROVED_FILE, "a") as f:
            f.write(user_id + "\n")

def set_user_emoji(user_id, emoji):
    data = {}
    if os.path.exists(EMOJI_FILE):
        with open(EMOJI_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    data[str(user_id)] = emoji
    with open(EMOJI_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

def get_user_emoji(user_id):
    if not os.path.exists(EMOJI_FILE):
        return ""
    with open(EMOJI_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get(str(user_id), "")

def get_server_info():
    ip = get_external_ip()
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

def get_external_ip():
    try:
        response = requests.get("https://api.ipify.org", timeout=10)
        if response.status_code == 200:
            return response.text
        return "IP не найден"
    except requests.RequestException as e:
        return f"Ошибка при запросе: {e}"
SERVER_IP = get_external_ip()

# --- Меню ---
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
        [InlineKeyboardButton(text="Просмотреть балансы", callback_data="view_balances")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def create_user_menu(client_name, back_callback="main_menu", is_admin=False, user_id=None):
    balance = get_balance(user_id) if user_id else 0.0
    keyboard = [
        [InlineKeyboardButton(text="🔐 OpenVPN", callback_data=f"select_openvpn_{client_name}")],
        [InlineKeyboardButton(text="🔗 WireGuard", callback_data=f"get_wg_{client_name}")],
        [InlineKeyboardButton(text="🌀 Amnezia", callback_data=f"get_amnezia_{client_name}")],
        [InlineKeyboardButton(text=f"💸 Пополнить баланс (Текущий: {balance:.2f} руб.)", 
                             callback_data=f"top_up_balance_{client_name}")]
    ]
    if is_admin:
        keyboard.append([InlineKeyboardButton(text="🗑 Удалить клиента", callback_data=f"delete_openvpn_{client_name}")])
        keyboard.append([InlineKeyboardButton(text="😀 Установить смайл", callback_data=f"set_emoji_{client_name}")])
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def create_server_manage_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Перезагрузить бота", callback_data="restart_bot")],
        [InlineKeyboardButton(text="Перезагрузить сервер", callback_data="reboot_server")],
        [InlineKeyboardButton(text="Вернуться", callback_data="main_menu")],
    ])

def create_wg_menu(client_name):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Обычный VPN", callback_data=f"info_wg_vpn_{client_name}"),
            InlineKeyboardButton(text="Antizapret (Рекомендую)", callback_data=f"info_wg_antizapret_{client_name}")
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"back_to_user_menu_{client_name}")]
    ])

def create_amnezia_menu(client_name):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Обычный VPN", callback_data=f"info_am_vpn_{client_name}"),
            InlineKeyboardButton(text="Antizapret (Рекомендую)", callback_data=f"info_am_antizapret_{client_name}")
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"back_to_user_menu_{client_name}")]
    ])

def create_openvpn_config_menu(client_name):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Обычный VPN", callback_data=f"download_openvpn_vpn_{client_name}")],
        [InlineKeyboardButton(text="✅ Antizapret (Рекомендуется)", callback_data=f"download_openvpn_antizapret_{client_name}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"cancel_openvpn_config_{client_name}")]
    ])

def create_wireguard_config_menu(client_name):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Обычный VPN", callback_data=f"send_wg_vpn_wg_{client_name}")],
        [InlineKeyboardButton(text="✅ Antizapret", callback_data=f"send_wg_antizapret_wg_{client_name}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"back_to_user_menu_{client_name}")]
    ])

def create_confirmation_keyboard(client_name, vpn_type):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да", callback_data=f"confirm_{vpn_type}_{client_name}")],
        [InlineKeyboardButton(text="❌ Нет", callback_data="cancel_delete")]
    ])

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

# --- Вспомогательные функции ---
async def safe_send_message(chat_id, text, **kwargs):
    try:
        await bot.send_message(chat_id, text, **kwargs)
    except Exception as e:
        print(f"[Ошибка отправки сообщения] chat_id={chat_id}: {e}")

async def switch_menu(callback: types.CallbackQuery, text: str, reply_markup=None, parse_mode="HTML"):
    try:
        await callback.message.delete()
    except Exception:
        pass
    await bot.send_message(callback.from_user.id, text, reply_markup=reply_markup, parse_mode=parse_mode)

async def show_menu(chat_id, text, reply_markup):
    msg = await bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode="HTML")
    set_last_menu_id(chat_id, msg.message_id)
    return msg

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

def set_last_menu_id(user_id, msg_id):
    data = {}
    if os.path.exists(LAST_MENUS_FILE):
        with open(LAST_MENUS_FILE, "r") as f:
            data = json.load(f)
    user_id = str(user_id)
    data[user_id] = [msg_id]
    with open(LAST_MENUS_FILE, "w") as f:
        json.dump(data, f)

async def set_bot_commands():
    async with Bot(token=BOT_TOKEN) as bot:
        commands = [
            BotCommand(command="start", description="Запустить бота"),
            BotCommand(command="announce", description="Сделать объявление (для админа)")
        ]
        await bot.set_my_commands(commands)

async def update_bot_description():
    async with Bot(token=BOT_TOKEN) as bot:
        await bot.set_my_description(BOT_DESCRIPTION, language_code="ru")

async def update_bot_about():
    async with Bot(token=BOT_TOKEN) as bot:
        await bot.set_my_short_description(BOT_ABOUT, language_code="ru")

# --- VPN-функции ---
async def client_exists(vpn_type: str, client_name: str) -> bool:
    clients = await get_clients(vpn_type)
    return client_name in clients

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

async def execute_script(option, client_name=None, days=None):
    cmd = ["/root/antizapret/client.sh", option]
    if client_name:
        cmd.append(client_name)
    if days:
        cmd.append(days)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return {"returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
    except Exception as e:
        return {"returncode": 1, "stdout": "", "stderr": str(e)}

async def send_config(chat_id: int, client_name: str, option: str) -> bool:
    try:
        files_found = []
        if option == "4":
            base_dir = "/root/antizapret/client/amneziawg/"
            ext = ".conf"
            prefix = f"amneziawg-{client_name}-"
            for root, _, files in os.walk(base_dir):
                for file in files:
                    if file.startswith(prefix) and file.endswith(ext):
                        files_found.append(os.path.join(root, file))
        else:
            base_dir = "/root/antizapret/client/openvpn/"
            ext = ".ovpn"
            prefix_vpn = f"vpn-{client_name}-"
            prefix_antizapret = f"antizapret-{client_name}-"
            for root, _, files in os.walk(base_dir):
                for file in files:
                    if (
                        (file.startswith(prefix_vpn) or file.startswith(prefix_antizapret))
                        and file.endswith(ext)
                    ):
                        files_found.append(os.path.join(root, file))
        for file_path in files_found:
            await bot.send_document(
                chat_id, FSInputFile(file_path), caption=f"🔐 {os.path.basename(file_path)}"
            )
        return bool(files_found)
    except Exception as e:
        print(f"Ошибка отправки конфигураций: {e}")
        return False

async def send_backup(chat_id: int) -> bool:
    paths_to_check = [
        f"/root/antizapret/backup-{SERVER_IP}.tar.gz",
        "/root/antizapret/backup.tar.gz",
    ]
    for backup_path in paths_to_check:
        try:
            if os.path.exists(backup_path):
                await bot.send_document(
                    chat_id=chat_id,
                    document=FSInputFile(backup_path),
                    caption="📦 Бэкап клиентов",
                )
                return True
        except Exception as e:
            print(f"Ошибка отправки бэкапа ({backup_path}): {e}")
            return False
    return False

def get_openvpn_filename(client_name, config_type):
    if config_type == "vpn":
        return f"{FILEVPN_NAME} - Обычный VPN - {client_name}.ovpn"
    elif config_type == "antizapret":
        return f"{FILEVPN_NAME} - {client_name}.ovpn"

async def cleanup_openvpn_files(client_name: str):
    clean_name = client_name.replace("antizapret-", "").replace("vpn-", "")
    dirs_to_check = [
        "/root/antizapret/client/openvpn/antizapret/",
        "/root/antizapret/client/openvpn/antizapret-tcp/",
        "/root/antizapret/client/openvpn/antizapret-udp/",
        "/root/antizapret/client/openvpn/vpn/",
        "/root/antizapret/client/openvpn/vpn-tcp/",
        "/root/antizapret/client/openvpn/vpn-udp/",
    ]
    deleted_files = []
    for dir_path in dirs_to_check:
        if not os.path.exists(dir_path):
            continue
        for filename in os.listdir(dir_path):
            if clean_name in filename:
                try:
                    file_path = os.path.join(dir_path, filename)
                    os.remove(file_path)
                    deleted_files.append(file_path)
                except Exception as e:
                    print(f"Ошибка удаления {file_path}: {e}")
    return deleted_files

def get_user_id_by_name(client_name):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE profile_name=?", (client_name,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None

def get_cert_expiry_info(client_name):
    try:
        result = subprocess.run(
            ["/etc/openvpn/easyrsa3/easyrsa", "--batch", "show-cert", client_name],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            for line in result.stdout.split("\n"):
                if "expire" in line.lower():
                    expiry_str = line.split(":")[1].strip()
                    expiry_date = datetime.strptime(expiry_str, "%Y%m%d%H%M%SZ")
                    days_left = (expiry_date - datetime.now()).days
                    return {"days_left": days_left}
        return None
    except:
        return None

def get_online_users_from_log():
    online = {}
    try:
        with open("/var/log/openvpn/status.log", "r") as f:
            for line in f:
                if "CLIENT_LIST" in line:
                    parts = line.split(",")
                    if len(parts) > 3:
                        client_name = parts[1]
                        online[client_name] = "OpenVPN"
    except Exception as e:
        print(f"[ERROR] get_online_users_from_log: {e}")
    return online

def get_online_wg_peers():
    peers = {}
    try:
        result = subprocess.run(["wg", "show", "wg0", "latest-handshakes"], 
                              capture_output=True, text=True)
        for line in result.stdout.split("\n"):
            if line.strip():
                pubkey, timestamp = line.split()
                if int(timestamp) > 0:
                    for base in ["/root/antizapret/client/wireguard", 
                               "/root/antizapret/client/amneziawg"]:
                        for root, _, files in os.walk(base):
                            for fname in files:
                                if fname.endswith(".conf"):
                                    path = os.path.join(root, fname)
                                    with open(path, "r") as cf:
                                        for cf_line in cf:
                                            if cf_line.strip().startswith("PublicKey") and pubkey in cf_line:
                                                client_name = fname.split("-")[1].split(".")[0]
                                                proto = "Amnezia" if "amneziawg" in path else "WG"
                                                peers[client_name] = proto
                                                break
                            if client_name in peers:
                                break
                        if client_name in peers:
                            break
    except Exception as e:
        print(f"[ERROR] wg show: {e}")
    return peers

async def notify_admin_download(user_id, username, filename, vpn_type):
    await safe_send_message(
        ADMIN_ID,
        f"📥 Пользователь <code>{user_id}</code> (@{username}) скачал конфиг:\n"
        f"<b>{filename}</b> ({vpn_type})",
        parse_mode="HTML"
    )

# --- Обработчики ---
@dp.callback_query(lambda c: c.from_user.id != ADMIN_ID
                          and c.data != "send_request"
                          and not is_approved_user(c.from_user.id)
                          and not is_pending(c.from_user.id))
async def _deny_unapproved_callback(callback: types.CallbackQuery):
    await callback.answer(
        "Йоу, у тебя нету доступа к сервесу. Чтобы отправить заявку, пропиши /start !",
        show_alert=True
    )

@dp.message(Command("start"))
async def start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    await delete_last_menus(user_id)
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
            f"Текущий баланс: <b>{get_balance(user_id):.2f} руб.</b>\n"
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
        reply_markup=markup)
    set_last_menu_id(user_id, msg.message_id)

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

@dp.callback_query(lambda c: c.data == "main_menu")
async def handle_main_menu(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await delete_last_menus(user_id)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await state.clear()
    stats = get_server_info()
    await show_menu(
        user_id,
        stats + "\n<b>Главное меню:</b>",
        create_main_menu()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "add_del_menu")
async def add_del_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    await show_menu(
        user_id,
        "Выберите действие:",
        InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить пользователя", callback_data="add_user")],
            [InlineKeyboardButton(text="➖ Удалить пользователя", callback_data="del_user")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
        ])
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "add_user")
async def add_user_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав!", show_alert=True)
        return
    await delete_last_menus(callback.from_user.id)
    msg = await bot.send_message(
        callback.from_user.id,
        "✏️ Введите Telegram-ID нового пользователя (только цифры).",
        reply_markup=cancel_markup
    )
    await state.update_data(manual_add_msg_id=msg.message_id)
    await state.set_state(VPNSetup.entering_user_id)
    await callback.answer()

@dp.message(VPNSetup.entering_user_id)
async def process_manual_user_id(message: types.Message, state: FSMContext):
    user_id_text = message.text.strip()
    data = await state.get_data()
    prev_msg_id = data.get("manual_add_msg_id")
    if prev_msg_id:
        try:
            await bot.delete_message(message.chat.id, prev_msg_id)
        except Exception:
            pass
    if user_id_text in ("❌", "❌ Отмена", "отмена", "Отмена"):
        await state.clear()
        await delete_last_menus(message.from_user.id)
        stats = get_server_info()
        await show_menu(message.from_user.id, stats + "\n<b>Главное меню:</b>", create_main_menu())
        return
    if not user_id_text.isdigit():
        warn = await message.answer("❌ Некорректный ID. Нужно ввести только цифры.", reply_markup=cancel_markup)
        await asyncio.sleep(1.5)
        try: await warn.delete()
        except: pass
        msg = await bot.send_message(
            message.chat.id,
            "✏️ Пожалуйста, введите корректный Telegram-ID (только цифры):",
            reply_markup=cancel_markup
        )
        await state.update_data(manual_add_msg_id=msg.message_id)
        return
    manual_user_id = int(user_id_text)
    await state.update_data(manual_user_id=manual_user_id)
    try:
        await message.delete()
    except:
        pass
    msg2 = await bot.send_message(
        message.chat.id,
        f"✏️ Теперь введите <b>имя профиля</b> для этого пользователя (латиница, цифры, _ или -), длиною до 32 символов.\n\n"
        f"Имя профиля понадобится для OpenVPN/WG/Amnezia (название сертификата).",
        parse_mode="HTML",
        reply_markup=cancel_markup
    )
    await state.update_data(manual_add_msg_id=msg2.message_id)
    await state.set_state(VPNSetup.entering_client_name_manual)

@dp.message(VPNSetup.entering_client_name_manual)
async def process_manual_client_name(message: types.Message, state: FSMContext):
    client_name = message.text.strip()
    data = await state.get_data()
    prev_msg_id = data.get("manual_add_msg_id")
    manual_user_id = data.get("manual_user_id")
    if prev_msg_id:
        try:
            await bot.delete_message(message.chat.id, prev_msg_id)
        except Exception:
            pass
    if client_name == "❌" or client_name.lower() == "отмена":
        await state.clear()
        await delete_last_menus(message.from_user.id)
        stats = get_server_info()
        await show_menu(
            message.from_user.id,
            stats + "\n<b>Главное меню:</b>",
            create_main_menu()
        )
        return
    if not re.match(r"^[a-zA-Z0-9_-]{1,32}$", client_name):
        warn = await message.answer(
            "❌ Некорректное имя профиля! Используйте латиницу, цифры, _ или -. Не больше 32 символов.",
            reply_markup=cancel_markup
        )
        await asyncio.sleep(1.5)
        try:
            await warn.delete()
        except:
            pass
        msg2 = await bot.send_message(
            message.chat.id,
            "✏️ Введите корректное имя профиля (латиница, цифры, _ или -):",
            reply_markup=cancel_markup
        )
        await state.update_data(manual_add_msg_id=msg2.message_id)
        return
    result = await execute_script("1", client_name, "30")
    if result["returncode"] != 0:
        await message.answer(
            f"❌ Ошибка при создании профиля <code>{client_name}</code>: {result['stderr']}",
            parse_mode="HTML"
        )
        await state.clear()
        return
    save_profile_name(manual_user_id, client_name)
    approve_user(manual_user_id)
    save_user_id(manual_user_id)
    try:
        await safe_send_message(
            manual_user_id,
            f"✅ Ваша учётная запись VPN <b>{client_name}</b> создана администратором!\n\n"
            "Теперь вы можете писать боту и сразу получать конфиг.",
            parse_mode="HTML",
            reply_markup=create_user_menu(client_name, user_id=manual_user_id)
        )
    except Exception:
        pass
    temp = await message.answer(
        "✅ Клиент успешно создан и подтверждён сразу! Пользователь может зайти в бот и сразу получить конфиги."
    )
    await asyncio.sleep(1)
    try:
        await temp.delete()
    except Exception:
        pass
    stats = get_server_info()
    await show_menu(
        message.from_user.id,
        stats + "\n<b>Главное меню:</b>",
        create_main_menu()
    )
    await state.clear()

@dp.callback_query(lambda c: c.data == "users_menu")
async def users_menu(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав!", show_alert=True)
        return
    await show_users_tab(callback.from_user.id, "users_tab_all")
    await callback.answer()

async def show_users_tab(chat_id: int, tab: str):
    raw_clients = await get_clients("openvpn")
    all_clients = [c for c in raw_clients if c != "antizapret-client"]
    open_online = set(get_online_users_from_log().keys())
    wg_online = set(get_online_wg_peers().keys())
    online_all = open_online | wg_online
    if tab == "users_tab_all":
        clients = all_clients
        header = "👥 <b>Все пользователи:</b>"
    elif tab == "users_tab_online":
        clients = [c for c in all_clients if c in online_all]
        header = "🟢 <b>Сейчас онлайн:</b>"
    else:
        clients = []
        for c in all_clients:
            uid = get_user_id_by_name(c)
            info = get_cert_expiry_info(c) if uid else None
            if info and 0 <= info["days_left"] <= 7:
                clients.append(c)
        header = "⏳ <b>Истекают (≤7д):</b>"
    rows = []
    for c in clients:
        uid = get_user_id_by_name(c)
        emoji = get_user_emoji(uid) if uid else ""
        if tab == "users_tab_expiring":
            days = get_cert_expiry_info(c)["days_left"]
            status = f"⏳{days}д"
        else:
            status = "🟢" if c in online_all else "🔴"
        label = f"{emoji+' ' if emoji else ''}{status} {c}"
        cb = f"manage_userid_{uid}" if uid else f"manage_user_{c}"
        rows.append([InlineKeyboardButton(text=label, callback_data=cb)])
    tab_row = make_users_tab_keyboard(tab).inline_keyboard[0]
    rows.append(tab_row)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")])
    markup = InlineKeyboardMarkup(inline_keyboard=rows)
    await show_menu(chat_id, header, markup)

@dp.callback_query(lambda c: c.data in {"users_tab_all","users_tab_online","users_tab_expiring"})
async def on_users_tab(callback: types.CallbackQuery):
    await show_users_tab(callback.from_user.id, callback.data)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("manage_userid_") or c.data.startswith("manage_user_"))
async def manage_user(callback: types.CallbackQuery):
    client_name = callback.data.split("_")[-1]
    user_id = callback.from_user.id
    await delete_last_menus(user_id)
    try:
        await callback.message.delete()
    except:
        pass
    await show_menu(
        user_id,
        f"Управление клиентом <b>{client_name}</b>:",
        create_user_menu(client_name, back_callback="users_menu", is_admin=(user_id == ADMIN_ID), user_id=user_id)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("set_emoji_"))
async def set_emoji_start(callback: types.CallbackQuery, state: FSMContext):
    client_name = callback.data[len("set_emoji_"):]
    user_id = callback.from_user.id
    target_user_id = get_user_id_by_name(client_name)
    if not target_user_id:
        await callback.answer("Пользователь не найден!", show_alert=True)
        return
    await state.set_state(SetEmojiState.waiting_for_emoji)
    await state.update_data(target_user_id=target_user_id, client_name=client_name)
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_set_emoji")]
        ]
    )
    msg = await bot.send_message(
        user_id,
        "Введи смайл (эмодзи) для этого пользователя, или отправь ❌ чтобы убрать смайл.",
        reply_markup=markup
    )
    await state.update_data(input_message_id=msg.message_id)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "cancel_set_emoji")
async def cancel_set_emoji(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    msg_id = data.get("input_message_id")
    client_name = data.get("client_name")
    try:
        await callback.bot.delete_message(callback.from_user.id, msg_id)
    except:
        pass
    await callback.answer("Отменено")
    await state.clear()
    await show_menu(
        callback.from_user.id,
        f"Меню пользователя <b>{client_name}</b>:",
        create_user_menu(client_name, back_callback="users_menu", is_admin=True, user_id=get_user_id_by_name(client_name))
    )

@dp.message(SetEmojiState.waiting_for_emoji)
async def set_emoji_process(message: types.Message, state: FSMContext):
    data = await state.get_data()
    target_user_id = data.get("target_user_id")
    client_name = data.get("client_name")
    input_msg_id = data.get("input_message_id")
    try:
        await message.bot.delete_message(message.from_user.id, input_msg_id)
    except:
        pass
    emoji = message.text.strip()
    if emoji == "❌":
        set_user_emoji(target_user_id, "")
        text = "Смайл удалён"
    else:
        set_user_emoji(target_user_id, emoji)
        text = f"Установлен смайл: {emoji}"
    info_msg = await message.answer(text)
    await asyncio.sleep(2)
    try:
        await info_msg.delete()
    except:
        pass
    await show_menu(
        message.from_user.id,
        f"Меню пользователя <b>{client_name}</b>:",
        create_user_menu(client_name, back_callback="users_menu", is_admin=(message.from_user.id == ADMIN_ID), 
                        user_id=target_user_id)
    )
    await state.clear()

@dp.callback_query(lambda c: c.data == "server_manage_menu")
async def server_manage_menu(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет доступа!", show_alert=True)
        return
    await callback.message.edit_text(
        "🛠 <b>Управление сервером:</b>", 
        reply_markup=create_server_manage_menu(),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "restart_bot")
async def handle_bot_restart(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа!", show_alert=True)
        return
    msg = await callback.message.edit_text("♻️ Перезапускаю бота через systemd...")
    await callback.answer()
    await asyncio.sleep(1)
    await msg.delete()
    await bot.send_message(
        callback.from_user.id,
        f"{get_server_info()}\n<b>👨‍💻 Главное меню:</b>",
        reply_markup=create_main_menu(),
        parse_mode="HTML"
    )
    os.system("systemctl restart vpnbot.service")

@dp.callback_query(lambda c: c.data == "reboot_server")
async def handle_reboot(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Нет доступа!", show_alert=True)
        return
    msg = await callback.message.edit_text("🔁 Сервер перезагружается...")
    await callback.answer()
    await asyncio.sleep(1)
    await msg.delete()
    await bot.send_message(
        callback.from_user.id,
        f"{get_server_info()}\n<b>👨‍💻 Главное меню:</b>",
        reply_markup=create_main_menu(),
        parse_mode="HTML"
    )
    os.system("reboot")

@dp.callback_query(lambda c: c.data == "admin_pending_list")
async def show_pending_list(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав!", show_alert=True)
        return
    if not os.path.exists(PENDING_FILE):
        await callback.message.delete()
        msg = await bot.send_message(callback.from_user.id, "Нет заявок.")
        await asyncio.sleep(1)
        try:
            await bot.delete_message(callback.from_user.id, msg.message_id)
        except Exception:
            pass
        stats = get_server_info()
        menu = await bot.send_message(
            callback.from_user.id,
            stats + "\n<b>Главное меню:</b>",
            reply_markup=create_main_menu(),
            parse_mode="HTML"
        )
        set_last_menu_id(callback.from_user.id, menu.message_id)
        return
    with open(PENDING_FILE) as f:
        pending = json.load(f)
    if not pending:
        await callback.message.delete()
        msg = await bot.send_message(callback.from_user.id, "Нет заявок.")
        await asyncio.sleep(1)
        try:
            await bot.delete_message(callback.from_user.id, msg.message_id)
        except Exception:
            pass
        stats = get_server_info()
        menu = await bot.send_message(
            callback.from_user.id,
            stats + "\n<b>Главное меню:</b>",
            reply_markup=create_main_menu(),
            parse_mode="HTML"
        )
        set_last_menu_id(callback.from_user.id, menu.message_id)
        return
    text = "📋 <b>Заявки на одобрение:</b>\n"
    keyboard = []
    for uid, info in pending.items():
        username = info.get("username") or "-"
        fullname = info.get("fullname") or "-"
        text += f"\nID: <code>{uid}</code> @{username}\nИмя: {fullname}\n"
        keyboard.append([
            InlineKeyboardButton(text="Принять", callback_data=f"approve_{uid}"),
            InlineKeyboardButton(text="Принять, но изменить имя", callback_data=f"approve_rename_{uid}"),
            InlineKeyboardButton(text="Отклонить", callback_data=f"reject_{uid}"),
        ])
    keyboard.append([InlineKeyboardButton(text="Вернуться", callback_data="main_menu")])
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    await callback.message.edit_text(text, reply_markup=markup, parse_mode="HTML")

@dp.callback_query(lambda c: c.data.startswith("approve_") or c.data.startswith("reject_"))
async def process_application(callback: types.CallbackQuery, state: FSMContext):
    action, user_id = callback.data.split("_", 1)
    user_id = int(user_id)
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав!", show_alert=True)
        return
    if action == "approve":
        user_obj = await bot.get_chat(user_id)
        client_name = user_obj.username or f"user{user_id}"
        client_name = str(client_name)[:32]
        result = await execute_script("1", client_name, "30")
        if result["returncode"] == 0:
            save_profile_name(user_id, client_name)
            approve_user(user_id)
            remove_pending(user_id)
            save_user_id(user_id)
            try:
                await callback.message.delete()
            except Exception:
                pass
            await safe_send_message(
                user_id,
                f"✅ Ваша заявка одобрена!\n"
                f"Имя профиля: <b>{client_name}</b>\nТеперь вам доступны функции VPN.",
                parse_mode="HTML",
                reply_markup=create_user_menu(client_name)
            )
            stats = get_server_info()
            await show_menu(
                callback.from_user.id,
                stats + "\n<b>Главное меню:</b>",
                create_main_menu()
            )
        else:
            await callback.message.edit_text(f"❌ Ошибка: {result['stderr']}")
        await callback.answer()
        return
    else:
        remove_pending(user_id)
        try:
            await callback.message.delete()
        except Exception:
            pass
        await safe_send_message(user_id, "❌ Ваша заявка отклонена. Обратитесь к администратору.")
        await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("approve_rename_"))
async def process_application_rename(callback: types.CallbackQuery, state: FSMContext):
    user_id = int(callback.data.split("_", 2)[-1])
    await state.update_data(approve_user_id=user_id, pending_menu_msg_id=callback.message.message_id)
    try:
        await callback.message.delete()
    except Exception:
        pass
    msg = await bot.send_message(
        callback.from_user.id,
        f"Введи новое имя для пользователя (id <code>{user_id}</code>):",
        parse_mode="HTML"
    )
    await state.set_state(RenameProfile.waiting_for_rename_approve)
    await state.update_data(rename_prompt_id=msg.message_id)
    await callback.answer()

@dp.message(RenameProfile.waiting_for_rename_approve)
async def process_rename_new_name(message: types.Message, state: FSMContext):
    new_name = message.text.strip()
    data = await state.get_data()
    rename_prompt_id = data.get("rename_prompt_id")
    pending_menu_msg_id = data.get("pending_menu_msg_id")
    if rename_prompt_id:
        try:
            await bot.delete_message(message.chat.id, rename_prompt_id)
        except Exception:
            pass
    try:
        await bot.delete_message(message.chat.id, message.message_id)
    except Exception:
        pass
    user_id = data.get("approve_user_id")
    if not user_id:
        await message.answer("Ошибка: не найден id пользователя.")
        await state.clear()
        return
    if not re.match(r"^[a-zA-Z0-9_-]{1,32}$", new_name):
        await safe_send_message(
            message.chat.id, 
            "❌ Некорректное имя! Используй только буквы, цифры, _ и -."
        )
        await state.clear()
        return
    result = await execute_script("1", new_name, "30")
    if result["returncode"] == 0:
        save_profile_name(user_id, new_name)
        approve_user(user_id)
        remove_pending(user_id)
        save_user_id(user_id)
        msg = await safe_send_message(
            user_id,
            f"✅ Ваша заявка одобрена!\nИмя профиля: <b>{new_name}</b>\nТеперь вам доступны функции VPN.",
            parse_mode="HTML",
            reply_markup=create_user_menu(new_name)
        )
        try:
            await bot.delete_message(message.chat.id, msg.message_id)
        except Exception:
            pass
        stats = get_server_info()
        menu = await show_menu(
            message.chat.id,
            stats + "\n<b>Главное меню:</b>",
            create_main_menu()
        )
        set_last_menu_id(message.chat.id, menu.message_id)
    else:
        await safe_send_message(
            message.chat.id,
            f"❌ Ошибка: {result['stderr']}"
        )
    await state.clear()

@dp.callback_query(lambda c: c.data.startswith("top_up_balance_"))
async def top_up_balance_start(callback: types.CallbackQuery, state: FSMContext):
    client_name = callback.data[len("top_up_balance_"):]
    user_id = callback.from_user.id
    await state.update_data(client_name=client_name)
    await delete_last_menus(user_id)
    try:
        await callback.message.delete()
    except Exception:
        pass
    msg = await bot.send_message(
        user_id,
        "💸 Введите сумму для пополнения баланса (в рублях, минимум 100, например, 100):",
        reply_markup=cancel_markup
    )
    await state.set_state(Payment.waiting_for_amount)
    await state.update_data(prompt_msg_id=msg.message_id)
    await callback.answer()

@dp.message(Payment.waiting_for_amount)
async def process_payment_amount(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    prompt_msg_id = data.get("prompt_msg_id")
    client_name = data.get("client_name")
    if prompt_msg_id:
        try:
            await bot.delete_message(user_id, prompt_msg_id)
        except Exception:
            pass
    if message.text in ("❌", "❌ Отмена", "отмена", "Отмена"):
        await state.clear()
        await delete_last_menus(user_id)
        await show_menu(
            user_id,
            f"Меню пользователя <b>{client_name}</b>:",
            create_user_menu(client_name, back_callback="users_menu" if user_id == ADMIN_ID else "main_menu", 
                            is_admin=(user_id == ADMIN_ID), user_id=user_id)
        )
        return
    try:
        amount = float(message.text.strip())
        if amount < 100:
            raise ValueError("Сумма должна быть не менее 100 рублей")
    except ValueError:
        warn = await message.answer(
            "❌ Некорректная сумма! Введите число не менее 100 (например, 100).",
            reply_markup=cancel_markup
        )
        await asyncio.sleep(1.5)
        try:
            await warn.delete()
        except:
            pass
        msg = await bot.send_message(
            user_id,
            "💸 Введите сумму для пополнения баланса (в рублях, минимум 100, например, 100):",
            reply_markup=cancel_markup
        )
        await state.update_data(prompt_msg_id=msg.message_id)
        return
    try:
        await message.delete()
    except:
        pass
    # Создание платежа через ЮKassa
    payment_id = str(uuid.uuid4())
    payment = Payment.create({
        "amount": {
            "value": f"{amount:.2f}",
            "currency": "RUB"
        },
        "confirmation": {
            "type": "redirect",
            "return_url": "https://your-bot-url/return"
        },
        "capture": True,
        "description": f"Пополнение баланса для user_id {user_id}",
        "metadata": {
            "user_id": str(user_id),
            "payment_id": payment_id
        }
    })
    payment_url = payment.confirmation.confirmation_url

    # В продакшене: обновляйте баланс только после вебхука от ЮKassa
    current_balance = get_balance(user_id)
    new_balance = current_balance + amount
    update_balance(user_id, new_balance)

    await bot.send_message(
        user_id,
        f"💳 Для пополнения баланса на {amount:.2f} руб. перейдите по ссылке:\n{payment_url}\n\n"
        f"Текущий баланс (временно обновлен): {new_balance:.2f} руб.",
        parse_mode="HTML"
    )

    await safe_send_message(
        ADMIN_ID,
        f"💸 Пользователь <code>{user_id}</code> (@{message.from_user.username or '-'}) "
        f"инициировал пополнение баланса на {amount:.2f} руб.\n"
        f"ID платежа: <code>{payment_id}</code>",
        parse_mode="HTML"
    )

    await show_menu(
        user_id,
        f"Меню пользователя <b>{client_name}</b>:",
        create_user_menu(client_name, back_callback="users_menu" if user_id == ADMIN_ID else "main_menu", 
                        is_admin=(user_id == ADMIN_ID), user_id=user_id)
    )
    await state.clear()

@dp.callback_query(lambda c: c.data == "view_balances")
async def view_balances(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав!", show_alert=True)
        return
    balances = get_all_balances()
    if not balances:
        await callback.message.edit_text("Нет пользователей с балансом.")
        await callback.answer()
        return
    text = "💰 <b>Балансы пользователей:</b>\n"
    for user_id, balance in balances:
        profile_name = get_profile_name(user_id) or f"user{user_id}"
        text += f"\nID: <code>{user_id}</code>, Профиль: <b>{profile_name}</b>, Баланс: {balance:.2f} руб.\n"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("get_wg_"))
async def get_wg_menu(callback: types.CallbackQuery):
    client_name = callback.data[len("get_wg_"):]
    await delete_last_menus(callback.from_user.id)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await bot.send_message(
        callback.from_user.id,
        "Выберите тип WireGuard-конфига:",
        reply_markup=create_wg_menu(client_name)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("get_amnezia_"))
async def get_amnezia_menu(callback: types.CallbackQuery):
    client_name = callback.data[len("get_amnezia_"):]
    await delete_last_menus(callback.from_user.id)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await bot.send_message(
        callback.from_user.id,
        "Выберите тип Amnezia-конфига:",
        reply_markup=create_amnezia_menu(client_name)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("info_wg_vpn_"))
async def show_info_wg_vpn(callback: types.CallbackQuery):
    client_name = callback.data.replace("info_wg_vpn_", "")
    text = (
        "🛡 <b>Как подключиться к обычному VPN (WireGuard):</b>\n\n"
        "📱 Поддерживаемые устройства:\n"
        "• Android 📱\n"
        "• iOS 📲\n"
        "• Windows 💻\n"
        "• macOS 🍏\n"
        "• Linux 🐧\n\n"
        "📖 <b>Инструкция по установке:</b>\n"
        "👉 <a href='https://bi4i.ru/install-wg/'>bi4i.ru/install-wg</a>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Скачать конфиг", callback_data=f"send_wg_vpn_wg_{client_name}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"get_wg_{client_name}")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("info_wg_antizapret_"))
async def show_info_wg_antizapret(callback: types.CallbackQuery):
    client_name = callback.data.replace("info_wg_antizapret_", "")
    text = (
        "🛡 <b>WireGuard + Antizapret:</b>\n\n"
        "📱 Поддерживаемые устройства:\n"
        "• Android 📱\n"
        "• iOS 📲\n"
        "• Windows 💻\n"
        "• macOS 🍏\n"
        "• Linux 🐧\n\n"
        "🚫 Использует DNS и маршруты обхода блокировок.\n\n"
        "📖 <b>Инструкция по установке:</b>\n"
        "👉 <a href='https://bi4i.ru/install-wg/'>bi4i.ru/install-wg</a>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Скачать конфиг", callback_data=f"send_wg_antizapret_wg_{client_name}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"get_wg_{client_name}")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("info_am_vpn_"))
async def show_info_am_vpn(callback: types.CallbackQuery):
    client_name = callback.data.replace("info_am_vpn_", "")
    text = (
        "🌀 <b>Amnezia VPN:</b>\n\n"
        "📱 Поддерживаемые устройства:\n"
        "• Android 📱\n"
        "• Windows 💻\n"
        "• macOS 🍏\n\n"
        "🧾 Простой запуск через приложение Amnezia.\n\n"
        "📖 <b>Инструкция по установке:</b>\n"
        "👉 <a href='https://bi4i.ru/install-amnezia/'>bi4i.ru/install-amnezia</a>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Скачать конфиг", callback_data=f"download_am_vpn_{client_name}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"get_amnezia_{client_name}")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("info_am_antizapret_"))
async def show_info_am_antizapret(callback: types.CallbackQuery):
    client_name = callback.data.replace("info_am_antizapret_", "")
    text = (
        "🌀 <b>Amnezia VPN + Antizapret:</b>\n\n"
        "📱 Поддерживаемые устройства:\n"
        "• Android 📱\n"
        "• Windows 💻\n"
        "• macOS 🍏\n\n"
        "🚫 Использует обход блокировок через Antizapret.\n\n"
        "📖 <b>Инструкция по установке:</b>\n"
        "👉 <a href='https://bi4i.ru/install-amnezia/'>bi4i.ru/install-amnezia</a>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Скачать конфиг", callback_data=f"download_am_antizapret_{client_name}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"get_amnezia_{client_name}")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("download_openvpn_"))
async def download_openvpn_config(callback: types.CallbackQuery):
    parts = callback.data.split("_", 3)
    _, _, config_type, client_name = parts
    user_id = callback.from_user.id
    username = callback.from_user.username or "Без username"
    await delete_last_menus(user_id)
    try:
        await callback.message.delete()
    except Exception:
        pass
    if config_type == "vpn":
        file_name = f"{FILEVPN_NAME} - Обычный VPN - {client_name}.ovpn"
        base_path = "/root/antizapret/client/openvpn/vpn/"
    else:
        file_name = f"{FILEVPN_NAME} - {client_name}.ovpn"
        base_path = "/root/antizapret/client/openvpn/antizapret/"
    file_path = os.path.join(base_path, file_name)
    if os.path.exists(file_path):
        await bot.send_document(
            user_id,
            FSInputFile(file_path),
            caption=f"🔐 {os.path.basename(file_path)}"
        )
        await notify_admin_download(user_id, username, os.path.basename(file_path), "ovpn")
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"cancel_openvpn_config_{client_name}")]
        ])
        await show_menu(user_id, "Вернуться к выбору типа конфига:", markup)
    else:
        await bot.send_message(user_id, "❌ Файл конфигурации не найден.")
        await show_menu(
            user_id,
            f"Меню пользователя <b>{client_name}</b>:",
            create_user_menu(client_name, back_callback="users_menu" if user_id == ADMIN_ID else "main_menu", 
                            is_admin=(user_id == ADMIN_ID), user_id=user_id)
        )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("cancel_openvpn_config_"))
async def cancel_openvpn_config(callback: types.CallbackQuery):
    client_name = callback.data.replace("cancel_openvpn_config_", "")
    user_id = callback.from_user.id
    await delete_last_menus(user_id)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await show_menu(
        user_id,
        f"Меню пользователя <b>{client_name}</b>:",
        create_user_menu(client_name, back_callback="users_menu" if user_id == ADMIN_ID else "main_menu", 
                        is_admin=(user_id == ADMIN_ID), user_id=user_id)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("back_to_user_menu_"))
async def back_to_user_menu(callback: types.CallbackQuery):
    client_name = callback.data.replace("back_to_user_menu_", "")
    user_id = callback.from_user.id
    await delete_last_menus(user_id)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await show_menu(
        user_id,
        f"Меню пользователя <b>{client_name}</b>:",
        create_user_menu(client_name, back_callback="users_menu" if user_id == ADMIN_ID else "main_menu", 
                        is_admin=(user_id == ADMIN_ID), user_id=user_id)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("send_wg_"))
async def handle_wg_config(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_", 3)
    _, _, config_type, client_name = parts
    user_id = callback.from_user.id
    username = callback.from_user.username or "Без username"
    await delete_last_menus(user_id)
    try:
        await callback.message.delete()
    except Exception:
        pass
    if config_type == "vpn":
        file_path = f"/root/antizapret/client/wireguard/vpn/{FILEVPN_NAME} - Обычный VPN -{client_name}.conf"
    else:
        file_path = f"/root/antizapret/client/wireguard/antizapret/{FILEVPN_NAME} -{client_name}.conf"
    if not os.path.exists(file_path):
        subprocess.run(['/root/antizapret/client.sh', '4', client_name], check=True)
    if os.path.exists(file_path):
        await bot.send_document(
            user_id,
            FSInputFile(file_path),
            caption=f"🔐 {os.path.basename(file_path)}"
        )
        await notify_admin_download(user_id, username, os.path.basename(file_path), "wg")
    else:
        await bot.send_message(user_id, "❌ Файл конфигурации не найден.")
    await show_menu(
        user_id,
        f"Меню пользователя <b>{client_name}</b>:",
        create_user_menu(client_name, back_callback="users_menu" if user_id == ADMIN_ID else "main_menu", 
                        is_admin=(user_id == ADMIN_ID), user_id=user_id)
    )
    await state.clear()
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("download_am_"))
async def download_am_config(callback: types.CallbackQuery):
    parts = callback.data.split("_", 3)
    _, _, am_type, client_name = parts
    user_id = callback.from_user.id
    username = callback.from_user.username or "Без username"
    if am_type == "vpn":
        file_path = f"/root/antizapret/client/amneziawg/vpn/{FILEVPN_NAME} - Обычный VPN -{client_name}.conf"
    else:
        file_path = f"/root/antizapret/client/amneziawg/antizapret/{FILEVPN_NAME} -{client_name}.conf"
    if not os.path.exists(file_path):
        subprocess.run(['/root/antizapret/client.sh', '4', client_name], check=True)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await delete_last_menus(user_id)
    if os.path.exists(file_path):
        await bot.send_document(user_id, FSInputFile(file_path), caption=f"🔐 {os.path.basename(file_path)}")
        await notify_admin_download(user_id, username, os.path.basename(file_path), "amnezia")
    else:
        await bot.send_message(user_id, "❌ Файл не найден")
    await show_menu(
        user_id,
        f"Меню пользователя <b>{client_name}</b>:",
        create_user_menu(client_name, back_callback="users_menu" if user_id == ADMIN_ID else "main_menu", 
                        is_admin=(user_id == ADMIN_ID), user_id=user_id)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "del_user")
async def delete_user_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав!", show_alert=True)
        return
    clients = await get_clients("openvpn")
    if not clients:
        await callback.message.edit_text("Нет клиентов для удаления.", 
                                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                            [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
                                        ]))
        await callback.answer()
        return
    keyboard = []
    for client in clients:
        keyboard.append([InlineKeyboardButton(text=client, callback_data=f"delete_openvpn_{client}")])
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")])
    await callback.message.edit_text(
        "Выберите клиента для удаления:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
    )
    await state.set_state(VPNSetup.deleting_client)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("delete_openvpn_"))
async def handle_delete_client(callback: types.CallbackQuery, state: FSMContext):
    client_name = callback.data[len("delete_openvpn_"):]
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав!", show_alert=True)
        return
    await state.update_data(client_name=client_name)
    await callback.message.edit_text(
        f"Вы уверены, что хотите удалить клиента <b>{client_name}</b>?",
        parse_mode="HTML",
        reply_markup=create_confirmation_keyboard(client_name, "delete")
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("confirm_delete_"))
async def confirm_delete_client(callback: types.CallbackQuery, state: FSMContext):
    client_name = callback.data[len("confirm_delete_"):]
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав!", show_alert=True)
        return
    result = await execute_script("2", client_name)
    if result["returncode"] == 0:
        user_id = get_user_id_by_name(client_name)
        if user_id:
            remove_user_id(user_id)
            remove_approved_user(user_id)
            set_user_emoji(user_id, "")
            save_profile_name(user_id, None)
        await cleanup_openvpn_files(client_name)
        await callback.message.edit_text(
            f"✅ Клиент <b>{client_name}</b> удалён.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
            ])
        )
    else:
        await callback.message.edit_text(
            f"❌ Ошибка при удалении клиента <b>{client_name}</b>: {result['stderr']}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
            ])
        )
    await state.clear()
    await callback.answer()

@dp.callback_query(lambda c: c.data == "cancel_delete")
async def cancel_delete_client(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    stats = get_server_info()
    await callback.message.edit_text(
        stats + "\n<b>Главное меню:</b>",
        reply_markup=create_main_menu(),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "7")
async def recreate_vpn_files(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав!", show_alert=True)
        return
    result = await execute_script("7")
    if result["returncode"] == 0:
        await callback.message.edit_text(
            "✅ VPN-файлы успешно пересозданы.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
            ])
        )
    else:
        await callback.message.edit_text(
            f"❌ Ошибка при пересоздании VPN-файлов: {result['stderr']}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
            ])
        )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "8")
async def create_backup(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав!", show_alert=True)
        return
    result = await execute_script("8")
    if result["returncode"] == 0:
        if await send_backup(callback.from_user.id):
            await callback.message.edit_text(
                "✅ Бэкап создан и отправлен.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
                ])
            )
        else:
            await callback.message.edit_text(
                "❌ Ошибка: Бэкап не найден или не удалось отправить.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
                ])
            )
    else:
        await callback.message.edit_text(
            f"❌ Ошибка при создании бэкапа: {result['stderr']}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
            ])
        )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "announce_menu")
async def announce_menu_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав!", show_alert=True)
        return
    await delete_last_menus(callback.from_user.id)
    try:
        await callback.message.delete()
    except Exception:
        pass
    msg = await bot.send_message(
        callback.from_user.id,
        "📢 Введите текст объявления для всех пользователей:",
        reply_markup=cancel_markup
    )
    await state.set_state(AdminAnnounce.waiting_for_text)
    await state.update_data(prompt_msg_id=msg.message_id)
    await callback.answer()

@dp.message(AdminAnnounce.waiting_for_text)
async def process_announce_text(message: types.Message, state: FSMContext):
    data = await state.get_data()
    prompt_msg_id = data.get("prompt_msg_id")
    if prompt_msg_id:
        try:
            await bot.delete_message(message.chat.id, prompt_msg_id)
        except Exception:
            pass
    if message.text in ("❌", "❌ Отмена", "отмена", "Отмена"):
        await state.clear()
        await delete_last_menus(message.from_user.id)
        stats = get_server_info()
        await show_menu(
            message.from_user.id,
            stats + "\n<b>Главное меню:</b>",
            create_main_menu()
        )
        return
    announcement = message.text.strip()
    try:
        await message.delete()
    except Exception:
        pass
    # Отправка объявления всем пользователям
    with open(USERS_FILE, "r") as f:
        user_ids = [line.strip() for line in f if line.strip().isdigit()]
    sent_count = 0
    for user_id in user_ids:
        try:
            await safe_send_message(
                int(user_id),
                f"📢 <b>Объявление от админа:</b>\n\n{announcement}",
                parse_mode="HTML"
            )
            sent_count += 1
        except Exception as e:
            print(f"[Ошибка отправки объявления] user_id={user_id}: {e}")
    await show_menu(
        message.from_user.id,
        f"✅ Объявление отправлено {sent_count} пользователям.\n\n<b>Главное меню:</b>",
        create_main_menu()
    )
    await state.clear()

@dp.callback_query(lambda c: c.data == "who_online")
async def who_online(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав!", show_alert=True)
        return
    openvpn_online = get_online_users_from_log()
    wg_online = get_online_wg_peers()
    all_online = {**openvpn_online, **wg_online}
    if not all_online:
        await callback.message.edit_text(
            "🟢 <b>В сети никого нет.</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
            ]),
            parse_mode="HTML"
        )
        await callback.answer()
        return
    text = "🟢 <b>Пользователи в сети:</b>\n\n"
    for client_name, proto in all_online.items():
        user_id = get_user_id_by_name(client_name)
        emoji = get_user_emoji(user_id) if user_id else ""
        text += f"{emoji + ' ' if emoji else ''}{client_name} ({proto})\n"
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
        ]),
        parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("select_openvpn_"))
async def select_openvpn_config(callback: types.CallbackQuery):
    client_name = callback.data[len("select_openvpn_"):]
    user_id = callback.from_user.id
    await delete_last_menus(user_id)
    try:
        await callback.message.delete()
    except Exception:
        pass
    await show_menu(
        user_id,
        "Выберите тип OpenVPN-конфига:",
        create_openvpn_config_menu(client_name)
    )
    await callback.answer()

# --- Запуск бота ---
async def main():
    await set_bot_commands()
    await update_bot_description()
    await update_bot_about()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
```

### Объяснение изменений
1. **Завершение обработчика `create_backup`**:
   - Проверяется результат выполнения скрипта для создания бэкапа.
   - Если бэкап создан успешно (`result["returncode"] == 0`), вызывается функция `send_backup` для отправки файла администратору.
   - В случае успеха выводится сообщение об успешной отправке, в случае ошибки — сообщение об ошибке.
   - Добавлена кнопка возврата в главное меню.

2. **Добавление обработчика `announce_menu`**:
   - Создан обработчик для callback_data="announce_menu", доступный только администратору.
   - Запрашивается текст объявления через состояние `AdminAnnounce.waiting_for_text`.
   - При получении текста объявление рассылается всем пользователям из `users.txt` с использованием `safe_send_message`.
   - После отправки возвращается в главное меню с отображением количества успешно отправленных сообщений.

3. **Добавление обработчика `who_online`**:
   - Обработчик для callback_data="who_online", доступный только администратору.
   - Получает списки онлайн-пользователей для OpenVPN и WireGuard/AmneziaWG с помощью функций `get_online_users_from_log` и `get_online_wg_peers`.
   - Формирует список пользователей в сети с указанием протокола и эмодзи (если есть).
   - Если никто не онлайн, выводится соответствующее сообщение.
   - Добавлена кнопка возврата в главное меню.

4. **Сохранение структуры и логики**:
   - Все существующие функции (управление пользователями, VPN-конфигурации, заявки, уведомления, интеграция с ЮKassa) сохранены без изменений.
   - Artifact_id и структура кода остались прежними, чтобы обозначить это как обновление.
   - Добавлены только необходимые обработчики для завершения функциональности.

5. **Запуск бота**:
   - В конце добавлен основной запуск бота с установкой команд, описания и информации о боте.

Если есть дополнительные функции или изменения, которые нужно добавить, напишите, и я продолжу доработку!
