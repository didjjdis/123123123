import logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
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
load_dotenv()
YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID")
YOOKASSA_SECRET_KEY = os.getenv("YOOKASSA_SECRET_KEY")
if YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY:
    Configuration.configure(YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY)
else:
    logging.error("YOOKASSA_SHOP_ID или YOOKASSA_SECRET_KEY не заданы в .env")
    sys.exit(1)

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
    try:
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
        logging.info(f"База данных инициализирована: {db_path}")
    except Exception as e:
        logging.error(f"Ошибка инициализации базы данных: {e}")
        raise
    finally:
        conn.close()

# Сохранение имени профиля и инициализация баланса
def save_profile_name(user_id, new_profile_name, db_path="/root/vpn.db"):
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE id=?", (user_id,))
        res = cur.fetchone()
        if res:
            cur.execute("UPDATE users SET profile_name=? WHERE id=?", (new_profile_name, user_id))
        else:
            cur.execute("INSERT INTO users (id, profile_name, balance) VALUES (?, ?, 0.0)", (user_id, new_profile_name))
        conn.commit()
        logging.info(f"Сохранено имя профиля для user_id={user_id}: {new_profile_name}")
    except Exception as e:
        logging.error(f"Ошибка сохранения имени профиля для user_id={user_id}: {e}")
    finally:
        conn.close()

# Получение баланса пользователя
def get_user_balance(user_id, db_path="/root/vpn.db"):
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT balance FROM users WHERE id=?", (user_id,))
        res = cur.fetchone()
        return res[0] if res else 0.0
    except Exception as e:
        logging.error(f"Ошибка получения баланса для user_id={user_id}: {e}")
        return 0.0
    finally:
        conn.close()

# Обновление баланса пользователя
def update_user_balance(user_id, amount, db_path="/root/vpn.db"):
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("UPDATE users SET balance = balance + ? WHERE id=?", (amount, user_id))
        conn.commit()
        logging.info(f"Баланс обновлен для user_id={user_id}: +{amount}")
    except Exception as e:
        logging.error(f"Ошибка обновления баланса для user_id={user_id}: {e}")
    finally:
        conn.close()

# Сохранение информации о платеже
def save_payment(payment_id, user_id, amount, status, db_path="/root/vpn.db"):
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO payments (payment_id, user_id, amount, status, created_at) VALUES (?, ?, ?, ?, ?)",
            (payment_id, user_id, amount, status, datetime.now(timezone.utc))
        )
        conn.commit()
        logging.info(f"Платеж сохранен: payment_id={payment_id}, user_id={user_id}, amount={amount}, status={status}")
    except Exception as e:
        logging.error(f"Ошибка сохранения платежа payment_id={payment_id}: {e}")
    finally:
        conn.close()

# Обновление статуса платежа
def update_payment_status(payment_id, status, db_path="/root/vpn.db"):
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("UPDATE payments SET status=? WHERE payment_id=?", (status, payment_id))
        conn.commit()
        logging.info(f"Статус платежа обновлен: payment_id={payment_id}, status={status}")
    except Exception as e:
        logging.error(f"Ошибка обновления статуса платежа payment_id={payment_id}: {e}")
    finally:
        conn.close()

# Получение статуса платежа
def get_payment_info(payment_id, db_path="/root/vpn.db"):
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT user_id, amount, status FROM payments WHERE payment_id=?", (payment_id,))
        res = cur.fetchone()
        return {"user_id": res[0], "amount": res[1], "status": res[2]} if res else None
    except Exception as e:
        logging.error(f"Ошибка получения информации о платеже payment_id={payment_id}: {e}")
        return None
    finally:
        conn.close()

# Получение списка всех пользователей и их балансов
def get_all_users_balances(db_path="/root/vpn.db"):
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT id, profile_name, balance FROM users")
        users = cur.fetchall()
        return users
    except Exception as e:
        logging.error(f"Ошибка получения списка пользователей: {e}")
        return []
    finally:
        conn.close()

# Инициализация базы данных
DB_PATH = "/root/vpn.db"
try:
    init_db(DB_PATH)
except Exception as e:
    logging.error(f"Не удалось инициализировать базу данных: {e}")
    sys.exit(1)

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

FILEVPN_NAME = os.getenv("FILEVPN_NAME")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")

if not FILEVPN_NAME:
    logging.error("FILEVPN_NAME не задан в .env")
    sys.exit(1)
if not BOT_TOKEN:
    logging.error("BOT_TOKEN не задан в .env")
    sys.exit(1)
if not ADMIN_ID:
    logging.error("ADMIN_ID не задан в .env")
    sys.exit(1)
try:
    ADMIN_ID = int(ADMIN_ID)
except ValueError:
    logging.error("ADMIN_ID должен быть числом")
    sys.exit(1)

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
    try:
        idempotence_key = str(uuid.uuid4())
        payment = Payment.create({
            "amount": {
                "value": f"{amount:.2f}",
                "currency": "RUB"
            },
            "confirmation": {
                "type": "redirect",
                "return_url": "https://t.me/your_bot_username"  # Замените на ваш Telegram-бот
            },
            "capture": True,
            "description": f"Пополнение баланса для пользователя {user_id}",
            "metadata": {"user_id": user_id}
        }, idempotence_key)
        logging.info(f"Платеж создан: payment_id={payment.id}, user_id={user_id}, amount={amount}")
        return payment
    except Exception as e:
        logging.error(f"Ошибка создания платежа для user_id={user_id}: {e}")
        raise

# Проверка статуса платежа
async def check_payment_status(payment_id):
    try:
        payment = Payment.find_one(payment_id)
        logging.info(f"Проверка статуса платежа: payment_id={payment_id}, status={payment.status}")
        return payment.status
    except Exception as e:
        logging.error(f"Ошибка проверки статуса платежа payment_id={payment_id}: {e}")
        raise

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
        logging.info(f"Меню отображено для chat_id={chat_id}, message_id={msg.message_id}")
        return msg
    except Exception as e:
        logging.error(f"[show_menu] Ошибка для chat_id={chat_id}: {e}")
        raise

# Сохранение ID последнего меню
def set_last_menu_id(user_id, msg_id):
    try:
        data = {}
        if os.path.exists(LAST_MENUS_FILE):
            with open(LAST_MENUS_FILE, "r") as f:
                data = json.load(f)
        user_id = str(user_id)
        data[user_id] = [msg_id]
        with open(LAST_MENUS_FILE, "w") as f:
            json.dump(data, f)
        logging.info(f"Сохранен message_id={msg_id} для user_id={user_id}")
    except Exception as e:
        logging.error(f"Ошибка сохранения message_id для user_id={user_id}: {e}")

# Получение ID последних меню
def get_last_menu_ids(user_id):
    if not os.path.exists(LAST_MENUS_FILE):
        return []
    try:
        with open(LAST_MENUS_FILE, "r") as f:
            data = json.load(f)
        return data.get(str(user_id), [])
    except Exception:
        logging.error(f"Ошибка чтения LAST_MENUS_FILE для user_id={user_id}")
        return []

# Удаление последних меню
async def delete_last_menus(user_id):
    if not os.path.exists(LAST_MENUS_FILE):
        return
    try:
        with open(LAST_MENUS_FILE, "r") as f:
            data = json.load(f)
        ids = data.get(str(user_id), [])
        for mid in ids:
            try:
                await bot.delete_message(user_id, mid)
                logging.info(f"Удалено сообщение message_id={mid} для user_id={user_id}")
            except Exception:
                pass
        data[str(user_id)] = []
        with open(LAST_MENUS_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        logging.error(f"Ошибка удаления меню для user_id={user_id}: {e}")

# Добавление в очередь ожидания
def add_pending(user_id, username, fullname):
    try:
        pending = {}
        if os.path.exists(PENDING_FILE):
            with open(PENDING_FILE, "r") as f:
                pending = json.load(f)
        pending[str(user_id)] = {"username": username, "fullname": fullname}
        with open(PENDING_FILE, "w") as f:
            json.dump(pending, f)
        logging.info(f"Пользователь user_id={user_id} добавлен в очередь ожидания")
    except Exception as e:
        logging.error(f"Ошибка добавления в очередь ожидания user_id={user_id}: {e}")

# Удаление из очереди ожидания
def remove_pending(user_id):
    if not os.path.exists(PENDING_FILE):
        return
    try:
        with open(PENDING_FILE, "r") as f:
            pending = json.load(f)
        pending.pop(str(user_id), None)
        with open(PENDING_FILE, "w") as f:
            json.dump(pending, f)
        logging.info(f"Пользователь user_id={user_id} удален из очереди ожидания")
    except Exception as e:
        logging.error(f"Ошибка удаления из очереди ожидания user_id={user_id}: {e}")

# Проверка статуса ожидания
def is_pending(user_id):
    if not os.path.exists(PENDING_FILE):
        return False
    try:
        with open(PENDING_FILE, "r") as f:
            pending = json.load(f)
        return str(user_id) in pending
    except Exception:
        logging.error(f"Ошибка проверки статуса ожидания для user_id={user_id}")
        return False

# Безопасная отправка сообщения
async def safe_send_message(chat_id, text, **kwargs):
    logging.info(f"[SAFE_SEND] chat_id={chat_id}, text={text[:50]}, kwargs={kwargs}")
    try:
        await bot.send_message(chat_id, text, **kwargs)
        logging.info(f"[SAFE_SEND] success to {chat_id}!")
    except Exception as e:
        logging.error(f"[Ошибка отправки сообщения] chat_id={chat_id}: {e}")

# Проверка регистрации пользователя
def user_registered(user_id):
    return bool(get_profile_name(user_id))

APPROVED_FILE = "approved_users.txt"

# Установка эмодзи пользователя
def set_user_emoji(user_id, emoji):
    try:
        data = {}
        if os.path.exists(EMOJI_FILE):
            with open(EMOJI_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        data[str(user_id)] = emoji
        with open(EMOJI_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        logging.info(f"Эмодзи установлен для user_id={user_id}: {emoji}")
    except Exception as e:
        logging.error(f"Ошибка установки эмодзи для user_id={user_id}: {e}")

# Получение эмодзи пользователя
def get_user_emoji(user_id):
    if not os.path.exists(EMOJI_FILE):
        return ""
    try:
        with open(EMOJI_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get(str(user_id), "")
    except Exception:
        logging.error(f"Ошибка получения эмодзи для user_id={user_id}")
        return ""

# Проверка одобренного пользователя
def is_approved_user(user_id):
    user_id = str(user_id)
    if not os.path.exists(APPROVED_FILE):
        return False
    try:
        with open(APPROVED_FILE, "r") as f:
            approved = [line.strip() for line in f]
        return user_id in approved
    except Exception as e:
        logging.error(f"Ошибка проверки одобренного пользователя user_id={user_id}: {e}")
        return False

# Одобрение пользователя
def approve_user(user_id):
    user_id = str(user_id)
    if not is_approved_user(user_id):
        try:
            with open(APPROVED_FILE, "a") as f:
                f.write(user_id + "\n")
            logging.info(f"Пользователь user_id={user_id} одобрен")
        except Exception as e:
            logging.error(f"Ошибка одобрения пользователя user_id={user_id}: {e}")

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
        logging.info(f"ID пользователя сохранен: {user_id}")
    except Exception as e:
        logging.error(f"[save_user_id] Ошибка при сохранении user_id={user_id}: {e}")

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
        logging.info(f"ID пользователя удален: {user_id}")
    except Exception as e:
        logging.error(f"[remove_user_id] Не удалось обновить {USERS_FILE}: {e}")

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
        logging.info(f"Одобренный пользователь удален: {user_id}")
    except Exception as e:
        logging.error(f"[remove_approved_user] Не удалось обновить {APPROVED_FILE}: {e}")

# Получение имени профиля
def get_profile_name(user_id, db_path="/root/vpn.db"):
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT profile_name FROM users WHERE id=?", (user_id,))
        res = cur.fetchone()
        return res[0] if res else None
    except Exception as e:
        logging.error(f"Ошибка получения имени профиля для user_id={user_id}: {e}")
        return None
    finally:
        conn.close()

# Получение ID пользователя по имени
def get_user_id_by_name(client_name, db_path="/root/vpn.db"):
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE profile_name=?", (client_name,))
        res = cur.fetchone()
        return res[0] if res else None
    except Exception as e:
        logging.error(f"Ошибка получения user_id по имени {client_name}: {e}")
        return None
    finally:
        conn.close()

# Получение информации о сертификате
def get_cert_expiry_info(client_name):
    try:
        result = subprocess.run(
            ["/etc/openvpn/easyrsa3/easyrsa", "--batch", "show-cert", client_name],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            logging.error(f"Ошибка получения информации о сертификате для {client_name}: {result.stderr}")
            return None
        expiry_match = re.search(r"notAfter=([^\n]+)", result.stdout)
        if not expiry_match:
            return None
        expiry_date = datetime.strptime(expiry_match.group(1), "%b %d %H:%M:%S %Y %Z")
        days_left = (expiry_date - datetime.now()).days
        return {"days_left": days_left}
    except Exception as e:
        logging.error(f"Ошибка обработки сертификата для {client_name}: {e}")
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
        logging.info(f"Скрипт выполнен: option={option}, client_name={client_name}, days={days}, returncode={process.returncode}")
        return {
            "returncode": process.returncode,
            "stdout": stdout.decode(),
            "stderr": stderr.decode(),
        }
    except Exception as e:
        logging.error(f"Ошибка выполнения скрипта: option={option}, client_name={client_name}: {e}")
        return {"returncode": 1, "stdout": "", "stderr": str(e)}

# Проверка существования клиента
async def client_exists(vpn_type: str, client_name: str) -> bool:
    try:
        clients = await get_clients(vpn_type)
        return client_name in clients
    except Exception as e:
        logging.error(f"Ошибка проверки существования клиента {client_name} для vpn_type={vpn_type}: {e}")
        return False

# Получение списка клиентов
async def get_clients(vpn_type: str):
    option = "3" if vpn_type == "openvpn" else "6"
    try:
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
            logging.info(f"Получен список клиентов для vpn_type={vpn_type}: {clients}")
            return clients
        return []
    except Exception as e:
        logging.error(f"Ошибка получения списка клиентов для vpn_type={vpn_type}: {e}")
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
        logging.info(f"Получены онлайн-пользователи OpenVPN: {users}")
    except Exception as e:
        logging.error(f"Ошибка чтения лога OpenVPN: {e}")
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
        logging.info(f"Получены онлайн-пользователи WireGuard: {peers}")
    except Exception as e:
        logging.error(f"[ERROR] wg show: {e}")
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
            logging.info(f"Внешний IP получен: {response.text}")
            return response.text
        logging.error("Ошибка получения IP: статус не 200")
        return "IP не найден"
    except requests.Timeout:
        logging.error("Ошибка получения IP: запрос превысил время ожидания")
        return "Ошибка: запрос превысил время ожидания."
    except requests.ConnectionError:
        logging.error("Ошибка получения IP: нет подключения к интернету")
        return "Ошибка: нет подключения к интернету."
    except requests.RequestException as e:
        logging.error(f"Ошибка получения IP: {e}")
        return f"Ошибка при запросе: {e}"

SERVER_IP = get_external_ip()

# Получение информации о сервере
def get_server_info():
    try:
        ip = SERVER_IP
        uptime_seconds = int(psutil.boot_time())
        uptime = datetime.now() - datetime.fromtimestamp(uptime_seconds)
        cpu = psutil.cpu_percent()
        mem = psutil.virtual_memory().percent
        hostname = socket.gethostname()
        os_version = platform.platform()
        info = f"""<b>💻 Сервер:</b> <code>{hostname}</code>
<b>🌐 IP:</b> <code>{ip}</code>
<b>🕒 Аптайм:</b> <code>{str(uptime).split('.')[0]}</code>
<b>🧠 RAM:</b> <code>{mem}%</code>
<b>⚡ CPU:</b> <code>{cpu}%</code>
<b>🛠 ОС:</b> <code>{os_version}</code>
"""
        logging.info("Информация о сервере получена")
        return info
    except Exception as e:
        logging.error(f"Ошибка получения информации о сервере: {e}")
        return "Ошибка получения информации о сервере"

# Обработчик команды /start
@dp.message(Command("start"))
async def start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    logging.info(f"Команда /start от user_id={user_id}")
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
        if not client_name:
            logging.error(f"Имя профиля не найдено для user_id={user_id}")
            msg = await message.answer("❌ Ошибка: профиль не найден. Свяжитесь с администратором.")
            set_last_menu_id(user_id, msg.message_id)
            return
        if not await client_exists("openvpn", client_name):
            result = await execute_script("1", client_name, "30")
            if result["returncode"] != 0:
                logging.error(f"Ошибка регистрации клиента {client_name}: {result['stderr
