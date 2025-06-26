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
import hashlib
from aiogram import types
from asyncio import sleep
from aiogram.filters import StateFilter
from aiogram.exceptions import TelegramBadRequest
import sqlite3
import uuid
from aiogram.fsm.state import State, StatesGroup

class SetEmoji(StatesGroup):
    waiting_for_emoji = State()

from collections import deque    
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, FSInputFile, BotCommand
from aiogram.fsm.context import FSMContext
from aiogram.client.default import DefaultBotProperties
class RenameProfile(StatesGroup):
    waiting_for_new_name = State()
    waiting_for_rename_approve = State()  # Новое состояние для одобрения с новым именем


import subprocess
from datetime import datetime, timedelta, timezone
import psutil
import platform
import socket

from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from db import init_db, get_profile_name, save_profile_name

DB_PATH = "vpn.db"
init_db(DB_PATH)

cancel_markup = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="❌ Отмена")]],
    resize_keyboard=True,
    one_time_keyboard=True
)

USERS_FILE = "users.txt"

LAST_MENU_FILE = "last_menu.json"

class SetEmojiState(StatesGroup):
    waiting_for_emoji = State()
    
# для SQLite
def save_profile_name(user_id, new_profile_name, db_path="/root/vpn.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE id=?", (user_id,))
    res = cur.fetchone()
    if res:
        cur.execute("UPDATE users SET profile_name=? WHERE id=?", (new_profile_name, user_id))
    else:
        cur.execute("INSERT INTO users (id, profile_name) VALUES (?, ?)", (user_id, new_profile_name))
    conn.commit()
    conn.close()


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


import json

MAX_BOT_MENUS = 1

PENDING_FILE = "pending_users.json"

class AdminAnnounce(StatesGroup):
    waiting_for_text = State()
    
async def safe_send_message(chat_id, text, **kwargs):
    print(f"[SAFE_SEND] chat_id={chat_id}, text={text[:50]}, kwargs={kwargs}")
    try:
        await bot.send_message(chat_id, text, **kwargs)
        print(f"[SAFE_SEND] success to {chat_id}!")
    except Exception as e:
        print(f"[Ошибка отправки сообщения] chat_id={chat_id}: {e}")


def get_last_menu_ids(user_id):
    if not os.path.exists(LAST_MENUS_FILE):
        return []
    try:
        with open(LAST_MENUS_FILE, "r") as f:
            data = json.load(f)
        return data.get(str(user_id), [])
    except Exception:
        return []

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


load_dotenv()
FILEVPN_NAME = os.getenv("FILEVPN_NAME")
if not FILEVPN_NAME:
    raise RuntimeError("FILEVPN_NAME не задан в .env")

# Конфигурация
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан в .env")
if not ADMIN_ID:
    raise RuntimeError("ADMIN_ID не задан в .env")
ADMIN_ID = int(ADMIN_ID)
EMOJI_FILE = "user_emojis.json"
LAST_MENUS_FILE = "last_menus.json"
MAX_MENUS_PER_USER = 3  # или сколько надо, обычно 3-5

# === Параметры 3x-UI для VLESS === 
#Если хотите чтоб у вас появилась отдельная кнопка VLESS онлайн то раскомментируйте параметры (просто уберите знак"#". Предварительно должна быть установлена панель 3X-UI
#BASE_URL    = "Замени свой адрес сюда/Путь к панели по типу Yy7jywStMXYLYzS"
#LOGIN_PATH  = "/login"
#ONLINES_PATH = "/panel/api/inbounds/onlines"
#USERNAME = "ЛОГИН"
#PASSWORD = "ПАРОЛЬ"

# Сессия для хранения куки 3x-UI
#session = requests.Session()

ITEMS_PER_PAGE = 5
AUTHORIZED_USERS = [ADMIN_ID]  # Список Telegram ID пользователей
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

print(f"=== BOT START ===")
print(f"BOT_TOKEN starts with: {BOT_TOKEN[:8]}... (length: {len(BOT_TOKEN) if BOT_TOKEN else 0})")
print(f"ADMIN_ID: {ADMIN_ID} ({type(ADMIN_ID)})")
print(f"==================")

# Проверяем, что переменные окружения корректны
if not BOT_TOKEN or BOT_TOKEN == "<Enter API Token>":
    print("Ошибка: BOT_TOKEN не задан или содержит значение по умолчанию.")
    sys.exit(1)

if not ADMIN_ID or ADMIN_ID == "<Enter your user ID>":
    print("Ошибка: ADMIN_ID не задан или содержит значение по умолчанию.")
    sys.exit(1)


class VPNSetup(StatesGroup):
    """Класс состояний для управления процессами настройки VPN через бота."""
    entering_user_id = State()        # ждём, что админ введёт Telegram-ID
    entering_client_name_manual = State()  # ждём, что админ введёт имя профиля для этого ID
    choosing_option = State()  # Состояние выбора опции (добавление/удаление клиента).
    entering_client_name = State()  # Состояние ввода имени клиента.
    entering_days = State()  # Состояние ввода количества дней для сертификата.
    deleting_client = State()  # Состояние подтверждения удаления клиента.
    list_for_delete = State()  # Состояние выбора клиента из списка для удаления.
    choosing_config_type = State()  # Состояние для выбора конфигурации
    choosing_protocol = State()  # Для выбора протокола OpenVPN
    choosing_wg_type = State()  # Для выбора типа WireGuard
    confirming_rename = State()  # Для подтверждения переименования файлов WireGuard


# Описание для вашего бота
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


#Для VLESS онлайн убери ниже #, чтоб строка начиналась с def authenticate() -> bool:
#def authenticate() -> bool:
#    """
#    Логинимся на 3x-UI, сохраняем куки в session.
#    Возвращает True, если логин успешен, иначе False.
#    """
#    url = BASE_URL + LOGIN_PATH
#    payload = {"username": USERNAME, "password": PASSWORD}
#    try:
#        resp = session.post(url, json=payload, timeout=10)
#        resp.raise_for_status()
#    except Exception as e:
#        logging.error(f"[AUTH] Ошибка при запросе /login: {e}")
#        return False
#
#    data = resp.json()
#    if data.get("success"):
#        logging.info("[AUTH] Успешный вход, куки сохранены.")
#        return True
#    else:
#        logging.error(f"[AUTH] Не удалось залогиниться: {data}")
#        return False






def user_registered(user_id):
    # Если юзер найден в базе — ОК
    return bool(get_profile_name(user_id))

APPROVED_FILE = "approved_users.txt"

# ==== Эмодзи хранение ====
@dp.message(StateFilter(SetEmoji.waiting_for_emoji))
async def set_user_emoji(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    client_name = (await state.get_data())['client_name']
    emoji = message.text.strip()

    # Удаляем текст "Введи смайл..."
    try:
        await message.reply_to_message.delete()
    except Exception:
        pass

    if emoji == "❌":
        set_emoji_for_user(client_name, "")  # твоя функция
        reply_text = "Смайл удалён"
    else:
        set_emoji_for_user(client_name, emoji)  # твоя функция
        reply_text = f"Установлен смайл: {emoji}"

    # Отправляем и через 2 сек удаляем сообщение
    info_msg = await message.answer(reply_text)
    await sleep(2)
    try:
        await info_msg.delete()
    except Exception:
        pass

    # Показываем меню управления пользователем
    await show_menu(
        user_id,
        f"Меню пользователя <b>{client_name}</b>:",
        create_user_menu(client_name, back_callback="users_menu", is_admin=(user_id == ADMIN_ID), user_id=user_id)
    )
    await state.clear()

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

async def switch_menu(callback: types.CallbackQuery, text: str, reply_markup=None, parse_mode="HTML"):
    try:
        await callback.message.delete()
    except Exception:
        pass  # сообщение уже могло быть удалено
    await bot.send_message(callback.from_user.id, text, reply_markup=reply_markup, parse_mode=parse_mode)

async def set_bot_commands():
    """
    Асинхронная функция для установки списка команд бота.
    """
    async with Bot(token=BOT_TOKEN) as bot:
        commands = [
            BotCommand(command="start", description="Запустить бота"),
        ]

        await bot.set_my_commands(commands)


@dp.callback_query(lambda c: c.from_user.id != ADMIN_ID
                          and c.data != "send_request"
                          and not is_approved_user(c.from_user.id)
                          and not is_pending(c.from_user.id))
async def _deny_unapproved_callback(callback: types.CallbackQuery):
    await callback.answer(
        "Йоу, у тебя нету доступа к сервесу. Чтобы отправить заявку, пропиши /start !",
        show_alert=True
    )


@dp.callback_query(lambda c: c.data.startswith("approve_rename_"))
async def process_application_rename(callback: types.CallbackQuery, state: FSMContext):
    user_id = int(callback.data.split("_", 2)[-1])
    # Сохраняем id заявки (меню заявки)
    await state.update_data(approve_user_id=user_id, pending_menu_msg_id=callback.message.message_id)
    try:
        await callback.message.delete()  # Уже удаляем
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

def remove_user_id(user_id):
    """Удаляет строку с данным user_id из файла users.txt."""
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
    """Удаляет строку с данным user_id из файла approved_users.txt."""
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


@dp.message(RenameProfile.waiting_for_rename_approve)
async def process_rename_new_name(message: types.Message, state: FSMContext):
    new_name = message.text.strip()
    data = await state.get_data()
    rename_prompt_id = data.get("rename_prompt_id")
    pending_menu_msg_id = data.get("pending_menu_msg_id")

    # Удаляем prompt "Введи новое имя..."
    if rename_prompt_id:
        try:
            await bot.delete_message(message.chat.id, rename_prompt_id)
        except Exception:
            pass

    # Удаляем само сообщение пользователя (введённое имя)
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
            message.chat.id, "❌ Некорректное имя! Используй только буквы, цифры, _ и -."
        )
        await state.clear()
        return

    result = await execute_script("1", new_name, "30")
    if result["returncode"] == 0:
        save_profile_name(user_id, new_name)
        approve_user(user_id)
        remove_pending(user_id)
        save_user_id(user_id)  # ВАЖНО! — сразу в users.txt
        msg = await safe_send_message(
            user_id,
            f"✅ Ваша заявка одобрена!\nИмя профиля: <b>{new_name}</b>\nТеперь вам доступны функции VPN.",
            parse_mode="HTML",
            reply_markup=create_user_menu(new_name)
        )
        # УДАЛЯЕМ сообщение "Пользователь ... активирован"
        try:
            await bot.delete_message(message.chat.id, msg.message_id)
        except Exception:
            pass

        # Главное меню админу
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




    

async def ensure_user_client(user_id: int):
    client_name = get_profile_name(user_id)
    
    if not await client_exists("openvpn", client_name):
        result = await execute_script("1", client_name, "30")  # Срок по умолчанию: 10 лет
        if result["returncode"] != 0:
            print(f"Ошибка создания клиента: {result['stderr']}")
            return False

    return True


async def update_bot_description():
    """
    Асинхронная функция для обновления описания бота.

    Описание устанавливается для русского языка ("ru").
    """
    async with Bot(token=BOT_TOKEN) as bot:
        await bot.set_my_description(BOT_DESCRIPTION, language_code="ru")


BOT_ABOUT = "Бот для пользования услугами VPN от БичиVPN."

def make_users_tab_keyboard(active_tab: str):
    tabs = [
        ("Все",        "users_tab_all"),
        ("В сети",  "users_tab_online"),
        ("Скоро ББ", "users_tab_expiring"),
    ]
    buttons = []
    for title, cb in tabs:
        # подсвечиваем активный таб
        text = f"» {title} «" if cb == active_tab else title
        buttons.append(InlineKeyboardButton(text=text, callback_data=cb))
    return InlineKeyboardMarkup(inline_keyboard=[buttons])


async def update_bot_about():
    """Асинхронная функция для обновления раздела «О боте»."""
    async with Bot(token=BOT_TOKEN) as bot:
        await bot.set_my_short_description(BOT_ABOUT, language_code="ru")


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

# ==== Главное меню ====
def create_main_menu():
    keyboard = [
        [InlineKeyboardButton(text="Управление пользователями", callback_data="users_menu")],
        [InlineKeyboardButton(text="Добавить/Удалить", callback_data="add_del_menu")],
        [InlineKeyboardButton(text="Пересоздать файлы VPN", callback_data="7")],
        [InlineKeyboardButton(text="Создайть резерв", callback_data="8")],
        [InlineKeyboardButton(text="Заявки на получение доступа", callback_data="admin_pending_list")],
        [InlineKeyboardButton(text="Управление сервером VPN", callback_data="server_manage_menu")],
        [InlineKeyboardButton(text="Сделать объявление", callback_data="announce_menu")],
        [InlineKeyboardButton(text="В сети", callback_data="who_online")],
#        [InlineKeyboardButton(text="🟢 Онлайн VLESS", callback_data="vless_online")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


#Для VLESS. Убери ## и выше тоже. Где поле ОНЛАЙН VLESS
#@dp.callback_query(lambda c: c.data == "vless_online")
#async def vless_online_handler(callback: types.CallbackQuery):
#    user_id = callback.from_user.id
#
#    # 1) Проверяем куки и авторизацию
#    if "3x-ui" not in session.cookies.get_dict():
#        ok = authenticate()
#        if not ok:
#            await bot.send_message(
#                user_id,
#                "❗ Не удалось аутентифицироваться на 3x-UI. Проверьте логин/пароль или доступ."
#            )
#            await callback.answer()
#            return
#
#    # 2) Выполняем POST-запрос
#    try:
#        resp = session.post(BASE_URL + ONLINES_PATH,
#                            headers={"Content-Type": "application/json"}, timeout=10)
#    except Exception as e:
#        logging.error(f"[VLESS_ONLINE] Network error при запросе /onlines: {e}")
#        # Удалим текущее меню, покажем ошибку и вернем главную
#        try: await callback.message.delete()
#        except: pass
#        err_msg = await bot.send_message(
#            user_id,
#            "❗ Произошла сетевая ошибка при получении списка онлайн VLESS клиентов."
#        )
#        await asyncio.sleep(1)
#        try: await err_msg.delete()
#        except: pass
#        stats = get_server_info()
#        menu = await bot.send_message(
#            user_id,
#            stats + "\n<b>Главное меню:</b>",
#            reply_markup=create_main_menu(),
#            parse_mode="HTML"
#        )
#        set_last_menu_id(user_id, menu.message_id)
#        await callback.answer()
#        return
#
#    # 3) Проверяем HTTP-статус
#    if resp.status_code != 200:
#        logging.error(f"[VLESS_ONLINE] Некорректный статус {resp.status_code}, body={resp.text!r}")
#        try: await callback.message.delete()
#        except: pass
#        err_msg = await bot.send_message(
#            user_id,
#            f"❗ Сервер вернул статус {resp.status_code} вместо JSON."
#        )
#        await asyncio.sleep(1)
#        try: await err_msg.delete()
#        except: pass
#        stats = get_server_info()
#        menu = await bot.send_message(
#            user_id,
#            stats + "\n<b>Главное меню:</b>",
#            reply_markup=create_main_menu(),
#            parse_mode="HTML"
#        )
#        set_last_menu_id(user_id, menu.message_id)
#        await callback.answer()
#        return
#
#    # 4) Пробуем распарсить JSON, но оборачиваем в try/except
#    try:
#        data = resp.json()
#    except ValueError as e:
#        logging.error(f"[VLESS_ONLINE] Не JSON в ответе: {e}; content={resp.text!r}")
#        # Удаляем текущее меню
#        try: await callback.message.delete()
#        except: pass
#        info_msg = await bot.send_message(
#            user_id,
#            "ℹ️ Сервер вернул пустой или некорректный JSON для онлайн VLESS."
#        )
#        await asyncio.sleep(1)
#        try: await info_msg.delete()
#        except: pass
#        stats = get_server_info()
#        menu = await bot.send_message(
#            user_id,
#            stats + "\n<b>Главное меню:</b>",
#            reply_markup=create_main_menu(),
#            parse_mode="HTML"
#        )
#        set_last_menu_id(user_id, menu.message_id)
#        await callback.answer()
#        return
#
#    # 5) Если JSON разобран, проверяем success-флаг
#    if not isinstance(data, dict) or not data.get("success"):
#        try: await callback.message.delete()
#        except: pass
#        info_msg = await bot.send_message(
#            user_id,
#            "ℹ️ Сервер вернул success=false или неожиданный формат JSON."
#        )
#        await asyncio.sleep(1)
#        try: await info_msg.delete()
#        except: pass
#        stats = get_server_info()
#        menu = await bot.send_message(
#            user_id,
#            stats + "\n<b>Главное меню:</b>",
#            reply_markup=create_main_menu(),
#            parse_mode="HTML"
#        )
#        set_last_menu_id(user_id, menu.message_id)
#        await callback.answer()
#        return
#
#    # 6) Извлекаем список онлайн-клиентов
#    online_list = data.get("obj", [])
#    if not online_list:
#        try: await callback.message.delete()
#        except: pass
#        info_msg = await bot.send_message(
#            user_id,
#            "ℹ️ В данный момент нет активных (онлайн) VLESS клиентов."
#        )
#        await asyncio.sleep(1)
#        try: await info_msg.delete()
#        except: pass
#        stats = get_server_info()
#        menu = await bot.send_message(
#            user_id,
#            stats + "\n<b>Главное меню:</b>",
#            reply_markup=create_main_menu(),
#            parse_mode="HTML"
#        )
#        set_last_menu_id(user_id, menu.message_id)
#        await callback.answer()
#        return
#
#    # 7) Формируем текст со списком
#    text_lines = ["🟢 <b>Сейчас онлайн VLESS:</b>"]
#    for nickname in online_list:
#        safe_name = nickname.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
#        text_lines.append(f"– <code>{safe_name}</code>")
#    text = "\n".join(text_lines)
#
#    kb = InlineKeyboardMarkup(inline_keyboard=[
#        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
#    ])
#
#    try:
#        await callback.message.delete()
#    except:
#        pass
#
#    await bot.send_message(
#        user_id,
#        text,
#        parse_mode="HTML",
#        reply_markup=kb,
#        disable_web_page_preview=True
#    )
#    await callback.answer()







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


def create_server_manage_menu():
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="Перезагрузить бота", callback_data="restart_bot")],
        [types.InlineKeyboardButton(text="Перезагрузить сервер", callback_data="reboot_server")],
        [types.InlineKeyboardButton(text="Вернуться", callback_data="main_menu")],
    ])




@dp.callback_query(lambda c: c.data == "admin_pending_list")
async def show_pending_list(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет прав!", show_alert=True)
        return

    # Нет файла с заявками
    if not os.path.exists(PENDING_FILE):
        await callback.message.delete()
        msg = await bot.send_message(callback.from_user.id, "Нет заявок.")
        await asyncio.sleep(1)
        try:
            await bot.delete_message(callback.from_user.id, msg.message_id)
        except Exception:
            pass
        # Показываем главное меню!
        stats = get_server_info()
        menu = await bot.send_message(
            callback.from_user.id,
            stats + "\n<b>Главное меню:</b>",
            reply_markup=create_main_menu(),
            parse_mode="HTML"
        )
        set_last_menu_id(callback.from_user.id, menu.message_id)
        return

    # Файл есть, но заявок нет
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
        # Показываем главное меню!
        stats = get_server_info()
        menu = await bot.send_message(
            callback.from_user.id,
            stats + "\n<b>Главное меню:</b>",
            reply_markup=create_main_menu(),
            parse_mode="HTML"
        )
        set_last_menu_id(callback.from_user.id, menu.message_id)
        return

    # Если заявки есть — стандартный вывод
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


@dp.callback_query(lambda c: c.data == "add_user")
async def add_user_start(callback: types.CallbackQuery, state: FSMContext):
    # 1) чистим старые меню у админа
    await delete_last_menus(callback.from_user.id)

    # 2) просим ввести Telegram-ID нового пользователя (цифрами)
    msg = await bot.send_message(
        callback.from_user.id,
        "✏️ Введите Telegram-ID нового пользователя (только цифры).",
        reply_markup=cancel_markup
    )
    # запоминаем ID сообщения, чтобы потом его удалить (если нажали ❌)
    await state.update_data(manual_add_msg_id=msg.message_id)

    # 3) переводим FSM в состояние ожидания ID
    await state.set_state(VPNSetup.entering_user_id)

    await callback.answer()

@dp.message(VPNSetup.entering_user_id)
async def process_manual_user_id(message: types.Message, state: FSMContext):
    user_id_text = message.text.strip()
    data = await state.get_data()
    prev_msg_id = data.get("manual_add_msg_id")

    # 1) удаляем подсказку “введите Telegram-ID…”
    if prev_msg_id:
        try:
            await bot.delete_message(message.chat.id, prev_msg_id)
        except Exception:
            pass

    # 2) если нажали ❌ Отмена, отменяем
    if user_id_text in ("❌", "❌ Отмена", "отмена", "Отмена"):
        await state.clear()
        await delete_last_menus(message.from_user.id)
        stats = get_server_info()
        await show_menu(message.from_user.id, stats + "\n<b>Главное меню:</b>", create_main_menu())
        return

    # 3) проверяем, что это действительно цифры
    if not user_id_text.isdigit():
        warn = await message.answer("❌ Некорректный ID. Нужно ввести только цифры.", reply_markup=cancel_markup)
        await asyncio.sleep(1.5)
        try: await warn.delete()
        except: pass
        # повторно запрашиваем ввод
        msg = await bot.send_message(
            message.chat.id,
            "✏️ Пожалуйста, введите корректный Telegram-ID (только цифры):",
            reply_markup=cancel_markup
        )
        await state.update_data(manual_add_msg_id=msg.message_id)
        return

    # 4) запоминаем user_id и просим ввести имя профиля
    manual_user_id = int(user_id_text)
    await state.update_data(manual_user_id=manual_user_id)

    # удаляем сообщение пользователя (с цифрами)
    try:
        await message.delete()
    except:
        pass

    # просим ввести имя профиля (латиница, цифры, _ и -)
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

    # 1) удаляем запрос “введите имя профиля…”
    if prev_msg_id:
        try:
            await bot.delete_message(message.chat.id, prev_msg_id)
        except Exception:
            pass

    # 2) проверка отмены
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

    # 3) проверяем, что имя подходит под шаблон [A-Za-z0-9_-]{1,32}
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

        # повторно запросим
        msg2 = await bot.send_message(
            message.chat.id,
            "✏️ Введите корректное имя профиля (латиница, цифры, _ или -):",
            reply_markup=cancel_markup
        )
        await state.update_data(manual_add_msg_id=msg2.message_id)
        return

    # 4) Создаём сертификат через client.sh:
    result = await execute_script("1", client_name, "30")
    if result["returncode"] != 0:
        # Если что-то пошло не так, сообщаем админу
        await message.answer(
            f"❌ Ошибка при создании профиля <code>{client_name}</code>: {result['stderr']}",
            parse_mode="HTML"
        )
        await state.clear()
        return

    # 5) Сохраняем (ID ↔ profile_name) в SQLite
    save_profile_name(manual_user_id, client_name)

    # 6) Добавляем пользователя в approved_users.txt и users.txt
    approve_user(manual_user_id)
    save_user_id(manual_user_id)

    # 7) Уведомляем пользователя, что он получил VPN (если он уже в чате — дойдёт немедленно;
    #    если он зайдёт позже, при /start будет определён как approved)
    try:
        await safe_send_message(
            manual_user_id,
            f"✅ Ваша учётная запись VPN <b>{client_name}</b> создана администратором!\n\n"
            "Теперь вы можете писать боту и сразу получать конфиг.",
            parse_mode="HTML",
            reply_markup=create_user_menu(client_name, user_id=manual_user_id)
        )
    except Exception:
        # Вполне нормально, если юзер ещё не писал боту — safe_send_message проигнорирует.
        pass

    # 8) Отправляем временное сообщение админу и удаляем его через 1 секунду
    temp = await message.answer(
        "✅ Клиент успешно создан и подтверждён сразу! Пользователь может зайти в бот и сразу получить конфиги."
    )
    await asyncio.sleep(1)
    try:
        await temp.delete()
    except Exception:
        pass

    # 9) Теперь показываем главное меню
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
        try: await callback.answer("Нет прав!", show_alert=True)
        except: pass
        return

    # по-умолчанию открываем вкладку «Все»
    await show_users_tab(callback.from_user.id, "users_tab_all")
    try: await callback.answer()
    except: pass


# ==== Список пользователей с эмодзи ====
@dp.callback_query(lambda c: c.data == "users_menu")
async def users_menu(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        try: await callback.answer("Нет прав!", show_alert=True)
        except: pass
        return

    # по-умолчанию открываем вкладку «Все»
    await show_users_tab(callback.from_user.id, "users_tab_all")
    try: await callback.answer()
    except: pass

async def show_users_tab(chat_id: int, tab: str):
    # 1) получаем всех клиентов, заодно убираем antizapret-client
    raw_clients = await get_clients("openvpn")
    all_clients = [c for c in raw_clients if c != "antizapret-client"]

    # 2) строим множество онлайн-клиентов
    open_online = set(get_online_users_from_log().keys())
    wg_online   = set(get_online_wg_peers().keys())
    online_all  = open_online | wg_online

    # 3) в зависимости от таба выбираем подмножество
    if tab == "users_tab_all":
        clients = all_clients
        header  = "👥 <b>Все пользователи:</b>"
    elif tab == "users_tab_online":
        clients = [c for c in all_clients if c in online_all]
        header  = "🟢 <b>Сейчас онлайн:</b>"
    else:  # users_tab_expiring
        clients = []
        for c in all_clients:
            uid = get_user_id_by_name(c)
            info = get_cert_expiry_info(c) if uid else None
            if info and 0 <= info["days_left"] <= 7:
                clients.append(c)
        header = "⏳ <b>Истекают (≤7д):</b>"

    # 2) строим список рядов кнопок
    rows = []
    for c in clients:
        uid   = get_user_id_by_name(c)
        emoji = get_user_emoji(uid) if uid else ""
        if tab == "users_tab_expiring":
            days   = get_cert_expiry_info(c)["days_left"]
            status = f"⏳{days}д"
        else:
            status = "🟢" if c in online_all else "🔴"
        label = f"{emoji+' ' if emoji else ''}{status} {c}"
        cb    = f"manage_userid_{uid}" if uid else f"manage_user_{c}"
        rows.append([InlineKeyboardButton(text=label, callback_data=cb)])

    # 3) добавляем строку табов
    tab_row = make_users_tab_keyboard(tab).inline_keyboard[0]
    rows.append(tab_row)

    # 4) добавляем «Назад»
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")])

    # 5) создаём раз и навсегда новую клаву
    markup = InlineKeyboardMarkup(inline_keyboard=rows)

    # 6) показываем
    await show_menu(chat_id, header, markup)


@dp.callback_query(lambda c: c.data in {"users_tab_all","users_tab_online","users_tab_expiring"})
async def on_users_tab(callback: types.CallbackQuery):
    await show_users_tab(callback.from_user.id, callback.data)
    try: await callback.answer()
    except: pass




def create_wg_menu(client_name):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Обычный VPN",
                callback_data=f"info_wg_vpn_{client_name}"
            ),
            InlineKeyboardButton(
                text="Antizapret (Рекомендую)",
                callback_data=f"info_wg_antizapret_{client_name}"
            )
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"back_to_user_menu_{client_name}")]
    ])

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
        [InlineKeyboardButton(text="✅ Скачать конфиг", callback_data=f"download_wg_vpn_{client_name}")],
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
        [InlineKeyboardButton(text="✅ Скачать конфиг", callback_data=f"download_wg_antizapret_{client_name}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"get_wg_{client_name}")]
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await callback.answer()


def create_amnezia_menu(client_name):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Обычный VPN",
                callback_data=f"info_am_vpn_{client_name}"
            ),
            InlineKeyboardButton(
                text="Antizapret (Рекомендую)",
                callback_data=f"info_am_antizapret_{client_name}"
            )
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"back_to_user_menu_{client_name}")]
    ])

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

@dp.callback_query(lambda c: c.data.startswith("download_wg_"))
async def download_wg_config(callback: types.CallbackQuery):
    parts = callback.data.split("_", 3)
    _, _, wg_type, client_name = parts
    user_id = callback.from_user.id
    username = callback.from_user.username or "Без username"

    if wg_type == "vpn":
        file_path = f"/root/antizapret/client/wireguard/vpn/{FILEVPN_NAME} - Обычный VPN -{client_name}.conf"
    else:
        file_path = f"/root/antizapret/client/wireguard/antizapret/{FILEVPN_NAME} -{client_name}.conf"

    # Генерируем, если нет файла
    if not os.path.exists(file_path):
        subprocess.run(['/root/antizapret/client.sh', '4', client_name], check=True)

    try:
        await callback.message.delete()
    except Exception:
        pass
    await delete_last_menus(user_id)

    if os.path.exists(file_path):
        await bot.send_document(user_id, FSInputFile(file_path), caption=f"🔐 {os.path.basename(file_path)}")
        await notify_admin_download(user_id, username, os.path.basename(file_path), "wg")
    else:
        await bot.send_message(user_id, "❌ Файл не найден")

    await show_menu(
        user_id,
        f"Меню пользователя <b>{client_name}</b>:",
        create_user_menu(client_name, back_callback="users_menu", is_admin=(user_id == ADMIN_ID), user_id=user_id)
    )

    await callback.answer()


@dp.callback_query(lambda c: c.data.startswith("download_wg_"))
async def download_wg_config(callback: types.CallbackQuery):
    parts = callback.data.split("_", 3)
    _, _, wg_type, client_name = parts
    user_id = callback.from_user.id
    username = callback.from_user.username or "Без username"

    if wg_type == "vpn":
        file_path = f"/root/antizapret/client/wireguard/vpn/{FILEVPN_NAME} - Обычный VPN -{client_name}.conf"
    else:
        file_path = f"/root/antizapret/client/wireguard/antizapret/{FILEVPN_NAME} -{client_name}.conf"

    # Генерируем если нет файла
    if not os.path.exists(file_path):
        subprocess.run(['/root/antizapret/client.sh', '4', client_name], check=True)

    # Удаляем старое меню
    try:
        await callback.message.delete()
    except Exception:
        pass
    await delete_last_menus(user_id)

    # Отправляем файл если есть
    if os.path.exists(file_path):
        await bot.send_document(user_id, FSInputFile(file_path), caption=f"🔐 {os.path.basename(file_path)}")
        await notify_admin_download(user_id, username, os.path.basename(file_path), "wg")
    else:
        await bot.send_message(user_id, "❌ Файл не найден")

    await show_menu(
        user_id,
        f"Меню пользователя <b>{client_name}</b>:",
        create_user_menu(client_name, back_callback="users_menu", is_admin=(user_id == ADMIN_ID), user_id=user_id)
    )
    await callback.answer()






# ==== Админ: установка смайла ====
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

    # Инлайн-кнопка отмены
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
    # Сохраним id сообщения для удаления
    await state.update_data(input_message_id=msg.message_id)

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

    # Удаляем сообщение с инструкцией
    try:
        await message.bot.delete_message(message.from_user.id, input_msg_id)
    except:
        pass

    emoji = message.text.strip()
    if emoji == "❌":
        set_user_emoji(target_user_id, "")
        text = "Смайл убран."
    else:
        if len(emoji) > 2:
            warn_msg = await message.answer("❌ Слишком длинный смайл!")
            await asyncio.sleep(2)
            await warn_msg.delete()
            return
        set_user_emoji(target_user_id, emoji)
        text = f"Установлен смайл: {emoji}"

    # Вывести уведомление, потом удалить через 2 сек
    notif = await message.answer(text)
    await asyncio.sleep(2)
    try:
        await notif.delete()
    except:
        pass

    await show_menu(
        message.from_user.id,
        f"Меню пользователя <b>{client_name}</b>:",
        create_user_menu(client_name, back_callback="users_menu", is_admin=True, user_id=target_user_id)
    )
    await state.clear()

    
# ==== Выдача WireGuard ====
@dp.callback_query(lambda c: c.data.startswith("get_wg_"))
async def send_wg_config(callback: types.CallbackQuery):
    client_name = callback.data[len("get_wg_"):]
    user_id = callback.from_user.id
    await execute_script("4", client_name)
    file_path = find_conf("/root/antizapret/client/wireguard", client_name)
    if not file_path:
        await callback.answer("❌ Файл WG не найден", show_alert=True)
        return
    await bot.send_document(
        user_id,
        FSInputFile(file_path),
        caption=f"🔐 WireGuard: {os.path.basename(file_path)}"
    )
    await callback.answer("✅ WireGuard-конфиг отправлен.")

    
def find_conf(base_dir, client_name):
    # Ищет во всех подпапках и по всем шаблонам
    patterns = [
        f"{base_dir}/*/*{client_name}*.conf",
        f"{base_dir}/*{client_name}*.conf",
    ]
    for pattern in patterns:
        files = glob.glob(pattern)
        if files:
            return files[0]
    return None    


def find_wg_conf(client_name):
    patterns = [
        f"/root/antizapret/client/wireguard/*/*{client_name}*.conf",
        f"/root/antizapret/client/wireguard/*{client_name}*.conf",
    ]
    for pattern in patterns:
        files = glob.glob(pattern)
        if files:
            return files[0]
    return None

   

# Новый вариант — по user_id
@dp.callback_query(lambda c: c.data.startswith("manage_userid_"))
async def manage_user_by_id(callback: types.CallbackQuery):
    target_user_id = int(callback.data.split("_")[-1])
    client_name = get_profile_name(target_user_id)

    await show_menu(
        callback.from_user.id,
        f"Управление клиентом <b>{client_name}</b>:",
        create_user_menu(
            client_name,
            back_callback="users_menu",
            is_admin=True,
            user_id=target_user_id   # <-- передали Telegram-ID
        )
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("manage_user_"))
async def manage_user(callback: types.CallbackQuery):
    client_name = callback.data.split("_",2)[-1]
    target_user_id = get_user_id_by_name(client_name)
    await show_menu(
        callback.from_user.id,
        f"Управление клиентом <b>{client_name}</b>:",
        create_user_menu(
            client_name,
            back_callback="users_menu",
            is_admin=(callback.from_user.id == ADMIN_ID),
            user_id=target_user_id  # <-- здесь
        )
    )
    await callback.answer()


def get_user_id_by_name(profile_name):
    conn = sqlite3.connect("/root/vpn.db")
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE profile_name=?", (profile_name,))
    res = cur.fetchone()
    conn.close()
    return res[0] if res else None



@dp.callback_query(lambda c: c.data == "7")
async def recreate_files(callback: types.CallbackQuery, state: FSMContext):
    result = await execute_script("7")
    if result["returncode"] == 0:
        await callback.message.edit_text("✅ Файлы успешно пересозданы!")
        await asyncio.sleep(1)
        try:
            await callback.message.delete()
        except Exception:
            pass
        # Удаляем все предыдущие меню!
        await delete_last_menus(callback.from_user.id)
        await state.clear()
        # Делаем с инфой сервера если админ
        if callback.from_user.id == ADMIN_ID:
            stats = get_server_info()
            menu_text = stats + "\n<b>Главное меню:</b>"
        else:
            menu_text = "Главное меню:"
        msg = await bot.send_message(callback.from_user.id, menu_text, reply_markup=create_main_menu(), parse_mode="HTML")
        set_last_menu_id(callback.from_user.id, msg.message_id)
    else:
        await callback.message.edit_text(f"❌ Ошибка: {result['stderr']}")
    await callback.answer()




@dp.callback_query(lambda c: c.data == "announce_menu")
async def admin_announce_menu(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await callback.answer("Нет прав!", show_alert=True)
        return

    # Удаляем все старые меню (ПРАВИЛЬНО!)
    await delete_last_menus(user_id)

    # Одно новое через show_menu!
    msg = await show_menu(
        user_id,
        "✏️ Введите текст объявления:",
        InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]])
    )
    await state.update_data(announce_msg_id=msg.message_id)
    await state.set_state(AdminAnnounce.waiting_for_text)
    await callback.answer()



 
@dp.message(AdminAnnounce.waiting_for_text)
async def process_announce_text(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    announce_msg_id = data.get("announce_msg_id")

    # Сразу удаляем как пользовательское сообщение, так и форму
    try:
        await bot.delete_message(user_id, message.message_id)
        if announce_msg_id:
            await bot.delete_message(user_id, announce_msg_id)
    except Exception:
        pass

    text = message.text.strip()
    if text == "⬅️ Назад":
        # Очистили всё — теперь просто показываем главное меню через show_menu
        await state.clear()
        stats = get_server_info()
        await show_menu(user_id, stats + "\n<b>Главное меню:</b>", create_main_menu())
        return

    if not text:
        # если пустой ввод — заново показываем форму
        msg = await show_menu(
            user_id,
            "✏️ Текст не может быть пустым. Введите текст объявления:",
            InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]]
            )
        )
        await state.update_data(announce_msg_id=msg.message_id)
        return

    # иначе рассылаем
    sent, failed = await announce_all(text)
    await bot.send_message(user_id, f"✅ Отправлено: {sent}, не доставлено: {failed}")
    await state.clear()
    stats = get_server_info()
    await show_menu(user_id, stats + "\n<b>Главное меню:</b>", create_main_menu())



async def announce_all(text):
    if not os.path.exists(USERS_FILE):
        return 0, 0

    sent, failed = 0, 0
    with open(USERS_FILE) as f:
        users = [line.strip() for line in f if line.strip().isdigit()]
    for uid in users:
        try:
            await bot.send_message(uid, f"📢 <b>Объявление:</b>\n\n{text}", parse_mode="HTML")
            sent += 1
        except Exception as e:
            failed += 1
            print(f"Не удалось отправить {uid}: {e}")

    return sent, failed


@dp.callback_query(lambda c: c.data == "8")
async def backup_files(callback: types.CallbackQuery):
    await callback.message.edit_text("⏳ Создаю бэкап...")
    result = await execute_script("8")
    if result["returncode"] == 0:
        if await send_backup(callback.from_user.id):
            await callback.message.delete()
            # То же самое, меню со статистикой!
            if callback.from_user.id == ADMIN_ID:
                stats = get_server_info()
                menu_text = stats + "\n<b>Главное меню:</b>"
            else:
                menu_text = "Главное меню:"
            await bot.send_message(callback.from_user.id, menu_text, reply_markup=create_main_menu(), parse_mode="HTML")
        else:
            await callback.message.edit_text("❌ Не удалось отправить бэкап")
    else:
        await callback.message.edit_text(f"❌ Ошибка при создании бэкапа: {result['stderr']}")
    await callback.answer()



@dp.callback_query(lambda c: c.data == "del_user")
async def del_user_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    await delete_last_menus(user_id)  # ← ОБЯЗАТЕЛЬНО сюда!
    clients = await get_clients("openvpn")
    if not clients:
        await show_menu(user_id, "❌ Нет пользователей для удаления.", create_main_menu())
        return
    keyboard = [
        [InlineKeyboardButton(text=client, callback_data=f"ask_del_{client}")]
        for client in clients
    ]
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="add_del_menu")])
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    msg = await bot.send_message(user_id, "Выберите пользователя для удаления:", reply_markup=markup)
    set_last_menu_id(user_id, msg.message_id)
    await callback.answer()






@dp.callback_query(lambda c: c.data.startswith("ask_del_"))
async def ask_delete_user(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    client_name = callback.data.split("_", 2)[-1]
    await delete_last_menus(user_id)  # ← добавь это!
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"confirm_del_{client_name}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="del_user")]
        ]
    )
    await bot.send_message(user_id, f"Удалить пользователя <b>{client_name}</b>?", reply_markup=markup, parse_mode="HTML")
    await callback.answer()



@dp.callback_query(lambda c: c.data.startswith("confirm_del_"))
async def confirm_delete_user(callback: types.CallbackQuery):
    client_name = callback.data.split("_", 2)[-1]
    admin_id = callback.from_user.id

    # 1) Узнаём user_id по имени клиента
    target_user_id = get_user_id_by_name(client_name)

    # 2) Выполняем удаление сертификата
    result = await execute_script("2", client_name)

    # 3) Убираем сообщение с подтверждением
    try:
        await callback.message.delete()
    except Exception:
        pass

    # 4) Если скрипт удалил клиента без ошибок, чистим файлы с ID
    if result["returncode"] == 0:
        if target_user_id is not None:
            remove_approved_user(target_user_id)
            remove_user_id(target_user_id)
            # Опционально: удаляем профиль из БД
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("DELETE FROM users WHERE id=?", (target_user_id,))
            conn.commit()
            conn.close()

        stats = get_server_info()
        await show_menu(
            admin_id,
            f"✅ Пользователь <b>{client_name}</b> удалён.\n\n{stats}\n<b>Главное меню:</b>",
            create_main_menu()
        )
    else:
        stats = get_server_info()
        await show_menu(
            admin_id,
            f"❌ Ошибка удаления: {result['stderr']}\n\n{stats}\n<b>Главное меню:</b>",
            create_main_menu()
        )

    await callback.answer()



def get_cert_expiry_days(cert_path):
    try:
        result = subprocess.run(
            ["openssl", "x509", "-in", cert_path, "-noout", "-enddate"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        if result.returncode != 0:
            return 30  # fallback, если не нашли сертификат
        not_after = result.stdout.strip().replace("notAfter=", "")
        dt = datetime.strptime(not_after, '%b %d %H:%M:%S %Y %Z').replace(tzinfo=timezone.utc)
        days_left = (dt - datetime.now(timezone.utc)).days
        return max(days_left, 1)
    except Exception as e:
        print(f"Ошибка чтения срока сертификата: {e}")
        return 30  # fallback

def create_openvpn_menu():
    """Создает меню OpenVPN в виде InlineKeyboardMarkup."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🆕 Создать клиента", callback_data="1"),
                InlineKeyboardButton(text="❌ Удалить клиента", callback_data="2"),
            ],
            [
                InlineKeyboardButton(text="📝 Список клиентов", callback_data="3"),
                InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu"),
            ],
        ]
    )

@dp.callback_query(lambda c: c.data == "rename_cancel")
async def rename_cancel(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id

    # Сохраняем имя профиля до очистки состояния
    data = await state.get_data()
    client_name = data.get("old_username")

    await state.clear()
    await delete_last_menus(user_id)

    is_admin = (user_id == ADMIN_ID)
    await show_menu(
        user_id,
        f"Меню пользователя <b>{client_name}</b>:",
        create_user_menu(client_name, back_callback="users_menu", is_admin=is_admin)
    )

    await callback.answer()







@dp.callback_query(lambda c: c.data.startswith("rename_profile_"))
async def start_rename_profile(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    old_username = callback.data.split("_", 2)[-1]
    await state.update_data(old_username=old_username)
    # Удаляем все прошлые меню
    await delete_last_menus(user_id)
    try:
        await callback.message.delete()  # ВАЖНО: удаляем меню управления пользователем!
    except Exception:
        pass

    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="rename_cancel")]
        ]
    )
    msg = await bot.send_message(
        user_id,
        f"Введите новое имя для профиля (сейчас: <b>{old_username}</b>):",
        parse_mode="HTML",
        reply_markup=markup
    )
    set_last_menu_id(user_id, msg.message_id)
    await state.set_state(RenameProfile.waiting_for_new_name)
    await callback.answer()



async def show_menu(user_id, text, reply_markup, parse_mode="HTML"):
    await delete_last_menus(user_id)  # Удаляем все прошлые меню этого юзера
    msg = await bot.send_message(user_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
    set_last_menu_id(user_id, msg.message_id)
    return msg


@dp.callback_query(lambda c: c.data.startswith("get_amnezia_"))
async def send_amnezia_config(callback: types.CallbackQuery):
    client_name = callback.data[len("get_amnezia_"):]
    user_id = callback.from_user.id
    # Всегда создавать/пересоздавать перед выдачей!
    await execute_script("4", client_name)
    file_path = find_conf("/root/antizapret/client/amneziawg", client_name)
    if not file_path:
        await callback.answer("❌ Файл Amnezia не найден", show_alert=True)
        return
    await bot.send_document(
        user_id,
        FSInputFile(file_path),
        caption=f"🔐 Amnezia: {os.path.basename(file_path)}"
    )
    await callback.answer("✅ Amnezia-конфиг отправлен.")



@dp.callback_query(lambda c: c.data.startswith("download_am_"))
async def download_amnezia_config(callback: types.CallbackQuery):
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
        create_user_menu(client_name, back_callback="users_menu", is_admin=(user_id == ADMIN_ID), user_id=user_id)
    )

    await callback.answer()




# Новые функции для создания меню выбора
def create_openvpn_config_menu(client_name: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="VPN", callback_data=f"openvpn_config_vpn_{client_name}"
                ),
                InlineKeyboardButton(
                    text="Antizapret",
                    callback_data=f"openvpn_config_antizapret_{client_name}",
                ),
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_client_list")],
        ]
    )


def create_openvpn_protocol_menu(interface: str, client_name: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Стандартный (auto)",
                    callback_data=f"send_ovpn_{interface}_default_{client_name}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="TCP", callback_data=f"send_ovpn_{interface}_tcp_{client_name}"
                ),
                InlineKeyboardButton(
                    text="UDP", callback_data=f"send_ovpn_{interface}_udp_{client_name}"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Назад",
                    callback_data=f"back_to_interface_{interface}_{client_name}",
                )
            ],
        ]
    )

def create_client_list_keyboard(clients, page, total_pages, vpn_type, action):
    """Создает клавиатуру с клиентами VPN."""
    buttons = []
    start_idx = (page - 1) * ITEMS_PER_PAGE
    end_idx = start_idx + ITEMS_PER_PAGE

    for client in clients[start_idx:end_idx]:
        prefix = "delete" if action == "delete" else "client"
        callback_data = f"{action}_{vpn_type}_{client}"

        if action == "delete":
            callback_data = f"delete_{vpn_type}_{client}"
        else:  # действие "client" (выдача конфигурационного файла)
            callback_data = f"client_{vpn_type}_{client}"

        buttons.append([InlineKeyboardButton(text=client, callback_data=callback_data)])

    pagination = []
    if page > 1:
        pagination.append(
            InlineKeyboardButton(
                text="⬅️ Предыдущая", callback_data=f"page_{action}_{vpn_type}_{page-1}"
            )
        )
    if page < total_pages:
        pagination.append(
            InlineKeyboardButton(
                text="Следующая ➡️", callback_data=f"page_{action}_{vpn_type}_{page+1}"
            )
        )

    if pagination:
        buttons.append(pagination)

    buttons.append(
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{vpn_type}_menu")]
    )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def create_confirmation_keyboard(client_name, vpn_type):
    """Создает клавиатуру подтверждения удаления клиента."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Подтвердить",
                    callback_data=f"confirm_{vpn_type}_{client_name}",
                ),
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_delete"),
            ]
        ]
    )

def get_user_traffic(client_name):
    log_files = [
        "/etc/openvpn/server/logs/antizapret-tcp-status.log",
        "/etc/openvpn/server/logs/antizapret-udp-status.log",
        "/etc/openvpn/server/logs/vpn-tcp-status.log",
        "/etc/openvpn/server/logs/vpn-udp-status.log",
    ]
    total_received = 0
    total_sent = 0
    for log_path in log_files:
        try:
            if os.path.exists(log_path):
                with open(log_path) as f:
                    for line in f:
                        if line.startswith("CLIENT_LIST"):
                            parts = line.strip().split(",")
                            if len(parts) > 4 and parts[1] == client_name:
                                # parts[3]: Bytes received
                                # parts[4]: Bytes sent
                                try:
                                    total_received += int(parts[3])
                                    total_sent += int(parts[4])
                                except Exception:
                                    continue
        except Exception:
            continue
    # Переводим в Гб (1 Гб = 1024^3 байт)
    gb_received = total_received / (1024**3)
    gb_sent = total_sent / (1024**3)
    return round(gb_sent, 2), round(gb_received, 2)

@dp.callback_query(lambda c: c.data.startswith("renew_user_"))
async def renew_user_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет доступа!", show_alert=True)
        return

    client_name = callback.data.split("_", 2)[-1]
    target_user_id = get_user_id_by_name(client_name)
    if target_user_id is None:
        await callback.answer("Пользователь не найден!", show_alert=True)
        return

    # Сохраняем и client_name, и реальный Telegram-ID пользователя
    await state.update_data(client_name=client_name, target_user_id=target_user_id)

    # Удаляем старое меню у админа
    await delete_last_menus(callback.from_user.id)
    try:
        await callback.message.delete()
    except Exception:
        pass

    # Запрашиваем у админа новый срок
    msg = await bot.send_message(
        callback.from_user.id,
        (
            f"✏️ <b>Установить срок действия</b>\n\n"
            f"Введите новый срок действия <b>(в днях)</b> для пользователя <code>{client_name}</code>:\n"
            f"<b>⚠️ Текущий срок будет заменён новым!</b>"
        ),
        parse_mode="HTML",
        reply_markup=cancel_markup
    )
    # Сохраним сообщение для удаления позже
    await state.update_data(renew_msg_ids=[msg.message_id])
    await state.set_state(VPNSetup.entering_days)
    await callback.answer()





from datetime import datetime, timezone

def get_cert_expiry_info(client_name):
    cert_path = f"/etc/openvpn/easyrsa3/pki/issued/{client_name}.crt"
    try:
        result = subprocess.run(
            ["openssl", "x509", "-in", cert_path, "-noout", "-enddate"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        if result.returncode != 0:
            return None
        not_after = result.stdout.strip().replace("notAfter=", "")
        date_to = datetime.strptime(not_after, '%b %d %H:%M:%S %Y %Z').replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        days_left = (date_to - now).days

        # Теперь узнаём дату выпуска (startdate)
        result2 = subprocess.run(
            ["openssl", "x509", "-in", cert_path, "-noout", "-startdate"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        not_before = result2.stdout.strip().replace("notBefore=", "")
        date_from = datetime.strptime(not_before, '%b %d %H:%M:%S %Y %Z').replace(tzinfo=timezone.utc)

        return {
            "date_from": date_from,
            "date_to": date_to,
            "days_left": days_left
        }
    except Exception as e:
        print(f"Ошибка чтения срока сертификата: {e}")
        return None


@dp.message(VPNSetup.entering_days)
async def process_renew_days(message: types.Message, state: FSMContext):
    data = await state.get_data()
    admin_id       = message.from_user.id
    target_user_id = data.get("target_user_id")
    client_name    = data.get("client_name")
    renew_msg_ids  = data.get("renew_msg_ids", [])

    # Удаляем индикаторы прогресса
    for mid in set(renew_msg_ids):
        try:
            await bot.delete_message(admin_id, mid)
        except:
            pass
    await state.update_data(renew_msg_ids=[])

    text = message.text.strip()
    if text.lower() in ("❌ отмена", "отмена"):
        await state.clear()
        await show_menu(
            admin_id,
            f"Меню пользователя <b>{client_name}</b>:",
            create_user_menu(client_name, back_callback="users_menu", is_admin=True, user_id=target_user_id)
        )
        return

    if not text.isdigit() or int(text) < 1:
        warn = await message.answer("❌ Введите целое число дней (>0)", reply_markup=cancel_markup)
        await asyncio.sleep(1.5)
        try: await warn.delete()
        except: pass
        return

    days = int(text)
    # Показать прогресс
    msg_wait = await message.answer(
        f"⏳ Продление сертификата <b>{client_name}</b> на {days} дней...",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.update_data(renew_msg_ids=[msg_wait.message_id])

    # Выполнить продление
    result = await execute_script("9", client_name, str(days))

    # Убрать прогресс
    try:
        await bot.delete_message(admin_id, msg_wait.message_id)
    except:
        pass

    # Собрать статус сертификата
    cert_info = get_cert_expiry_info(client_name)
    if cert_info:
        date_to_str = cert_info["date_to"].strftime('%d.%m.%Y')
        days_left   = cert_info["days_left"]
        status_text = f"до <b>{date_to_str}</b> (осталось <b>{days_left}</b> д.)"
    else:
        status_text = "точную дату определить не удалось"

    if result["returncode"] == 0:
        # Сообщение пользователю
        await bot.send_message(
            target_user_id,
            f"🎉 Поздравляю, твой VPN продлён на <b>{days}</b> дней — {status_text}!",
            parse_mode="HTML"
        )
        # Сообщение админу
        await bot.send_message(
            admin_id,
            f"✅ Пользователь <b>{client_name}</b> продлён {status_text}.",
            parse_mode="HTML"
        )
    else:
        # Ошибка админу
        await bot.send_message(
            admin_id,
            f"❌ Ошибка продления: {result['stderr']}",
            parse_mode="HTML"
        )

    # Вернуть админа в меню
    await show_menu(
        admin_id,
        f"Меню пользователя <b>{client_name}</b>:",
        create_user_menu(client_name, back_callback="users_menu", is_admin=True, user_id=target_user_id)
    )
    await state.clear()



# ==== Меню управления пользователем (с эмодзи и WG/Amnezia кнопками) ====
def create_user_menu(
    client_name: str,
    *,
    back_callback: str | None = None,
    is_admin: bool = False,
    user_id: int | None = None
) -> InlineKeyboardMarkup:
    """
    Собирает меню управления для данного клиента.
    - client_name — название профиля.
    - back_callback — callback_data кнопки «⬅️ Назад».
    - is_admin — добавлять ли админские кнопки.
    - user_id — Telegram-ID клиента (для url-кнопки).
    """

    keyboard: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="Статистика",
                callback_data=f"user_stats_{client_name}"
            )
        ],
        [
            InlineKeyboardButton(
                text="Получить OpenVPN",
                callback_data=f"select_openvpn_{client_name}"
            )
        ],
        [
            InlineKeyboardButton(
                text="Получить VPN WireGuard",
                callback_data=f"get_wg_{client_name}"
            )
        ],
        [
            InlineKeyboardButton(
                text="🦄 Получить VPN Amnezia",
                callback_data=f"get_amnezia_{client_name}"
            )
        ],
        [
            InlineKeyboardButton(
                text="📬 Получить VPN VLESS",
                callback_data=f"get_vless_{client_name}"
            )
        ],
    ]

    if is_admin:
        # админские опции
        keyboard.append([
            InlineKeyboardButton(
                text="✏️ Изменить имя профиля",
                callback_data=f"rename_profile_{client_name}"
            )
        ])
        keyboard.append([
            InlineKeyboardButton(
                text="🤡 Установить смайл",
                callback_data=f"set_emoji_{client_name}"
            )
        ])
        keyboard.append([
            InlineKeyboardButton(
                text="✏️ Установить срок действия",
                callback_data=f"renew_user_{client_name}"
            )
        ])
        keyboard.append([
            InlineKeyboardButton(
                text="❌ Удалить пользователя",
                callback_data=f"delete_user_{client_name}"
            )
        ])

        # кнопка «Назад»
        if back_callback:
            keyboard.append([
                InlineKeyboardButton(
                    text="⬅️ Назад",
                    callback_data=back_callback
                )
            ])
    else:
        # для обычного пользователя
        keyboard.append([
            InlineKeyboardButton(
                text="Поддержка",
                url="https://t.me/chechenecp"
            )
        ])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)



@dp.callback_query(lambda c: c.data.startswith("delete_user_"))
async def delete_user_from_user_menu(callback: types.CallbackQuery, state: FSMContext):
    client_name = callback.data.split("_", 2)[-1]
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"confirm_del_{client_name}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"manage_user_{client_name}")]
        ]
    )
    await callback.message.edit_text(
        f"Удалить пользователя <b>{client_name}</b>?",
        reply_markup=markup,
        parse_mode="HTML"
    )
    await callback.answer()



#Удаление пользователя
@dp.message(RenameProfile.waiting_for_new_name)
async def handle_new_username(message: types.Message, state: FSMContext):
    new_username = message.text.strip()
    data = await state.get_data()
    old_username = data.get("old_username")

    # Проверка нового имени
    if not re.match(r"^[a-zA-Z0-9_-]{1,32}$", new_username):
        await message.answer("❌ Некорректное имя! Используй только буквы, цифры, _ и -.")
        await state.clear()
        return

    # Получить user_id по старому имени
    user_id = None
    conn = sqlite3.connect("/root/vpn.db")
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE profile_name=?", (old_username,))
    res = cur.fetchone()
    if res:
        user_id = res[0]
    conn.close()
    if not user_id:
        await message.answer("❌ Пользователь по старому имени не найден!")
        await state.clear()
        return

    # Узнаём сколько дней осталось у старого сертификата
    old_cert_path = f"/etc/openvpn/easyrsa3/pki/issued/{old_username}.crt"
    days_left = get_cert_expiry_days(old_cert_path)

    # Удаляем старый сертификат
    result_del = await execute_script("2", old_username)
    if result_del["returncode"] != 0:
        stderr = result_del.get("stderr", "")
        await message.answer(f"❌ Ошибка удаления старого профиля: {stderr}")
        await state.clear()
        return

    # Создаём новый сертификат
    result_add = await execute_script("1", new_username, str(days_left))
    if result_add["returncode"] != 0:
        await message.answer(f"❌ Ошибка создания нового профиля: {result_add['stderr']}")
        await state.clear()
        return

    # Универсально обновляем имя в базе
    save_profile_name(user_id, new_username)

    await delete_last_menus(message.from_user.id)
    await show_menu(
        message.from_user.id,
        "✅ Имя профиля успешно изменено!\n\nТеперь вы можете скачать новый конфиг через меню кнопкой 📥 <b>Получить конфиг OpenVPN</b>.",
        create_user_menu(new_username, back_callback="users_menu", is_admin=(message.from_user.id == ADMIN_ID))
    )
    await state.clear()









def get_cert_expiry_days_for_user(client_name):
    cert_path = f"/etc/openvpn/client/keys/{client_name}.crt"
    return get_cert_expiry_days(cert_path)

async def get_config_stats(client_name):
    days_left = get_cert_expiry_days_for_user(client_name)
    now = datetime.now()
    date_from = now
    date_to = now + timedelta(days=days_left)
    return {
        "date_from": date_from.strftime("%d.%m.%Y"),
        "date_to": date_to.strftime("%d.%m.%Y"),
        "days_left": days_left
    }

#Статистика пользователя
@dp.callback_query(lambda c: c.data.startswith("user_stats_"))
async def user_stats(callback: types.CallbackQuery):
    client_name = callback.data[len("user_stats_"):]
    user_id = callback.from_user.id

    # Собираем блок с информацией о сертификате
    cert_info = get_cert_expiry_info(client_name)
    if cert_info:
        date_from_str = cert_info["date_from"].strftime('%d.%m.%Y')
        date_to_str   = cert_info["date_to"].strftime('%d.%m.%Y')
        days_left     = cert_info["days_left"]
        cert_block = (
            f"<b>Срок действия:</b>\n"
            f"• С {date_from_str} по {date_to_str}\n"
            f"• Осталось <b>{days_left}</b> дней\n"
        )
    else:
        cert_block = "<b>Срок действия:</b> неизвестно\n"
    text = cert_block

    # 1) Удаляем текущее сообщение (например, окно «Выберите тип» или старый stats-экран)
    await delete_last_menus(user_id)
    try:
        await callback.message.delete()
    except Exception:
        pass

    # 2) Показываем новое «Меню управления клиентом» через show_menu():
    if user_id == ADMIN_ID:
        # Админу — с кнопкой «⬅️ Назад» к списку пользователей
        await show_menu(
            user_id,
            text,
            create_user_menu(client_name, back_callback="users_menu", is_admin=True)
        )
    else:
        # Обычному юзеру — без кнопки «Назад»
        await show_menu(
            user_id,
            text,
            create_user_menu(client_name, is_admin=False)
        )

    await callback.answer()






async def execute_script(option: str, client_name: str = None, days: str = None):
    script_path = "/root/antizapret/client.sh"
    if not os.path.exists(script_path):
        return {
            "returncode": 1,
            "stdout": "",
            "stderr": f"❌ Файл {script_path} не найден!",
        }
    command = f"{script_path} {option}"
    if option not in ["8", "7"] and client_name:
        command += f" {client_name}"
        if days and option == "1" or option == "9":
            command += f" {days}"
    try:
        env = os.environ.copy()
        env["PATH"] = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        stdout, stderr = await process.communicate()
        # Добавь эти строки для дебага!
        print("==[DEBUG EXEC]==")
        print("COMMAND:", command)
        print("RET:", process.returncode)
        print("STDOUT:", stdout.decode())
        print("STDERR:", stderr.decode())
        print("==[END DEBUG]==")
        return {
            "returncode": process.returncode,
            "stdout": stdout.decode().strip(),
            "stderr": stderr.decode().strip(),
        }
    except Exception as e:
        return {
            "returncode": 1,
            "stdout": "",
            "stderr": f"❌ Ошибка при выполнении скрипта: {str(e)}",
        }


async def send_single_config(chat_id: int, path: str, caption: str):
    if os.path.exists(path):
        await bot.send_document(
            chat_id, document=FSInputFile(path), caption=f"🔐 {caption}"
        )
        return True
    return False


#Кто онлайн
def get_online_users_from_log():
    """
    Собирает CLIENT_LIST из всех четырёх status-файлов OpenVPN
    и возвращает всех реально подключённых клиентов.
    """
    status_files = [
        "/etc/openvpn/server/logs/antizapret-tcp-status.log",
        "/etc/openvpn/server/logs/antizapret-udp-status.log",
        "/etc/openvpn/server/logs/vpn-tcp-status.log",
        "/etc/openvpn/server/logs/vpn-udp-status.log",
    ]
    users = {}  # client_name -> "OpenVPN"

    for path in status_files:
        if not os.path.exists(path):
            continue
        with open(path) as f:
            for line in f:
                # CLIENT_LIST — это все живые сессии в момент снимка
                if not line.startswith("CLIENT_LIST,"):
                    continue
                parts = line.strip().split(",")
                if len(parts) >= 2 and parts[1]:
                    users[parts[1]] = "OpenVPN"

    return users

def get_online_wg_peers():
    """
    Возвращает всех реально подключённых WireGuard/Amnezia-клиентов:
    берём только те строки wg show all latest-handshakes, где
    ts != 0, и сопоставляем pubkey→client по .conf-файлам.
    """
    peers = {}
    try:
        out = subprocess.run(
            ["wg", "show", "all", "latest-handshakes"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True
        ).stdout
        for line in out.splitlines():
            parts = line.split()
            # пропускаем всё, что не “<pubkey> <timestamp>”
            if len(parts) != 2:
                continue
            pubkey, ts = parts
            if ts == "0":
                continue

            # ищем, в каком .conf встречается этот pubkey
            for base_dir, label in [
                ("/root/antizapret/client/amneziawg", "Amnezia"),
                ("/root/antizapret/client/wireguard", "WG")
            ]:
                found = False
                for root, _, files in os.walk(base_dir):
                    for fn in files:
                        if not fn.endswith(".conf"):
                            continue
                        try:
                            with open(os.path.join(root, fn), encoding="utf-8", errors="ignore") as cf:
                                if pubkey in cf.read():
                                    client = fn.rsplit("-", 1)[-1].rsplit(".", 1)[0]
                                    peers[client] = label
                                    found = True
                                    break
                        except Exception:
                            pass
                    if found:
                        break
                if found:
                    break
    except Exception as e:
        print(f"[ERROR] wg show: {e}")
    return peers

@dp.callback_query(lambda c: c.data == "who_online")
async def who_online(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    # Получаем OpenVPN и WG/Amnezia
    openvpn_map = get_online_users_from_log()  # {client_name: "OpenVPN"}
    wg_map      = get_online_wg_peers()       # {client_name: "WG" или "Amnezia"}

    # Объединяем OpenVPN и WG-клиентов
    merged = dict(openvpn_map)
    for client, proto in wg_map.items():
        if client not in merged:
            merged[client] = proto

    # Если никого нет — уведомляем и возвращаем в главное меню
    if not merged:
        try:
            await callback.message.edit_text("❌ Сейчас нет никого онлайн")
        except:
            try:
                await callback.message.delete()
                await callback.bot.send_message(user_id, "❌ Сейчас нет никого онлайн")
            except:
                pass
        await asyncio.sleep(2)
        try:
            await callback.message.delete()
        except:
            pass
        stats = get_server_info()
        await callback.bot.send_message(
            user_id,
            stats + "\n<b>Главное меню:</b>",
            reply_markup=create_main_menu(),
            parse_mode="HTML"
        )
        await callback.answer()
        return

    # Удаляем старое меню
    try:
        await callback.message.delete()
    except:
        pass

    # Строим новое с кнопками и смайликами пользователей
    buttons = []
    text_lines = ["🟢 <b>Кто в сети:</b>"]
    for client in merged.keys():
        # Получаем Telegram-ID пользователя по имени профиля
        uid = get_user_id_by_name(client)
        # Подтягиваем смайлик (или пустую строку, если не задан)
        emoji = get_user_emoji(uid) if uid else ""
        # Склеиваем метку кнопки
        label = f"{emoji + ' ' if emoji else ''}{client}"
        buttons.append([
            InlineKeyboardButton(text=label, callback_data=f"manage_online_{client}")
        ])

    # Кнопка "Назад"
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)

    # Отправляем сообщение
    await callback.bot.send_message(
        user_id,
        "\n".join(text_lines),
        parse_mode="HTML",
        reply_markup=keyboard
    )
    await callback.answer()







@dp.callback_query(lambda c: c.data.startswith("manage_online_"))
async def manage_online_user(callback: types.CallbackQuery):
    client_name = callback.data[len("manage_online_"):]
    user_id = callback.from_user.id

    # Удаляем старые меню
    await delete_last_menus(user_id)
    try:
        await callback.message.delete()
    except:
        pass

    # Везде используем единый create_user_menu, но с back_callback="who_online"
    await show_menu(
        user_id,
        f"Управление клиентом <b>{client_name}</b>:",
        create_user_menu(client_name, back_callback="who_online", is_admin=(user_id == ADMIN_ID))
    )
    await callback.answer()






@dp.message(Command("start"))
async def start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    await delete_last_menus(user_id)  # ← в самом начале

    # Удаляем ВСЕ последние свои меню (N штук)
    for mid in get_last_menu_ids(user_id):
        try:
            await bot.delete_message(user_id, mid)
        except Exception:
            pass

    # Дальше как обычно:
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
        "У вас нет доступа к VPN. Чтобы получить доступ — отправьте заявку на одобрение администратором:", reply_markup=markup)
    set_last_menu_id(user_id, msg.message_id)





@dp.callback_query(lambda c: c.data == "send_request")
async def send_request(callback: types.CallbackQuery):
    print("[SEND_REQUEST] send_request вызван")
    user_id = callback.from_user.id
    if is_pending(user_id):
        await callback.answer("Ваша заявка уже на рассмотрении", show_alert=True)
        return
    add_pending(user_id, callback.from_user.username, callback.from_user.full_name)
    # Шлём админу уведомление с кнопками — принять/отклонить/принять с изменением имени
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve_{user_id}")],
        [InlineKeyboardButton(text="✏️ Одобрить с изменением имени", callback_data=f"approve_rename_{user_id}")],
        [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{user_id}")]
    ])
    print(f"[DEBUG] Отправляю заявку админу {ADMIN_ID}")
    print(f"[DEBUG] markup: {markup}")
    print(f"[DEBUG] text: {f'🔔 <b>Новая заявка:</b>...'}")

    await safe_send_message(
        ADMIN_ID,
        f"🔔 <b>Новая заявка:</b>\nID: <code>{user_id}</code>\nUsername: @{callback.from_user.username or '-'}\nИмя: {callback.from_user.full_name or '-'}",
        reply_markup=markup,
        parse_mode="HTML"
    )
    await callback.message.edit_text("Заявка отправлена, ждите одобрения администратора.")
    await callback.answer("Заявка отправлена!", show_alert=True)






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







async def client_exists(vpn_type: str, client_name: str) -> bool:
    clients = await get_clients(vpn_type)
    return client_name in clients


@dp.callback_query(lambda c: c.data == "main_menu")
async def handle_main_menu(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id

    # Удаляем всё что есть у юзера
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







# 2) Обработчик «Получить VLESS» — учитываем контекст: обычный юзер или админ
@dp.callback_query(lambda c: c.data.startswith("get_vless_"))
async def send_vless_link(callback: types.CallbackQuery):
    client_name = callback.data.split("_", 2)[-1]
    user_id = callback.from_user.id

    # 1) Удаляем предыдущее меню, чтобы не было «зависших» кнопок
    await delete_last_menus(user_id)
    try:
        await callback.message.delete()
    except Exception:
        pass

    # 2) Готовим путь к файлу с конфигом для данного client_name
    vless_file_path = f"/root/vless-configs/{client_name}.txt"

    if os.path.exists(vless_file_path):
        # 3) Читаем готовую ссылку из файла
        try:
            with open(vless_file_path, "r", encoding="utf-8") as f:
                vless_link = f.read().strip()
        except Exception as e:
            # Если вдруг не удалось прочитать файл, выдаём ошибку
            await bot.send_message(user_id, f"❌ Не удалось прочитать конфиг VLESS: {e}")
            await callback.answer()
            return

        # 4) Формируем итоговый текст, вставляя ссылку
        text = (
            "<b>📖 Инструкция по установке VLESS:</b>\n"
            "👉 <a href=\"https://bi4i.ru/install-vless/\">bi4i.ru/install-vless</a>\n\n"
            "🔐 <b>Ваша персональная ссылка для подключения:</b>\n"
            f"<code>{vless_link}</code>\n\n"
            "📱 <b>Android</b>: v2rayNG, NekoBox, v2RayTun\n"
            "🍎 <b>iOS</b>: Streisand, FoXray, V2Box, Shadowrocket\n"
            "💻 <b>Windows</b>: Furious, InvisibleMan, Nekoray\n"
            "🍏 <b>macOS</b>: V2Box, FoXray, Nekoray, V2RayXS\n\n"
            "Скопируйте свою ссылку и вставьте в приложение — подключение займет пару секунд."
        )

        # 5) Кнопка «Назад» возвращает в меню управления этим клиентом
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[
                InlineKeyboardButton(text="⬅️ Назад",
                                     callback_data=f"back_to_user_menu_{client_name}")
            ]]
        )

        await bot.send_message(
            user_id,
            text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=keyboard
        )
        await callback.answer()

    else:
        # Если для этого client_name нет файла — сообщаем об ошибке
        await bot.send_message(
            user_id,
            f"❌ Конфигурация VLESS для <b>{client_name}</b> не найдена.\n"
            "Обратитесь к администратору.",
            parse_mode="HTML"
        )
        await callback.answer()



@dp.callback_query(lambda c: c.data.startswith("back_to_user_menu_"))
async def back_to_user_menu(callback: types.CallbackQuery):
    client_name = callback.data[len("back_to_user_menu_"):]
    user_id = callback.from_user.id

    # Удаляем всё, что там было
    await delete_last_menus(user_id)
    try:
        await callback.message.delete()
    except Exception:
        pass

    # Возвращаемся в меню управления клиентом
    await show_menu(
        user_id,
        f"Управление клиентом <b>{client_name}</b>:",
        create_user_menu(client_name, back_callback="users_menu", is_admin=(user_id == ADMIN_ID))
    )
    await callback.answer()


    

@dp.callback_query(lambda c: c.data.startswith("cancel_openvpn_") or c.data == "select_openvpn_back")
async def back_from_openvpn(callback: types.CallbackQuery, state: FSMContext):
    # Разбираем callback.data:
    # если data = "cancel_openvpn_config_<client_name>"
    if callback.data.startswith("cancel_openvpn_config_"):
        client_name = callback.data[len("cancel_openvpn_config_"):]
    # (редкий случай) если data = "cancel_openvpn_<client_name>"
    elif callback.data.startswith("cancel_openvpn_"):
        client_name = callback.data[len("cancel_openvpn_"):]
    else:
        # Вариант "select_openvpn_back"
        data = await state.get_data()
        client_name = data.get("client_name")
        if not client_name:
            stats = get_server_info()
            await show_menu(callback.from_user.id, stats + "\n<b>Главное меню:</b>", create_main_menu())
            await callback.answer()
            return

    user_id = callback.from_user.id

    # Удаляем текущее меню
    await delete_last_menus(user_id)
    try:
        await callback.message.delete()
    except Exception:
        pass

    # Возвращаемся в меню управления этим клиентом (заголовок без "config_")
    await show_menu(
        user_id,
        f"Управление клиентом <b>{client_name}</b>:",
        create_user_menu(client_name, back_callback="users_menu", is_admin=(user_id == ADMIN_ID))
    )
    await state.clear()
    await callback.answer()

 


@dp.callback_query(lambda c: c.data.startswith("client_"))
async def handle_client_selection(callback: types.CallbackQuery, state: FSMContext):
    _, vpn_type, client_name = callback.data.split("_", 2)
    await state.update_data(client_name=client_name, vpn_type=vpn_type)

    if vpn_type == "openvpn":
        await callback.message.delete()
        await bot.send_message(
            callback.from_user.id,
            "Выберите тип конфигурации OpenVPN:",
            reply_markup=create_openvpn_config_menu(client_name),
        )
        await state.set_state(VPNSetup.choosing_config_type)
    else:
        await callback.message.delete()
        await bot.send_message(
            callback.from_user.id,
            "Выберите тип конфигурации WireGuard:",
            reply_markup=create_wireguard_config_menu(client_name),
        )
        await state.set_state(VPNSetup.choosing_config_type)
        await callback.answer()

@dp.callback_query(lambda c: c.data == "openvpn_menu")
async def openvpn_menu(callback: types.CallbackQuery):
    await switch_menu(callback, "Меню OpenVPN:", reply_markup=create_openvpn_menu())
    await callback.answer()


@dp.callback_query(VPNSetup.choosing_config_type)
async def handle_interface_selection(callback: types.CallbackQuery, state: FSMContext):
    user_data = await state.get_data()
    client_name = user_data["client_name"]
    vpn_type = user_data["vpn_type"]
    user_id = callback.from_user.id

    # Обработка клавиши «⬅️ Назад» (в create_openvpn_config_menu прописан callback "cancel_openvpn_<client_name>")
    if callback.data == f"cancel_openvpn_config_{client_name}" or callback.data == f"cancel_openvpn_{client_name}":
        # Удаляем этот экран
        await delete_last_menus(user_id)
        try:
            await callback.message.delete()
        except Exception:
            pass

        # Возвращаемся в меню управления клиентом
        if user_id == ADMIN_ID:
            await show_menu(
                user_id,
                f"Управление клиентом <b>{client_name}</b>:",
                create_user_menu(client_name, back_callback="users_menu", is_admin=True)
            )
        else:
            await show_menu(
                user_id,
                f"Меню пользователя <b>{client_name}</b>:",
                create_user_menu(client_name, is_admin=False)
            )
        await state.clear()
        await callback.answer()
        return


@dp.callback_query(VPNSetup.choosing_protocol)
async def handle_protocol_selection(callback: types.CallbackQuery, state: FSMContext):
    user_data = await state.get_data()
    client_name = user_data["client_name"]

    if callback.data.startswith("send_ovpn_"):
        _, _, interface, proto, _ = callback.data.split("_", 4)
        name_core = client_name.replace("antizapret-", "").replace("vpn-", "")

        if proto == "default":
            dir_path = f"/root/antizapret/client/openvpn/{interface}/"
        else:
            dir_path = f"/root/antizapret/client/openvpn/{interface}-{proto}/"

        matched_file = None
        if os.path.exists(dir_path):
            for file in os.listdir(dir_path):
                # Исправлено: ищем по вхождению name_core, не по паттерну!
                if name_core in file and file.endswith(".ovpn"):
                    matched_file = os.path.join(dir_path, file)
                    break

        if matched_file and await send_single_config(
            callback.from_user.id, matched_file, os.path.basename(matched_file)
        ):
            await callback.message.delete()
            await callback.message.answer(
                "Главное меню:", reply_markup=create_main_menu()
            )
            await state.clear()
        else:
            await callback.answer("❌ Файл не найден", show_alert=True)

    elif callback.data.startswith("back_to_interface_"):
        await handle_back_to_interface(callback, state)


async def handle_wg_type_selection(callback: types.CallbackQuery, state: FSMContext):
    user_data = await state.get_data()
    client_name = user_data["client_name"]

    # Обработка кнопки "Назад"
    if callback.data.startswith("back_to_interface_"):
        await handle_back_to_interface(callback, state)
        await callback.answer()
        return

    if callback.data.startswith("send_wg_"):
        _, _, interface, wg_type, _ = callback.data.split("_", 4)

        name_core = client_name.replace("antizapret-", "").replace("vpn-", "")
        dir_path = f"/root/antizapret/client/{'wireguard' if wg_type == 'wg' else 'amneziawg'}/{interface}/"

        matched_file = None
        if os.path.exists(dir_path):
            for file in os.listdir(dir_path):
                # Исправлено: ищем по вхождению name_core и типу wg/am, не по паттерну!
                if name_core in file and wg_type in file and file.endswith(".conf"):
                    matched_file = os.path.join(dir_path, file)
                    break

        if not matched_file:
            await callback.answer("❌ Файл конфигурации не найден", show_alert=True)
            await state.clear()
            return

        await state.update_data(
            {
                "file_path": matched_file,
                "original_name": os.path.basename(matched_file),
                "short_name": f"{name_core}-{wg_type}.conf",
            }
        )

        await callback.message.edit_text(
            "Android может не принимать файлы с длинными именами.\nХотите переименовать файл при отправке?",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text="✅ Да", callback_data="confirm_rename"),
                        InlineKeyboardButton(text="❌ Нет", callback_data="no_rename"),
                    ]
                ]
            ),
        )
        await state.set_state(VPNSetup.confirming_rename)


@dp.callback_query(VPNSetup.confirming_rename)
async def handle_rename_confirmation(callback: types.CallbackQuery, state: FSMContext):
    user_data = await state.get_data()
    file_path = user_data["file_path"]

    # Проверяем, существует ли файл
    if not os.path.exists(file_path):
        print(f"Файл не найден: {file_path}")
        await callback.answer("❌ Файл не найден", show_alert=True)
        await state.clear()
        return

    # Проверяем размер файла (не пустой и не слишком большой)
    file_size = os.path.getsize(file_path)
    if file_size == 0:
        print(f"Файл пуст: {file_path}")
        await callback.answer("❌ Файл пуст", show_alert=True)
        await state.clear()
        return

    if file_size > 50 * 1024 * 1024:  # 50MB
        print(f"Файл слишком большой: {file_path} ({file_size} байт)")
        await callback.answer(
            "❌ Файл слишком большой для отправки в Telegram", show_alert=True
        )
        await state.clear()
        return

    try:
        if callback.data == "confirm_rename":
            file = FSInputFile(file_path, filename=user_data["short_name"])
            caption = f"🔐 {user_data['short_name']}"
        else:
            file = FSInputFile(file_path)
            caption = f"🔐 {user_data['original_name']}"

        await bot.send_document(
            chat_id=callback.from_user.id, document=file, caption=caption
        )

        await callback.message.delete()
        await callback.message.answer("Главное меню:", reply_markup=create_main_menu())

    except Exception as e:
        print(f"Ошибка при отправке файла: {e}")
        await callback.answer("❌ Ошибка при отправке файла", show_alert=True)

    await state.clear()


async def handle_back_to_interface(callback: types.CallbackQuery, state: FSMContext):
    user_data = await state.get_data()
    client_name = user_data["client_name"]
    vpn_type = user_data["vpn_type"]

    if vpn_type == "openvpn":
        try:
            await callback.message.delete()
        except Exception:
            pass
        await bot.send_message(
            callback.from_user.id,
            "Выберите тип конфигурации OpenVPN:",
            reply_markup=create_openvpn_config_menu(client_name),
        )
        await state.set_state(VPNSetup.choosing_config_type)
    else:
        await callback.message.edit_text(
            "Выберите тип конфигурации WireGuard:",
            reply_markup=create_wireguard_config_menu(client_name),
        )
        await state.set_state(VPNSetup.choosing_config_type)
    await callback.answer()



@dp.callback_query(lambda c: c.data.startswith("cancel_config_"))
async def handle_config_cancel(callback: types.CallbackQuery, state: FSMContext):
    client_name = callback.data.split("_")[-1]
    user_data = await state.get_data()
    vpn_type = user_data["vpn_type"]

    clients = await get_clients(vpn_type)
    total_pages = (len(clients) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    await callback.message.edit_text(
        "Список клиентов:",
        reply_markup=create_client_list_keyboard(
            clients, 1, total_pages, vpn_type, "list"
        ),
    )
    await state.clear()
    await callback.answer()

@dp.message(Command("announce"))
async def announce_command(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await bot.send_message(message.chat.id, "⛔ Нет доступа!")
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await bot.send_message(message.chat.id, "Напиши текст для рассылки после /announce!")
        return

    text = parts[1]
    sent, failed = await announce_all(text)
    await bot.send_message(message.chat.id, f"✅ Отправлено: {sent}, не доставлено: {failed}")



async def cleanup_openvpn_files(client_name: str):
    """Дополнительная очистка файлов OpenVPN после основного скрипта"""
    # Получаем имя файла без префиксов
    clean_name = client_name.replace("antizapret-", "").replace("vpn-", "")

    # Директории для проверки
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
            # Удаляем все файлы, содержащие имя клиента
            if clean_name in filename:
                try:
                    file_path = os.path.join(dir_path, filename)
                    os.remove(file_path)
                    deleted_files.append(file_path)
                except Exception as e:
                    print(f"Ошибка удаления {file_path}: {e}")

    return deleted_files

@dp.callback_query(lambda c: c.data.startswith("select_openvpn_"))
async def select_openvpn_config(callback: types.CallbackQuery):
    client_name = callback.data.replace("select_openvpn_", "")
    
    text = (
        "🔐 <b>OpenVPN — выбор конфигурации:</b>\n\n"
        "📱 Поддерживаемые устройства:\n"
        "• Android 🤖\n"
        "• iOS 🍎\n"
        "• Windows 💻\n"
        "• macOS 🍏\n"
        "• Linux 🐧\n\n"
        "📖 <b>Инструкция по установке:</b>\n"
        "👉 <a href=\"https://bi4i.ru/install/\">bi4i.ru/install</a>"
    )
    
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Обычный VPN", callback_data=f"download_openvpn_vpn_{client_name}")],
        [InlineKeyboardButton(text="✅ Antizapret (Рекомендуется)", callback_data=f"download_openvpn_antizapret_{client_name}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"back_to_user_menu_{client_name}")]
    ])
    
    await delete_last_menus(callback.from_user.id)
    try:
        await callback.message.delete()
    except Exception:
        pass

    await bot.send_message(
        callback.from_user.id,
        text,
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=markup
    )
    await callback.answer()




def get_openvpn_filename(client_name, config_type):
    if config_type == "vpn":
        return f"{FILEVPN_NAME} - Обычный VPN - {client_name}.ovpn"
    elif config_type == "antizapret":
        return f"{FILEVPN_NAME} - {client_name}.ovpn"


# Вывод конфига для OpenVPN
@dp.callback_query(lambda c: c.data.startswith("download_openvpn_"))
async def download_openvpn_config(callback: types.CallbackQuery):
    parts = callback.data.split("_", 3)
    _, _, config_type, client_name = parts
    user_id = callback.from_user.id
    username = callback.from_user.username or "Без username"

    # Удаляем прошлые inline-сообщения
    await delete_last_menus(user_id)
    try:
        await callback.message.delete()
    except Exception:
        pass

    # Определяем пути
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
        await callback.answer("✅ Конфигурация отправлена.")

        # Уведомление админу
        await notify_admin_download(user_id, username, os.path.basename(file_path), "ovpn")

        # Кнопка назад
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"cancel_openvpn_config_{client_name}")]
        ])
        await show_menu(user_id, "Вернуться к выбору типа конфига:", markup)
    else:
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"cancel_openvpn_config_{client_name}")]
        ])
        files_list = os.listdir(base_path) if os.path.exists(base_path) else []
        await show_menu(
            user_id,
            f"❌ Не найден файл {file_name} в папке {base_path}\n"
            f"Файлы в папке: {files_list}",
            markup
        )
        await callback.answer("❌ Файл конфигурации не найден.", show_alert=True)





@dp.message(VPNSetup.entering_client_name)
async def handle_client_name(message: types.Message, state: FSMContext):
    data = await state.get_data()

    # Удаляем сообщение "Введите имя нового пользователя:"
    msg_id = data.get("add_user_msg_id")
    if msg_id:
        try:
            await bot.delete_message(message.chat.id, msg_id)
        except Exception:
            pass

    # --- Вот здесь if, с правильным отступом ---
    if message.text == "❌ Отмена":
        await state.clear()
        await delete_last_menus(message.from_user.id)  # ← сразу после clear!
        stats = get_server_info()
        await show_menu(message.from_user.id, stats + "\n<b>Главное меню:</b>", create_main_menu())
        return

    client_name = message.text.strip()
    if not re.match(r"^[a-zA-Z0-9_-]{1,32}$", client_name):
        await message.answer("❌ Некорректное имя! Используйте буквы, цифры, _ и -", reply_markup=cancel_markup)
        return

    data = await state.get_data()

    # --- 1. Это создание профиля по заявке от админа
    if "approve_user_id" in data:
        user_id = data["approve_user_id"]
        result = await execute_script("1", client_name, "30")
        if result["returncode"] == 0:
            save_profile_name(user_id, client_name)
            remove_pending(user_id)
            await safe_send_message(
                user_id,
                f"✅ Ваша заявка одобрена!\n"
                f"Имя профиля: <b>{client_name}</b>\n"
                "Теперь вам доступны функции VPN.",
                parse_mode="HTML",
                reply_markup=create_user_menu(client_name)
            )
            await show_menu(message.from_user.id, "Пользователь активирован и уведомлен!", create_main_menu())
        else:
            await message.answer(f"❌ Ошибка: {result['stderr']}")
        await state.clear()
        return

    # --- 2. Обычное добавление/удаление пользователя через меню
    option = data.get("action")
    if option == "1":
        result = await execute_script("1", client_name, "30")
        if result["returncode"] == 0:
            msg = await message.answer("✅ Клиент создан на 30 дней!", reply_markup=ReplyKeyboardRemove())
            await asyncio.sleep(1)
            try:
                await msg.delete()
            except Exception:
                pass
            stats = get_server_info()
            await show_menu(message.from_user.id, stats + "\n<b>Главное меню:</b>", create_main_menu())
        else:
            await message.answer(f"❌ Ошибка: {result['stderr']}", reply_markup=ReplyKeyboardRemove())
        await state.clear()
        return
    elif option == "2":
        result = await execute_script(option, client_name)
        if result["returncode"] == 0:
            stats = get_server_info()
            await show_menu(message.from_user.id, stats + "\n<b>Главное меню:</b>", create_main_menu())
        else:
            await message.answer(f"❌ Ошибка: {result['stderr']}")
        await state.clear()
        return
    else:
        await message.answer("Ошибка: неизвестное действие")
        await state.clear()
        return




@dp.message(VPNSetup.deleting_client)
async def handle_delete_client(message: types.Message, state: FSMContext):
    """Обрабатывает запрос на удаление клиента в боте."""
    client_name = message.text.strip()
    data = await state.get_data()
    vpn_type = "openvpn" if data["action"] == "2" else "wireguard"

    await message.answer(
        f"Вы уверены, что хотите удалить клиента {client_name}?",
        reply_markup=create_confirmation_keyboard(client_name, vpn_type),
    )   
    await state.clear()


async def get_clients(vpn_type: str):
    option = "3" if vpn_type == "openvpn" else "6"
    result = await execute_script(option)

    if result["returncode"] == 0:
        # Фильтруем строки, убирая заголовки и пустые строки
        clients = [
            c.strip()
            for c in result["stdout"].split("\n")
            if c.strip()  # Убираем пустые строки
            and not c.startswith("OpenVPN client names:")  # Убираем заголовок OpenVPN
            and not c.startswith(
                "WireGuard/AmneziaWG client names:"
            )  # Убираем заголовок WireGuard
            and not c.startswith(
                "OpenVPN - List clients"
            )  # Убираем строку "OpenVPN - List clients"
            and not c.startswith(
                "WireGuard/AmneziaWG - List clients"
            )  # Убираем строку "WireGuard/AmneziaWG - List clients"
        ]
        return clients
    return []


async def send_config(chat_id: int, client_name: str, option: str) -> bool:
    try:
        files_found = []
        # Для WireGuard/AmneziaWG
        if option == "4":
            base_dir = "/root/antizapret/client/amneziawg/"
            ext = ".conf"
            prefix = f"amneziawg-{client_name}-"
            for root, _, files in os.walk(base_dir):
                for file in files:
                    if file.startswith(prefix) and file.endswith(ext):
                        files_found.append(os.path.join(root, file))
        # Для OpenVPN — ищем по обоим вариантам
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


# Добавляем функцию send_backup здесь
async def send_backup(chat_id: int) -> bool:
    """Функция отправки резервной копии"""

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

    return False  # Если ни один файл не найден

#@dp.callback_query()
async def handle_callback_query(callback: types.CallbackQuery, state: FSMContext):
    """Обрабатывает нажатия на кнопки в Telegram боте и выполняет соответствующие действия."""
    data = callback.data
    user_id = callback.from_user.id

    try:
        if user_id != ADMIN_ID and user_id not in AUTHORIZED_USERS:
            await callback.answer("Доступ запрещен!")
            return

        # Пагинация
        if data.startswith("page_"):
            _, action, vpn_type, page = data.split("_", 3)
            page = int(page)
            clients = await get_clients(vpn_type)
            total_pages = (len(clients) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
            await callback.message.edit_text(
                "Список клиентов:",
                reply_markup=create_client_list_keyboard(
                    clients, page, total_pages, vpn_type, action
                ),
            )
            await callback.answer()
            return

        # Обработка удаления (начальная кнопка удалить)
        if data.startswith("delete_"):
            _, vpn_type, client_name = data.split("_", 2)
            await callback.message.edit_text(
                f"❓ Удалить клиента {client_name} ({vpn_type})?",
                reply_markup=create_confirmation_keyboard(client_name, vpn_type),
            )
            await callback.answer()
            return

        # Обработка пагинации для удаления
        if data.startswith("page_delete_"):
            _, _, vpn_type, page = data.split("_")
            page = int(page)
            clients = await get_clients(vpn_type)
            total_pages = (len(clients) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE

            await callback.message.edit_text(
                "Выберите клиента для удаления:",
                reply_markup=create_client_list_keyboard(
                    clients, page, total_pages, vpn_type, "delete"
                ),
            )
            await callback.answer()
            return

        # Инициализация удаления через главное меню (цифровые callback: 2 и 5)
        if data in ["2", "5"]:
            vpn_type = "openvpn" if data == "2" else "wireguard"
            clients = await get_clients(vpn_type)
            if not clients:
                await callback.message.edit_text("❌ Нет клиентов для удаления")
                await callback.answer()
                return
            total_pages = (len(clients) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
            await callback.message.edit_text(
                "Выберите клиента для удаления:",
                reply_markup=create_client_list_keyboard(
                    clients, 1, total_pages, vpn_type, "delete"
                ),
            )
            await state.set_state(VPNSetup.list_for_delete)
            await callback.answer()
            return

        # Подтверждение удаления (confirm_openvpn_имя или confirm_wireguard_имя)
        if data.startswith("confirm_"):
            _, vpn_type, client_name = data.split("_", 2)
            option = "2" if vpn_type == "openvpn" else "5"
            try:
                result = await execute_script(option, client_name)
                # Дополнительная очистка для OpenVPN
                if vpn_type == "openvpn" and result["returncode"] == 0:
                    deleted_files = await cleanup_openvpn_files(client_name)
                    if deleted_files:
                        result["additional_deleted"] = deleted_files

                if result["returncode"] == 0:
                    msg = f"✅ Клиент {client_name} удален!"
                    if vpn_type == "openvpn" and result.get("additional_deleted"):
                        msg += f"\nДополнительно удалено файлов: {len(result['additional_deleted'])}"
                    await callback.message.edit_text(msg)
                    await callback.message.answer("Главное меню:", reply_markup=create_main_menu())
                else:
                    await callback.message.edit_text(f"❌ Ошибка: {result['stderr']}")
            except Exception as e:
                print(f"Ошибка при удалении клиента: {e}")
            finally:
                await callback.answer()
                await state.clear()
            return

        # Отмена удаления
        if data == "cancel_delete":
            await callback.message.edit_text("❌ Удаление отменено", reply_markup=create_main_menu())
            await callback.answer()
            return

        # Список клиентов (цифровые callback: 3 и 6)
        if data in ["3", "6"]:
            vpn_type = "openvpn" if data == "3" else "wireguard"
            clients = await get_clients(vpn_type)
            total_pages = (len(clients) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
            await callback.message.edit_text(
                "Список клиентов:",
                reply_markup=create_client_list_keyboard(
                    clients, 1, total_pages, vpn_type, "list"
                ),
            )
            await callback.answer()
            return

        # Создание клиента (цифровые callback: 1 и 4)
        if data in ["1", "4"]:
            await state.update_data(action=1)
            await callback.message.edit_text("Введите имя нового клиента:")
            await state.set_state(VPNSetup.entering_client_name)
            await callback.answer()
            return

        # Пересоздание файлов
        if data == "7":
            await callback.message.edit_text("⏳ Идет пересоздание файлов...")
            result = await execute_script("7")
            if result["returncode"] == 0:
                await callback.message.edit_text("✅ Файлы успешно пересозданы!")
                await callback.message.answer("Главное меню:", reply_markup=create_main_menu())
            else:
                await callback.message.edit_text(f"❌ Ошибка: {result['stderr']}")
            await callback.answer()
            return

        # Создание бэкапа
        if data == "8":
            await callback.message.edit_text("⏳ Создаю бэкап...")
            result = await execute_script("8")
            if result["returncode"] == 0:
                if await send_backup(callback.from_user.id):
                    await callback.message.delete()
                    await callback.message.answer("Главное меню:", reply_markup=create_main_menu())
                else:
                    await callback.message.edit_text("❌ Не удалось отправить бэкап")
            else:
                await callback.message.edit_text(
                    f"❌ Ошибка при создании бэкапа: {result['stderr']}"
                )
            await callback.answer()
            return

    except Exception as e:
        print(f"Error: {e}")
        await callback.answer("⚠️ Произошла ошибка!")


async def notify_admin_download(user_id, username, file_name, vpn_type):
    vpn_emoji = "📥"
    vpn_text = {
        "wg": "WireGuard",
        "amnezia": "Amnezia",
        "ovpn": "OpenVPN"
    }
    text = (
        f"{vpn_emoji} Скачивание конфига\n"
        f"Пользователь: <code>{user_id}</code> (@{username})\n"
        f"Файл: {file_name}"
    )
    try:
        await bot.send_message(ADMIN_ID, text, parse_mode="HTML")
    except Exception as e:
        print(f"Ошибка при отправке уведомления админу: {e}")


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

            # 1) удаляем сообщение с заявкой у админа
            try:
                await callback.message.delete()
            except Exception:
                pass

            # 2) уведомляем пользователя
            await safe_send_message(
                user_id,
                f"✅ Ваша заявка одобрена!\n"
                f"Имя профиля: <b>{client_name}</b>\nТеперь вам доступны функции VPN.",
                parse_mode="HTML",
                reply_markup=create_user_menu(client_name)
            )

            # 3) показываем админу главное меню
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

    # ───── обработка «Отклонить» ─────
    else:  # action == "reject"
        remove_pending(user_id)

        # тоже удаляем сообщение с заявкой
        try:
            await callback.message.delete()
        except Exception:
            pass

        # уведомляем пользователя об отклонении
        await safe_send_message(user_id, "❌ Ваша заявка отклонена. Обратитесь к администратору.")

        # (не обязательно) можно показать админу краткий текст вместо меню
        # await callback.message.edit_text("❌ Заявка отклонена.")
        await callback.answer()




async def notify_expiring_users():
    while True:
        try:
            if not os.path.exists(APPROVED_FILE):
                await asyncio.sleep(12 * 3600)
                continue

            with open(APPROVED_FILE, "r") as f:
                approved_users = [line.strip() for line in f if line.strip().isdigit()]

            for user_id in approved_users:
                user_id_int = int(user_id)
                client_name = get_profile_name(user_id_int)
                if not client_name:
                    continue

                cert_info = get_cert_expiry_info(client_name)
                if not cert_info:
                    continue

                days_left = cert_info["days_left"]
                # флаг для конкретного дня, чтобы не спамить
                flag_file = f".notified_{user_id}_{days_left}.flag"

                # Напоминания за 5,4,3,2,1 день
                if 1 <= days_left <= 5:
                    if not os.path.exists(flag_file):
                        # 1) отправляем пользователю
                        try:
                            await bot.send_message(
                                user_id_int,
                                f"⚠️ Осталось {days_left} дн. до окончания VPN-сертификата.",
                                parse_mode="HTML"
                            )
                        except:
                            pass
                        # 2) администратору
                        try:
                            await bot.send_message(
                                ADMIN_ID,
                                f"⚠️ Пользователю <code>{user_id_int}</code> ({client_name}) осталось {days_left} дн.",
                                parse_mode="HTML"
                            )
                        except:
                            pass
                        # 3) флаг, чтобы не повторять за этот день
                        with open(flag_file, "w") as f_flag:
                            f_flag.write("notified")

                # После истечения срока
                elif days_left < 0:
                    await revoke_and_cleanup(client_name, user_id_int)

                # Если более 5 дней — сбросить все флаги
                elif days_left > 5:
                    for fn in os.listdir():
                        if fn.startswith(f".notified_{user_id}_") and fn.endswith(".flag"):
                            try:
                                os.remove(fn)
                            except:
                                pass

        except Exception as e:
            print(f"[notify_expiring_users] Ошибка: {e}")

        await asyncio.sleep(12 * 3600)


async def revoke_and_cleanup(client_name: str, user_id_int: int):
    """
    Отзывает сертификат client_name, обновляет CRL,
    обновляет OpenVPN, удаляет WireGuard-peer,
    чистит конфиги, переводит пользователя в pending
    и уведомляет его и администратора.
    """
    # 1) revoke + gen-crl через Easy-RSA
    easyrsa = "/etc/openvpn/easyrsa3/easyrsa"
    subprocess.run([easyrsa, "--batch", "revoke", client_name], check=True)
    subprocess.run([easyrsa, "gen-crl"],                    check=True)

    # 2) скопировать новый CRL в место, где его ждёт OpenVPN
    src_crl = "/etc/openvpn/easyrsa3/pki/crl.pem"
    dst_crl = "/etc/openvpn/server/keys/crl.pem"
    shutil.copy(src_crl, dst_crl)
    os.chmod(dst_crl, 0o644)

    # 3) мягко перечитать CRL – SIGUSR1 всем openvpn-процессам
    subprocess.run(["pkill", "-USR1", "openvpn"], check=True)

    # 4) удалить WireGuard-peer
    pubkey = get_pubkey_for_client(client_name)
    if pubkey:
        subprocess.run(["wg", "set", "wg0", "peer", pubkey, "remove"], check=True)

    # 5) очистить все клиентские конфиги (OpenVPN, WireGuard, VLESS)
    cleanup_configs_for_client(client_name)

    # 6) снять одобрение и перевести в pending
    remove_approved_user(user_id_int)
    add_pending(user_id_int, "", "")

    # 7) удалить все открытые меню у пользователя
    await delete_last_menus(user_id_int)

    # 8) уведомить пользователя
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Отправить заявку на доступ", callback_data="send_request")]
    ])
    try:
        await bot.send_message(
            user_id_int,
            "⛔ Срок действия вашего VPN-сертификата истёк.\n\n"
            "Для восстановления доступа отправьте новую заявку:",
            parse_mode="HTML",
            reply_markup=markup
        )
    except:
        pass

    # 9) уведомить администратора
    try:
        await bot.send_message(
            ADMIN_ID,
            f"⚠️ Доступ пользователя <code>{user_id_int}</code> ({client_name}) снят по истечении срока.",
            parse_mode="HTML"
        )
    except:
        pass


def get_pubkey_for_client(client_name: str) -> str | None:
    """
    Ищет публичный ключ клиента client_name
    в конфиге WireGuard/AmneziaWG и возвращает его.
    """
    for base in ["/root/antizapret/client/wireguard",
                 "/root/antizapret/client/amneziawg"]:
        for root, _, files in os.walk(base):
            for fname in files:
                if client_name in fname and fname.endswith(".conf"):
                    path = os.path.join(root, fname)
                    try:
                        with open(path, encoding="utf-8") as cf:
                            for line in cf:
                                if line.strip().startswith("PublicKey"):
                                    return line.split("=", 1)[1].strip()
                    except:
                        continue
    return None


def cleanup_configs_for_client(client_name: str):
    """
    Удаляет все конфиги OpenVPN, WireGuard и VLESS,
    связанные с client_name.
    """
    patterns = [
        "/root/antizapret/client/openvpn/**/*{cn}*.ovpn",
        "/root/antizapret/client/wireguard/**/*{cn}*.conf",
        "/root/antizapret/client/amneziawg/**/*{cn}*.conf",
        "/root/vless-configs/{cn}.txt",
    ]
    for pat in patterns:
        for path in glob.glob(pat.format(cn=client_name), recursive=True):
            try:
                os.remove(path)
            except:
                pass


# ==== Старт бота ====
async def main():
    print("✅ Бот успешно запущен!")
    asyncio.create_task(notify_expiring_users())
    await set_bot_commands()
    await dp.start_polling(bot)




if __name__ == "__main__":
    asyncio.run(main())
