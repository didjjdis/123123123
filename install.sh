#!/bin/bash
#
# Установочный скрипт для VPN-бота (TG-Bot-OpenVPN-Antizapret)

set -e

### 0) Проверка, что скрипт запущен от root
if [ "$EUID" -ne 0 ]; then
  echo "Ошибка: скрипт нужно запускать от root."
  exit 1
fi

echo "=============================================="
echo "Установка VPN-бота (TG-Bot-OpenVPN-Antizapret) v2.8.5"
echo "=============================================="
echo

### 1) Установка системных пакетов (git, wget, curl, python3-venv, python3-pip, easy-rsa)
echo "=== Шаг 1: Установка системных пакетов ==="
apt update -qq

REQUIRED_PKG=("git" "wget" "curl" "python3-venv" "python3-pip" "easy-rsa")
for pkg in "${REQUIRED_PKG[@]}"; do
  if ! dpkg -s "$pkg" &>/dev/null; then
    echo "  • Устанавливаем: $pkg"
    apt install -y "$pkg"
  else
    echo "  • $pkg уже установлен — пропуск."
  fi
done

echo

### 2) Копирование easy-rsa в /etc/openvpn/easyrsa3
echo "=== Шаг 2: Настройка easy-rsa → /etc/openvpn/easyrsa3 ==="
EASY_SRC="/usr/share/easy-rsa"
EASY_DST="/etc/openvpn/easyrsa3"

if [ -d "$EASY_SRC" ]; then
  echo "  Копируем '$EASY_SRC' → '$EASY_DST'"
  mkdir -p "$EASY_DST"
  cp -r "$EASY_SRC/"* "$EASY_DST/"
  chmod -R 755 "$EASY_DST"
  echo "  easy-rsa скопирован."
else
  echo "  ⚠️  Папка '$EASY_SRC' не найдена, easy-rsa не установлен?"
fi

echo

### 3) Запрос BOT_TOKEN, ADMIN_ID и FILEVPN_NAME
echo "=== Шаг 3: Настройка BOT_TOKEN, ADMIN_ID и FILEVPN_NAME ==="
read -p "Введите BOT_TOKEN (токен из BotFather): " BOT_TOKEN
BOT_TOKEN="$(echo "$BOT_TOKEN" | xargs)"
if [ -z "$BOT_TOKEN" ]; then
  echo "Ошибка: BOT_TOKEN не может быть пустым."
  exit 1
fi

read -p "Введите ADMIN_ID (Telegram User ID): " ADMIN_ID
ADMIN_ID="$(echo "$ADMIN_ID" | xargs)"
if [ -z "$ADMIN_ID" ]; then
  echo "Ошибка: ADMIN_ID не может быть пустым."
  exit 1
fi

echo
read -p "Введите название для VPN-файлов (FILEVPN_NAME), например: MyVPN: " FILEVPN_NAME
FILEVPN_NAME="$(echo "$FILEVPN_NAME" | xargs)"
if [ -z "$FILEVPN_NAME" ]; then
  echo "Ошибка: FILEVPN_NAME не может быть пустым."
  exit 1
fi

echo
echo "Вы ввели:"
echo "  BOT_TOKEN    = \"$BOT_TOKEN\""
echo "  ADMIN_ID     = \"$ADMIN_ID\""
echo "  FILEVPN_NAME = \"$FILEVPN_NAME\""
echo

### 4) Сохранение переменных в /root/.env (UTF-8 без BOM)
echo "=== Шаг 4: Запись переменных в /root/.env ==="
cat > "/root/.env" <<EOF
BOT_TOKEN=$BOT_TOKEN
ADMIN_ID=$ADMIN_ID
FILEVPN_NAME=$FILEVPN_NAME
EOF
# Убедимся, что файл UTF-8:
iconv -f utf-8 -t utf-8 "/root/.env" -o "/root/.env.tmp" && mv "/root/.env.tmp" "/root/.env"
echo "  Файл /root/.env записан (UTF-8)."
echo

### 5) Клонирование репозитория во временную папку
TMP_DIR="/tmp/antizapret-install"
GIT_URL="https://github.com/didjjdis/123123123"
BRANCH="main"

if [ -d "$TMP_DIR" ]; then
  echo "Удаляем старую временную папку $TMP_DIR"
  rm -rf "$TMP_DIR"
fi

echo "=== Шаг 5: Клонируем репозиторий в $TMP_DIR ==="
git clone "$GIT_URL" "$TMP_DIR"
cd "$TMP_DIR"
git checkout "$BRANCH"

echo
echo "Сбрасываем локальные изменения и подтягиваем origin/$BRANCH..."
git fetch origin "$BRANCH"
git reset --hard "origin/$BRANCH"
echo

### 6) Копирование подпапок в целевые директории (перезапись без удаления остального)
echo "=== Шаг 6: Копирование файлов из временного клона ==="

# 6.1) antizapret → /root/antizapret
SRC_ANTIZAPRET="$TMP_DIR/antizapret"
DST_ANTIZAPRET="/root/antizapret"
if [ -d "$SRC_ANTIZAPRET" ]; then
  echo "  Копируем '$SRC_ANTIZAPRET' → '$DST_ANTIZAPRET'"
  mkdir -p "$DST_ANTIZAPRET"
  cp -r "$SRC_ANTIZAPRET/"* "$DST_ANTIZAPRET/"
else
  echo "  ⚠️  Папка '$SRC_ANTIZAPRET' не найдена."
fi

# 6.2) etc/openvpn → /etc/openvpn
SRC_OPENVPN="$TMP_DIR/etc/openvpn"
DST_OPENVPN="/etc/openvpn"
if [ -d "$SRC_OPENVPN" ]; then
  echo "  Копируем '$SRC_OPENVPN' → '$DST_OPENVPN'"
  mkdir -p "$DST_OPENVPN"
  cp -r "$SRC_OPENVPN/"* "$DST_OPENVPN/"

  # 6.3) Копирование пользовательских серверных конфигов OpenVPN → /etc/openvpn/server
  echo "  Копируем серверные конфиги OpenVPN → /etc/openvpn/server"
  mkdir -p /etc/openvpn/server
  # защита на случай отсутствия конфигов
  shopt -s nullglob
  for src in "$TMP_DIR/etc/openvpn/server/"*.conf; do
    cp "$src" /etc/openvpn/server/
  done
  shopt -u nullglob

  # Подставляем FILEVPN_NAME и выставляем права
  for f in /etc/openvpn/server/*.conf; do
    sed -i "s|\${FILEVPN_NAME}|${FILEVPN_NAME}|g" "$f" || true
    chmod 644 "$f"
    echo "    Настроен и права 644: $f"
  done
else
  echo "  ⚠️  Папка '$SRC_OPENVPN' не найдена."
fi

# 6.4) root → /root
SRC_ROOT="$TMP_DIR/root"
DST_ROOT="/root"
if [ -d "$SRC_ROOT" ]; then
  echo "  Копируем '$SRC_ROOT' → '$DST_ROOT'"
  cp -r "$SRC_ROOT/"* "$DST_ROOT/"
else
  echo "  ⚠️  Папка '$SRC_ROOT' не найдена."
fi

echo "Копирование завершено."
echo

### 7) Замена "${FILEVPN_NAME}" и "$FILEVPN_NAME", затем приведём файлы в UTF-8
echo "=== Шаг 7: Замена \"\${FILEVPN_NAME}\" и \"\$FILEVPN_NAME\" → \"$FILEVPN_NAME\" (UTF-8) ==="

# Функция для перекодирования в UTF-8
recode_to_utf8() {
  local file="$1"
  if [ -f "$file" ]; then
    iconv -f utf-8 -t utf-8 "$file" -o "${file}.tmp" && mv "${file}.tmp" "$file"
  fi
}

# 7.1) В /root/antizapret, кроме client/openvpn/vpn/**
grep -RIl --exclude-dir="client/openvpn/vpn" '\${FILEVPN_NAME}' /root/antizapret 2>/dev/null | while IFS= read -r f; do
  sed -i "s|\${FILEVPN_NAME}|${FILEVPN_NAME}|g" "$f"
  recode_to_utf8 "$f"
  echo "  Заменено \${FILEVPN_NAME} и UTF-8: $f"
done || true

grep -RIl --exclude-dir="client/openvpn/vpn" '\$FILEVPN_NAME' /root/antizapret 2>/dev/null | while IFS= read -r f; do
  sed -i "s|\$FILEVPN_NAME|${FILEVPN_NAME}|g" "$f"
  recode_to_utf8 "$f"
  echo "  Заменено \$FILEVPN_NAME и UTF-8: $f"
done || true

# 7.2) В /etc/openvpn
grep -RIl '\${FILEVPN_NAME}' /etc/openvpn 2>/dev/null | while IFS= read -r f; do
  sed -i "s|\${FILEVPN_NAME}|${FILEVPN_NAME}|g" "$f"
  recode_to_utf8 "$f"
  echo "  Заменено \${FILEVPN_NAME} и UTF-8: $f"
done || true

grep -RIl '\$FILEVPN_NAME' /etc/openvpn 2>/dev/null | while IFS= read -r f; do
  sed -i "s|\$FILEVPN_NAME|${FILEVPN_NAME}|g" "$f"
  recode_to_utf8 "$f"
  echo "  Заменено \$FILEVPN_NAME и UTF-8: $f"
done || true

# 7.3) В /root/bot.py и /root/client.sh (если есть)
for f in /root/bot.py /root/client.sh; do
  if [ -f "$f" ]; then
    if grep -q '\${FILEVPN_NAME}' "$f"; then
      sed -i "s|\${FILEVPN_NAME}|${FILEVPN_NAME}|g" "$f"
      recode_to_utf8 "$f"
      echo "  Заменено \${FILEVPN_NAME} и UTF-8: $f"
    fi
    if grep -q '\$FILEVPN_NAME' "$f"; then
      sed -i "s|\$FILEVPN_NAME|${FILEVPN_NAME}|g" "$f"
      recode_to_utf8 "$f"
      echo "  Заменено \$FILEVPN_NAME и UTF-8: $f"
    fi
  fi
done

echo

### 8) Принудительное пересоздание виртуального окружения и установка зависимостей
echo "=== Шаг 8: Пересоздание виртуального окружения и установка зависимостей ==="
VENV_DIR="/root/venv"

if [ -d "$VENV_DIR" ]; then
  echo "  Удаляем существующее виртуальное окружение: rm -rf $VENV_DIR"
  rm -rf "$VENV_DIR"
fi

echo "  Создаём виртуальное окружение: python3 -m venv $VENV_DIR"
python3 -m venv "$VENV_DIR"

echo "  Активируем venv и устанавливаем зависимости из /root/requirements.txt"
source "$VENV_DIR/bin/activate"
if [ -f "/root/requirements.txt" ]; then
  pip install --upgrade pip
  pip install -r /root/requirements.txt
else
  echo "  ⚠️  /root/requirements.txt не найден — зависимости не установлены!"
fi
deactivate

echo

### 9) Даем всем скопированным файлам права 777
echo "=== Шаг 9: Полные права (777) всем скопированным файлам ==="
if [ -d "/root/antizapret" ]; then
  chmod -R 777 "/root/antizapret"
  echo "  Права 777 выставлены на /root/antizapret"
fi

if [ -d "/etc/openvpn" ]; then
  chmod -R 777 "/etc/openvpn"
  echo "  Права 777 выставлены на /etc/openvpn"
fi

if [ -d "$SRC_ROOT" ]; then
  find "$SRC_ROOT" -type f | while IFS= read -r srcf; do
    rel="${srcf#$SRC_ROOT/}"
    target="/root/$rel"
    if [ -e "$target" ]; then
      chmod 777 "$target"
      echo "  Права 777 выставлены на $target"
    fi
  done
fi

echo

### 10) Создание systemd-юнита vpnbot.service
echo "=== Шаг 10: Создание systemd-юнита /etc/systemd/system/vpnbot.service ==="
cat > /etc/systemd/system/vpnbot.service <<EOF
[Unit]
Description=VPN Telegram Bot
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root
EnvironmentFile=/root/.env
ExecStart=/root/venv/bin/python /root/bot.py
Restart=on-failure
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

echo "  Юнит записан: /etc/systemd/system/vpnbot.service"
echo

### 11) Перезагрузка systemd, автозапуск, запуск службы
echo "=== Шаг 11: Перезагрузка systemd и запуск vpnbot.service ==="
systemctl daemon-reload
systemctl enable vpnbot.service
systemctl restart vpnbot.service

echo

### 12) Итоговое сообщение и инструкции
echo "=============================================="
echo "Установка завершена! Бот запущен как vpnbot.service."
echo
echo "Команды для управления ботом:"
echo "  ● Проверить статус:     systemctl status vpnbot.service"
echo "  ● Перезапустить бота:   systemctl restart vpnbot.service"
echo "  ● Смотреть логи:        journalctl -u vpnbot -f"
echo
echo "Основные пути и параметры:"
echo "  ● /root/antizapret       — скопировано из репозитория antizapret/"
echo "  ● /etc/openvpn          — скопировано из репозитория etc/openvpn/"
echo "  ● /etc/openvpn/easyrsa3  — скопирован easy-rsa"
echo "  ● /root                 — скопировано из репозитория root/ (bot.py, client.sh, requirements.txt и т. д.)"
echo "  ● Виртуальное окружение: /root/venv"
echo "  ● Файл с переменными:    /root/.env"
echo "       • BOT_TOKEN    = $BOT_TOKEN"
echo "       • ADMIN_ID     = $ADMIN_ID"
echo "       • FILEVPN_NAME = $FILEVPN_NAME"
echo "=============================================="
