import os
import re
import logging
import asyncio
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, ChatMemberUpdatedFilter, IS_NOT_MEMBER, IS_MEMBER
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ChatMemberUpdated
from aiogram.enums import ParseMode

import asyncpg

# --- Логирование ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Переменные окружения ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is not set in environment")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in environment")

# --- Инициализация бота и диспетчера ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- Подключение к базе данных (пул соединений) ---
db_pool = None

async def init_db_pool():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    logger.info("Database pool created")

async def close_db_pool():
    if db_pool:
        await db_pool.close()
        logger.info("Database pool closed")

async def init_tables():
    async with db_pool.acquire() as conn:
        # Таблица продаж
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS sales (
                id SERIAL PRIMARY KEY,
                date TIMESTAMP NOT NULL,
                amount NUMERIC(10,2) NOT NULL,
                participants INTEGER NOT NULL,
                raw_text TEXT,
                user_id BIGINT,
                chat_id BIGINT
            )
        ''')
        # Таблица групп
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS groups (
                chat_id BIGINT PRIMARY KEY,
                title TEXT NOT NULL,
                added_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        # Таблица настроек пользователей (выбранная группа)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id BIGINT PRIMARY KEY,
                selected_chat_id BIGINT
            )
        ''')
    logger.info("Tables created/verified")

# --- Функции работы с БД (асинхронные) ---
async def add_group(chat_id: int, title: str):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO groups (chat_id, title) VALUES ($1, $2) ON CONFLICT (chat_id) DO UPDATE SET title = EXCLUDED.title",
            chat_id, title
        )

async def remove_group(chat_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM groups WHERE chat_id = $1", chat_id)

async def get_groups():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT chat_id, title FROM groups ORDER BY title")
        return [(row['chat_id'], row['title']) for row in rows]

async def get_user_selected_chat(user_id: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT selected_chat_id FROM user_settings WHERE user_id = $1", user_id)
        return row['selected_chat_id'] if row else None

async def set_user_selected_chat(user_id: int, chat_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO user_settings (user_id, selected_chat_id) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET selected_chat_id = EXCLUDED.selected_chat_id",
            user_id, chat_id
        )

async def add_sale(date: datetime, amount: float, participants: int, raw_text: str, user_id: int, chat_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO sales (date, amount, participants, raw_text, user_id, chat_id) VALUES ($1, $2, $3, $4, $5, $6)",
            date, amount, participants, raw_text, user_id, chat_id
        )

async def get_stats(start_date: datetime, end_date: datetime, chat_id: int = None):
    async with db_pool.acquire() as conn:
        if chat_id is not None:
            rows = await conn.fetch(
                "SELECT amount, participants FROM sales WHERE date >= $1 AND date < $2 AND chat_id = $3",
                start_date, end_date, chat_id
            )
        else:
            rows = await conn.fetch(
                "SELECT amount, participants FROM sales WHERE date >= $1 AND date < $2",
                start_date, end_date
            )
    if not rows:
        return None
    total_revenue = sum(row['amount'] for row in rows)
    total_participants = sum(row['participants'] for row in rows)
    num_sales = len(rows)
    avg_check = total_revenue / num_sales if num_sales > 0 else 0
    return {
        "revenue": total_revenue,
        "participants": total_participants,
        "sales_count": num_sales,
        "avg_check": avg_check
    }

async def get_group_title(chat_id: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT title FROM groups WHERE chat_id = $1", chat_id)
        return row['title'] if row else "Неизвестная группа"

# --- Функции для статистики ---
def get_stats_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📅 Сегодня", callback_data="stats_day"),
            InlineKeyboardButton(text="📆 Неделя", callback_data="stats_week")
        ],
        [
            InlineKeyboardButton(text="📊 Месяц", callback_data="stats_month"),
            InlineKeyboardButton(text="🔄 3 дня", callback_data="stats_3days")
        ],
        [
            InlineKeyboardButton(text="❌ Закрыть", callback_data="stats_close")
        ]
    ])
    return keyboard

def parse_sale(text: str):
    """Парсит сообщение формата: 1000₽ нал Эл (3у)"""
    text_lower = text.lower()
    amount_match = re.search(r'(\d+)\s*[₽руб]', text_lower)
    if not amount_match:
        return None
    amount = float(amount_match.group(1))

    participants_match = re.search(r'\((\d+)\s*[у]\)', text_lower)
    if not participants_match:
        participants_match = re.search(r'(\d+)\s*(участника?|человека?|чел)', text_lower)
    participants = int(participants_match.group(1)) if participants_match else 1
    return {"amount": amount, "participants": participants}

def format_stats_message(stats, period_text: str):
    if not stats:
        return f"📭 {period_text} продаж нет."
    return (
        f"📊 *{period_text}*\n\n"
        f"💰 *Выручка:* {stats['revenue']:,.0f}₽\n"
        f"🧑‍🤝‍🧑 *Участников:* {stats['participants']}\n"
        f"🛒 *Сделок:* {stats['sales_count']}\n"
        f"📈 *Средний чек:* {stats['avg_check']:,.0f}₽"
    )

async def get_chat_id_for_stats(message: Message):
    """Возвращает chat_id для статистики: если в группе — id группы, иначе выбранную пользователем группу или первую из списка."""
    if message.chat.type in ["group", "supergroup"]:
        return message.chat.id
    # ЛС
    selected = await get_user_selected_chat(message.from_user.id)
    if selected is not None:
        return selected
    # Если пользователь ещё не выбрал, возьмём первую группу
    groups = await get_groups()
    if groups:
        return groups[0][0]
    return None

async def reply_with_stats(message: Message, period_func, period_text_func):
    chat_id = await get_chat_id_for_stats(message)
    if chat_id is None:
        await message.answer("⚠️ Бот не добавлен ни в одну группу. Добавьте его в группу и повторите.")
        return
    start, end = period_func()
    stats = await get_stats(start, end, chat_id)
    period_text = period_text_func()
    if message.chat.type not in ["group", "supergroup"]:
        title = await get_group_title(chat_id)
        period_text += f"\nГруппа: {title}"
    response = format_stats_message(stats, period_text)
    await message.answer(response, parse_mode=ParseMode.MARKDOWN)

# --- Хэндлеры ---
@dp.my_chat_member(ChatMemberUpdatedFilter(IS_NOT_MEMBER >> IS_MEMBER))
async def on_bot_added_to_group(event: ChatMemberUpdated):
    chat = event.chat
    if chat.type in ["group", "supergroup"]:
        await add_group(chat.id, chat.title)
        logger.info(f"Bot added to group: {chat.title} ({chat.id})")

@dp.my_chat_member(ChatMemberUpdatedFilter(IS_MEMBER >> IS_NOT_MEMBER))
async def on_bot_removed_from_group(event: ChatMemberUpdated):
    chat = event.chat
    if chat.type in ["group", "supergroup"]:
        await remove_group(chat.id)
        logger.info(f"Bot removed from group: {chat.id}")

@dp.message(F.chat.type.in_(["group", "supergroup"]))
async def handle_group_message(message: Message):
    if message.from_user.is_bot:
        return
    text = message.text or message.caption
    if not text or text.startswith('/'):
        return

    parsed = parse_sale(text)
    if parsed:
        await add_sale(message.date, parsed["amount"], parsed["participants"], text, message.from_user.id, message.chat.id)
        await message.reply(
            f"✅ Сохранено: {parsed['amount']:,.0f}₽, {parsed['participants']} уч.",
            disable_notification=True
        )
        logger.info(f"Sale saved: {parsed['amount']}₽, {parsed['participants']} уч. | {text}")

# --- Команды статистики ---
@dp.message(Command("day"))
async def stats_day(message: Message):
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today + timedelta(days=1)
    await reply_with_stats(message, lambda: (today, tomorrow), lambda: f"Статистика за сегодня ({datetime.now().strftime('%d.%m.%Y')})")

@dp.message(Command("week"))
async def stats_week(message: Message):
    today = datetime.now()
    start_of_week = today - timedelta(days=today.weekday())
    start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_week = start_of_week + timedelta(days=7)
    await reply_with_stats(message, lambda: (start_of_week, end_of_week), lambda: f"Статистика за неделю\n📅 {start_of_week.strftime('%d.%m.%Y')} — {(end_of_week - timedelta(days=1)).strftime('%d.%m.%Y')}")

@dp.message(Command("month"))
async def stats_month(message: Message):
    today = datetime.now()
    start_of_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if today.month == 12:
        end_of_month = today.replace(year=today.year+1, month=1, day=1)
    else:
        end_of_month = today.replace(month=today.month+1, day=1)
    await reply_with_stats(message, lambda: (start_of_month, end_of_month), lambda: f"Статистика за {today.strftime('%B %Y')}")

@dp.message(Command("3days"))
async def stats_3days(message: Message):
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    start_date = end_date - timedelta(days=3)
    await reply_with_stats(message, lambda: (start_date, end_date), lambda: f"Статистика за последние 3 дня\n📅 {start_date.strftime('%d.%m.%Y')} — {(end_date - timedelta(days=1)).strftime('%d.%m.%Y')}")

@dp.message(Command("custom"))
async def stats_custom(message: Message):
    try:
        parts = message.text.split()
        if len(parts) < 3:
            await message.answer("❌ Формат: /custom 2025-03-01 2025-03-21")
            return
        start = datetime.strptime(parts[1], "%Y-%m-%d")
        end = datetime.strptime(parts[2], "%Y-%m-%d") + timedelta(days=1)  # включаем конец дня
        if start >= end:
            await message.answer("❌ Начальная дата должна быть меньше конечной")
            return
        chat_id = await get_chat_id_for_stats(message)
        if chat_id is None:
            await message.answer("⚠️ Бот не добавлен ни в одну группу.")
            return
        stats = await get_stats(start, end, chat_id)
        period_text = f"Статистика за период\n📅 {parts[1]} — {parts[2]}"
        if message.chat.type not in ["group", "supergroup"]:
            title = await get_group_title(chat_id)
            period_text += f"\nГруппа: {title}"
        response = format_stats_message(stats, period_text)
        await message.answer(response, parse_mode=ParseMode.MARKDOWN)
    except ValueError:
        await message.answer("❌ Неверный формат даты. Используйте: /custom 2025-03-01 2025-03-21")
    except Exception as e:
        logger.exception("Custom stats error")
        await message.answer(f"❌ Ошибка: {e}")

@dp.message(Command("groups"))
async def show_groups(message: Message):
    groups = await get_groups()
    if not groups:
        await message.answer("Бот пока не добавлен ни в одну группу. Добавьте бота в группу и повторите.")
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    for chat_id, title in groups:
        keyboard.inline_keyboard.append([InlineKeyboardButton(text=title, callback_data=f"select_group_{chat_id}")])
    keyboard.inline_keyboard.append([InlineKeyboardButton(text="❌ Закрыть", callback_data="select_group_close")])
    await message.answer("Выберите группу для просмотра статистики:", reply_markup=keyboard)

@dp.callback_query(lambda c: c.data.startswith("select_group_"))
async def select_group_callback(callback: CallbackQuery):
    action = callback.data
    if action == "select_group_close":
        await callback.message.delete()
        await callback.answer()
        return
    chat_id = int(action.split("_")[2])
    user_id = callback.from_user.id
    await set_user_selected_chat(user_id, chat_id)
    group_title = await get_group_title(chat_id)
    await callback.message.edit_text(
        f"✅ Выбрана группа: {group_title}\n\nТеперь вы можете просматривать статистику по этой группе.",
        reply_markup=get_stats_keyboard()
    )
    await callback.answer()

@dp.callback_query()
async def handle_stats_buttons(callback: CallbackQuery):
    action = callback.data
    user_id = callback.from_user.id
    selected_chat = await get_user_selected_chat(user_id)
    if selected_chat is None:
        groups = await get_groups()
        if not groups:
            await callback.message.edit_text("⚠️ Бот не добавлен ни в одну группу.")
            await callback.answer()
            return
        selected_chat = groups[0][0]
        await set_user_selected_chat(user_id, selected_chat)

    group_title = await get_group_title(selected_chat)
    now = datetime.now()

    if action == "stats_day":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        stats = await get_stats(start, end, selected_chat)
        period_text = f"Статистика за сегодня ({now.strftime('%d.%m.%Y')})\nГруппа: {group_title}"
        response = format_stats_message(stats, period_text)
        await callback.message.edit_text(response, parse_mode=ParseMode.MARKDOWN, reply_markup=get_stats_keyboard())
    elif action == "stats_week":
        start = now - timedelta(days=now.weekday())
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=7)
        stats = await get_stats(start, end, selected_chat)
        period_text = f"Статистика за неделю\n📅 {start.strftime('%d.%m.%Y')} — {(end - timedelta(days=1)).strftime('%d.%m.%Y')}\nГруппа: {group_title}"
        response = format_stats_message(stats, period_text)
        await callback.message.edit_text(response, parse_mode=ParseMode.MARKDOWN, reply_markup=get_stats_keyboard())
    elif action == "stats_month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if now.month == 12:
            end = now.replace(year=now.year+1, month=1, day=1)
        else:
            end = now.replace(month=now.month+1, day=1)
        stats = await get_stats(start, end, selected_chat)
        period_text = f"Статистика за {now.strftime('%B %Y')}\nГруппа: {group_title}"
        response = format_stats_message(stats, period_text)
        await callback.message.edit_text(response, parse_mode=ParseMode.MARKDOWN, reply_markup=get_stats_keyboard())
    elif action == "stats_3days":
        end = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        start = end - timedelta(days=3)
        stats = await get_stats(start, end, selected_chat)
        period_text = f"Статистика за последние 3 дня\n📅 {start.strftime('%d.%m.%Y')} — {(end - timedelta(days=1)).strftime('%d.%m.%Y')}\nГруппа: {group_title}"
        response = format_stats_message(stats, period_text)
        await callback.message.edit_text(response, parse_mode=ParseMode.MARKDOWN, reply_markup=get_stats_keyboard())
    elif action == "stats_close":
        await callback.message.delete()
    await callback.answer()

@dp.message(Command("start"))
async def start_cmd(message: Message):
    if message.chat.type in ["group", "supergroup"]:
        await message.answer(
            "👋 Привет! Я бот для учета продаж.\n\n"
            "📝 Просто отправьте сообщение в формате:\n"
            "`1000₽ нал Эл (3у)`\n"
            "Или `1500 руб наличные (2У)`\n\n"
            "📊 Команды для статистики:\n"
            "/day - статистика за сегодня\n"
            "/week - за неделю\n"
            "/month - за месяц\n"
            "/3days - за последние 3 дня",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        groups = await get_groups()
        if groups:
            selected = await get_user_selected_chat(message.from_user.id)
            if selected is None:
                selected = groups[0][0]
                await set_user_selected_chat(message.from_user.id, selected)
            await message.answer(
                "👋 Привет! Я бот для учета продаж в картинг-группе.\n\n"
                "🔍 *Выберите период для статистики:*",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_stats_keyboard()
            )
        else:
            await message.answer(
                "👋 Привет! Я бот для учета продаж в картинг-группе.\n\n"
                "Сначала добавьте меня в группу, а затем используйте /groups для выбора группы.",
                parse_mode=ParseMode.MARKDOWN
            )

# --- Глобальный обработчик ошибок ---
@dp.errors()
async def on_error(update: types.Update, exception: Exception):
    logger.exception("Unhandled exception", exc_info=exception)
    return True

# --- Запуск ---
async def main():
    await init_db_pool()
    await init_tables()
    logger.info("Bot started")
    try:
        await dp.start_polling(bot)
    finally:
        await close_db_pool()

if __name__ == "__main__":
    asyncio.run(main())