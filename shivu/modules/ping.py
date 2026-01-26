import time
from datetime import timedelta

import psutil
import httpx
from telegram import Update
from telegram.ext import CommandHandler, ContextTypes

from shivu import application, sudo_users, user_collection

# Store bot start time
BOT_START_TIME = time.time()


def format_uptime(seconds: int) -> str:
    uptime = timedelta(seconds=seconds)
    days = uptime.days
    hours, remainder = divmod(uptime.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{days}d {hours}h {minutes}m {seconds}s"


def format_mb(value: float) -> str:
    return f"{value / (1024 * 1024):.1f} MB"


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if str(update.effective_user.id) not in sudo_users:
        await update.message.reply_text("Nouu.. its Sudo user's Command..")
        return

    # 1) Telegram message latency
    start_msg = time.time()
    msg = await update.message.reply_text("Pong")
    msg_latency = (time.time() - start_msg) * 1000

    # 2) Telegram API latency
    start_api = time.time()
    await context.bot.get_me()
    api_latency = (time.time() - start_api) * 1000

    # 3) MongoDB latency
    mongo_latency = None
    try:
        start_db = time.time()
        await user_collection.database.command("ping")
        mongo_latency = (time.time() - start_db) * 1000
    except Exception:
        mongo_latency = None

    # 4) HTTP latency
    http_latency = None
    try:
        start_http = time.time()
        async with httpx.AsyncClient(timeout=5) as client:
            await client.get("https://www.google.com")
        http_latency = (time.time() - start_http) * 1000
    except Exception:
        http_latency = None

    # Uptime
    uptime_seconds = int(time.time() - BOT_START_TIME)
    uptime_text = format_uptime(uptime_seconds)

    # CPU + RAM
    cpu_percent = psutil.cpu_percent(interval=0.2)
    ram = psutil.virtual_memory()
    process = psutil.Process()
    bot_ram = process.memory_info().rss

    response = (
        "<blockquote>"
        "<b>Pong</b>\n"
        f"<b>Message:</b> {msg_latency:.2f} ms\n"
        f"<b>API:</b> {api_latency:.2f} ms\n"
        f"<b>Mongo:</b> {mongo_latency:.2f} ms\n" if mongo_latency is not None else
        "<b>Mongo:</b> N/A\n"
    )

    response += (
        f"<b>HTTP:</b> {http_latency:.2f} ms\n" if http_latency is not None else
        "<b>HTTP:</b> N/A\n"
    )

    response += (
        f"<b>Uptime:</b> {uptime_text}\n"
        f"<b>CPU:</b> {cpu_percent:.1f}%\n"
        f"<b>RAM:</b> {format_mb(bot_ram)} (bot) | {format_mb(ram.used)} / {format_mb(ram.total)}"
        "</blockquote>"
    )

    await msg.edit_text(response, parse_mode="HTML")


application.add_handler(CommandHandler("ping", ping))