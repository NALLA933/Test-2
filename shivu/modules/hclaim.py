import logging
from datetime import datetime, timedelta, timezone
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CommandHandler, CallbackContext
from telegram.constants import ParseMode
import pytz

from shivu import application, user_collection, collection

KOLKATA_TZ = pytz.timezone('Asia/Kolkata')
UTC_TZ = pytz.UTC
active_claims = set()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_character(user_id):
    try:
        user_data = await user_collection.find_one({'id': user_id})
        claimed_ids = [c['id'] for c in user_data.get('characters', [])] if user_data else []
        cursor = collection.aggregate([
            {'$match': {'id': {'$nin': claimed_ids}}},
            {'$sample': {'size': 1}}
        ])
        result = await cursor.to_list(length=1)
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Fetch error: {e}")
        return None

def format_time(delta):
    s = int(delta.total_seconds())
    h, r = divmod(s, 3600)
    m, s = divmod(r, 60)
    return f"{h}h {m}m {s}s"

async def daily_claim(update: Update, context: CallbackContext):
    user = update.effective_user
    uid = user.id
    
    if uid in active_claims:
        await update.message.reply_text("⚙️ <b>Processing...</b>", parse_mode=ParseMode.HTML)
        return
    
    active_claims.add(uid)
    
    try:
        now = datetime.now(UTC_TZ)
        user_data = await user_collection.find_one({'id': uid}) or {}
        last = user_data.get('last_daily_claim')
        
        if last:
            if isinstance(last, str):
                last = datetime.fromisoformat(last.replace('Z', '+00:00'))
            if last.tzinfo is None:
                last = UTC_TZ.localize(last)
            elapsed = now - last
            if elapsed < timedelta(hours=24):
                remaining = timedelta(hours=24) - elapsed
                await update.message.reply_text(
                    f"🕒 <b>Cooldown Active</b>\n\n⌛ Time left: {format_time(remaining)}",
                    parse_mode=ParseMode.HTML
                )
                return
        
        char = await fetch_character(uid)
        if not char:
            await update.message.reply_text("❗ <b>No characters available</b>", parse_mode=ParseMode.HTML)
            return
        
        await user_collection.update_one(
            {'id': uid},
            {'$push': {'characters': char}, '$set': {'last_daily_claim': now, 'first_name': user.first_name}},
            upsert=True
        )
        
        caption = (
            f"<b>✨ Daily Claim Success!</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👤 {user.first_name}\n"
            f"🎴 {char.get('name')}\n"
            f"🎬 {char.get('anime')}\n"
            f"🆔 {char.get('id')}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💡 Next claim in 24h"
        )
        
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🎒 Collection", switch_inline_query_current_chat=f"collection.{uid}")
        ]])
        
        await update.message.reply_photo(
            photo=char.get('img_url'),
            caption=caption,
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Error: {e}")
        await update.message.reply_text("❗ <b>Error occurred</b>", parse_mode=ParseMode.HTML)
    finally:
        active_claims.discard(uid)

application.add_handler(CommandHandler('hclaim', daily_claim, block=False))