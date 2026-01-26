import asyncio
import random
import traceback
import importlib
from html import escape
from contextlib import asynccontextmanager
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import CommandHandler, MessageHandler, filters, CallbackContext, ContextTypes
from telegram.error import BadRequest

from shivu import db, shivuu, application, LOGGER
from shivu.modules import ALL_MODULES

collection = db['anime_characters_lol']
user_collection = db['user_collection_lmaoooo']
user_totals_collection = db['user_totals_lmaoooo']
group_user_totals_collection = db['group_user_totalsssssss']
top_global_groups_collection = db['top_global_groups']

MESSAGE_FREQUENCY = 40
DESPAWN_TIME = 180
AMV_ALLOWED_GROUP_ID = -1003100468240

state = {
    'message_counts': {},
    'sent_characters': {},
    'last_characters': {},
    'first_correct_guesses': {},
    'spawn_messages': {},
    'spawn_message_links': {},
    'currently_spawning': {},
    'locks': {}
}

spawn_settings_collection = None
group_rarity_collection = None
get_spawn_settings = None
get_group_exclusive = None

SMALL_CAPS_MAP = str.maketrans({
    'A': 'ᴀ', 'B': 'ʙ', 'C': 'ᴄ', 'D': 'ᴅ', 'E': 'ᴇ', 'F': 'ғ', 'G': 'ɢ', 
    'H': 'ʜ', 'I': 'ɪ', 'J': 'ᴊ', 'K': 'ᴋ', 'L': 'ʟ', 'M': 'ᴍ', 'N': 'ɴ',
    'O': 'ᴏ', 'P': 'ᴘ', 'Q': 'ǫ', 'R': 'ʀ', 'S': 's', 'T': 'ᴛ', 'U': 'ᴜ',
    'V': 'ᴠ', 'W': 'ᴡ', 'X': 'x', 'Y': 'ʏ', 'Z': 'ᴢ',
    'a': 'ᴀ', 'b': 'ʙ', 'c': 'ᴄ', 'd': 'ᴅ', 'e': 'ᴇ', 'f': 'ғ', 'g': 'ɢ',
    'h': 'ʜ', 'i': 'ɪ', 'j': 'ᴊ', 'k': 'ᴋ', 'l': 'ʟ', 'm': 'ᴍ', 'n': 'ɴ',
    'o': 'ᴏ', 'p': 'ᴘ', 'q': 'ǫ', 'r': 'ʀ', 's': 's', 't': 'ᴛ', 'u': 'ᴜ',
    'v': 'ᴠ', 'w': 'ᴡ', 'x': 'x', 'y': 'ʏ', 'z': 'ᴢ'
})

def to_small_caps(text: str) -> str:
    return text.translate(SMALL_CAPS_MAP) if text else text

@asynccontextmanager
async def get_lock(chat_id: str):
    if chat_id not in state['locks']:
        state['locks'][chat_id] = asyncio.Lock()
    async with state['locks'][chat_id]:
        yield

async def is_character_allowed(character: dict, chat_id: int = None) -> bool:
    try:
        if character.get('removed', False):
            return False

        char_rarity = character.get('rarity', '🟢 Common')
        rarity_emoji = char_rarity.split()[0] if ' ' in str(char_rarity) else char_rarity
        is_video = character.get('is_video', False)

        if is_video and rarity_emoji == '🎥':
            return chat_id == AMV_ALLOWED_GROUP_ID

        if group_rarity_collection and chat_id:
            exclusive = await group_rarity_collection.find_one({
                'chat_id': chat_id,
                'rarity_emoji': rarity_emoji
            })
            if exclusive:
                return True

            other_exclusive = await group_rarity_collection.find_one({
                'rarity_emoji': rarity_emoji,
                'chat_id': {'$ne': chat_id}
            })
            if other_exclusive:
                return False

        if spawn_settings_collection and get_spawn_settings:
            settings = await get_spawn_settings()
            rarities = settings.get('rarities', {}) if settings else {}
            if rarity_emoji in rarities and not rarities[rarity_emoji].get('enabled', True):
                return False

        return True
    except Exception as e:
        LOGGER.error(f"Character filter error: {e}")
        return True

async def update_user_data(user_id: int, username: str, first_name: str, character: dict) -> None:
    user = await user_collection.find_one({'id': user_id})
    
    update_fields = {
        'username': username,
        'first_name': first_name
    }
    
    if user:
        await user_collection.update_one(
            {'id': user_id},
            {
                '$set': {k: v for k, v in update_fields.items() if v != user.get(k)},
                '$push': {'characters': character}
            }
        )
        if 'pass_data' in user:
            await user_collection.update_one(
                {'id': user_id},
                {'$inc': {'pass_data.tasks.grabs': 1}}
            )
    else:
        await user_collection.insert_one({
            'id': user_id,
            **update_fields,
            'characters': [character]
        })

async def update_group_stats(user_id: int, chat_id: int, username: str, first_name: str, group_name: str) -> None:
    await group_user_totals_collection.update_one(
        {'user_id': user_id, 'group_id': chat_id},
        {
            '$set': {'username': username, 'first_name': first_name},
            '$inc': {'count': 1}
        },
        upsert=True
    )
    
    await top_global_groups_collection.update_one(
        {'group_id': chat_id},
        {
            '$set': {'group_name': group_name},
            '$inc': {'count': 1}
        },
        upsert=True
    )

async def despawn_character(chat_id: int, message_id: int, character: dict, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        await asyncio.sleep(DESPAWN_TIME)

        if chat_id in state['first_correct_guesses']:
            for key in ['last_characters', 'spawn_messages', 'spawn_message_links', 'currently_spawning']:
                state[key].pop(chat_id, None)
            return

        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except BadRequest:
            pass

        rarity = character.get('rarity', '🟢 Common')
        rarity_emoji = rarity.split()[0] if ' ' in str(rarity) else '🟢'

        missed_caption = f"""⏰ ᴛɪᴍᴇ's ᴜᴘ! ʏᴏᴜ ᴀʟʟ ᴍɪssᴇᴅ ᴛʜɪs ᴡᴀɪғᴜ!

{rarity_emoji} ɴᴀᴍᴇ: <b>{escape(character.get('name', 'Unknown'))}</b>
⚡ ᴀɴɪᴍᴇ: <b>{escape(character.get('anime', 'Unknown'))}</b>
🎯 ʀᴀʀɪᴛʏ: <b>{escape(rarity)}</b>

💔 ʙᴇᴛᴛᴇʀ ʟᴜᴄᴋ ɴᴇxᴛ ᴛɪᴍᴇ!</b>"""

        send_method = context.bot.send_video if character.get('is_video') else context.bot.send_photo
        media_key = 'video' if character.get('is_video') else 'photo'
        
        missed_msg = await send_method(
            chat_id=chat_id,
            **{media_key: character.get('img_url')},
            caption=missed_caption,
            parse_mode='HTML'
        )

        await asyncio.sleep(10)
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=missed_msg.message_id)
        except BadRequest:
            pass

        for key in ['last_characters', 'spawn_messages', 'spawn_message_links', 'currently_spawning']:
            state[key].pop(chat_id, None)

    except Exception as e:
        LOGGER.error(f"Despawn error: {e}\n{traceback.format_exc()}")

async def message_counter(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.type not in ['group', 'supergroup']:
        return

    chat_id = str(update.effective_chat.id)
    
    async with get_lock(chat_id):
        state['message_counts'][chat_id] = state['message_counts'].get(chat_id, 0) + 1

        if state['message_counts'][chat_id] >= MESSAGE_FREQUENCY:
            if not state['currently_spawning'].get(chat_id, False):
                state['currently_spawning'][chat_id] = True
                state['message_counts'][chat_id] = 0
                asyncio.create_task(send_image(update, context))

async def select_character(allowed_chars: list, chat_id: int) -> dict:
    try:
        group_setting = None
        if group_rarity_collection and get_group_exclusive:
            group_setting = await get_group_exclusive(chat_id)

        global_rarities = {}
        if spawn_settings_collection and get_spawn_settings:
            settings = await get_spawn_settings()
            global_rarities = settings.get('rarities', {}) if settings else {}

        rarity_pools = {}
        for char in allowed_chars:
            emoji = char.get('rarity', '🟢 Common').split()[0] if ' ' in str(char.get('rarity', '')) else '🟢'
            rarity_pools.setdefault(emoji, []).append(char)

        weighted_choices = []

        if group_setting and group_setting['rarity_emoji'] in rarity_pools:
            weighted_choices.append({
                'chars': rarity_pools[group_setting['rarity_emoji']],
                'chance': group_setting.get('chance', 10.0)
            })

        for emoji, rarity_data in global_rarities.items():
            if not rarity_data.get('enabled', True):
                continue
            if group_setting and emoji == group_setting['rarity_emoji']:
                continue
            if emoji in rarity_pools:
                weighted_choices.append({
                    'chars': rarity_pools[emoji],
                    'chance': rarity_data.get('chance', 5.0)
                })

        if weighted_choices:
            total = sum(c['chance'] for c in weighted_choices)
            rand = random.uniform(0, total)
            cumulative = 0
            
            for choice in weighted_choices:
                cumulative += choice['chance']
                if rand <= cumulative:
                    return random.choice(choice['chars'])

    except Exception as e:
        LOGGER.error(f"Character selection error: {e}")

    return random.choice(allowed_chars)

async def send_image(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id

    try:
        all_characters = await collection.find({}).to_list(length=None)
        
        if not all_characters:
            state['currently_spawning'][str(chat_id)] = False
            return

        if chat_id not in state['sent_characters']:
            state['sent_characters'][chat_id] = []

        if len(state['sent_characters'][chat_id]) >= len(all_characters):
            state['sent_characters'][chat_id] = []

        available = [c for c in all_characters if c.get('id') not in state['sent_characters'][chat_id]]
        
        if not available:
            available = all_characters
            state['sent_characters'][chat_id] = []

        allowed = [c for c in available if await is_character_allowed(c, chat_id)]
        
        if not allowed:
            state['currently_spawning'][str(chat_id)] = False
            return

        character = await select_character(allowed, chat_id)

        state['sent_characters'][chat_id].append(character['id'])
        state['last_characters'][chat_id] = character
        state['first_correct_guesses'].pop(chat_id, None)

        caption = """✨ ʟᴏᴏᴋ! ᴀ ᴡᴀɪꜰᴜ ʜᴀꜱ ᴀᴘᴘᴇᴀʀᴇᴅ ✨
✦ ᴍᴀᴋᴇ ʜᴇʀ ʏᴏᴜʀꜱ — ᴛʏᴘᴇ /ɢʀᴀʙ &lt;ᴡᴀɪꜰᴜ_ɴᴀᴍᴇ&gt;

⏳ ᴛɪᴍᴇ ʟɪᴍɪᴛ: 3 ᴍɪɴᴜᴛᴇꜱ!</b>"""

        send_method = context.bot.send_video if character.get('is_video') else context.bot.send_photo
        media_key = 'video' if character.get('is_video') else 'photo'
        
        spawn_msg = await send_method(
            chat_id=chat_id,
            **{media_key: character.get('img_url')},
            caption=caption,
            parse_mode='HTML'
        )

        state['spawn_messages'][chat_id] = spawn_msg.message_id

        username = update.effective_chat.username
        if username:
            state['spawn_message_links'][chat_id] = f"https://t.me/{username}/{spawn_msg.message_id}"
        else:
            chat_id_str = str(chat_id).replace('-100', '')
            state['spawn_message_links'][chat_id] = f"https://t.me/c/{chat_id_str}/{spawn_msg.message_id}"

        state['currently_spawning'][str(chat_id)] = False

        asyncio.create_task(despawn_character(chat_id, spawn_msg.message_id, character, context))

    except Exception as e:
        LOGGER.error(f"Spawn error: {e}\n{traceback.format_exc()}")
        state['currently_spawning'][str(chat_id)] = False

async def guess(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    try:
        if chat_id not in state['last_characters']:
            await update.message.reply_html('<b>ɴᴏ ᴄʜᴀʀᴀᴄᴛᴇʀ ʜᴀs sᴘᴀᴡɴᴇᴅ ʏᴇᴛ!</b>')
            return

        if chat_id in state['first_correct_guesses']:
            await update.message.reply_html('<b>🚫 ᴡᴀɪғᴜ ᴀʟʀᴇᴀᴅʏ ɢʀᴀʙʙᴇᴅ ʙʏ sᴏᴍᴇᴏɴᴇ ᴇʟsᴇ ⚡</b>')
            return

        guess_text = ' '.join(context.args).lower() if context.args else ''

        if not guess_text:
            await update.message.reply_html('<b>ᴘʟᴇᴀsᴇ ᴘʀᴏᴠɪᴅᴇ ᴀ ɴᴀᴍᴇ!</b>')
            return

        if "()" in guess_text or "&" in guess_text:
            await update.message.reply_html("<b>ɴᴀʜʜ ʏᴏᴜ ᴄᴀɴ'ᴛ ᴜsᴇ ᴛʜɪs ᴛʏᴘᴇs ᴏғ ᴡᴏʀᴅs...❌</b>")
            return

        character_name = state['last_characters'][chat_id].get('name', '').lower()
        name_parts = character_name.split()

        is_correct = (
            sorted(name_parts) == sorted(guess_text.split()) or
            any(part == guess_text for part in name_parts) or
            guess_text == character_name
        )

        if is_correct:
            state['first_correct_guesses'][chat_id] = user_id

            if chat_id in state['spawn_messages']:
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=state['spawn_messages'][chat_id])
                except BadRequest:
                    pass
                state['spawn_messages'].pop(chat_id, None)

            character = state['last_characters'][chat_id]
            
            await update_user_data(
                user_id,
                getattr(update.effective_user, 'username', None),
                update.effective_user.first_name,
                character
            )

            await update_group_stats(
                user_id,
                chat_id,
                getattr(update.effective_user, 'username', None),
                update.effective_user.first_name,
                update.effective_chat.title
            )

            char_name = to_small_caps(character.get('name', 'Unknown'))
            anime = to_small_caps(character.get('anime', 'Unknown'))
            rarity = to_small_caps(character.get('rarity', '🟢 Common'))
            owner_name = to_small_caps(update.effective_user.first_name)

            success_message = f"""🎊 ᴄᴏɴɢʀᴀᴛᴜʟᴀᴛɪᴏɴs! ɴᴇᴡ ᴄʜᴀʀᴀᴄᴛᴇʀ ᴜɴʟᴏᴄᴋᴇᴅ 🎊
╭════════•┈┈┈┈•════════╮
┃ ✦ ɴᴀᴍᴇ: 𓂃ࣰࣲ {escape(char_name)}
┃ ✦ ʀᴀʀɪᴛʏ: {escape(rarity)}
┃ ✦ ᴀɴɪᴍᴇ: {escape(anime)}
┃ ✦ ɪᴅ: 🆔 {escape(str(character.get('id', 'Unknown')))}
┃ ✦ ꜱᴛᴀᴛᴜꜱ: ᴀᴅᴅᴇᴅ ᴛᴏ ʜᴀʀᴇᴍ ✅
┃ ✦ ᴏᴡɴᴇʀ: ✧ {escape(owner_name)}
╰════════•┈┈┈┈•════════╯

✧ ᴄʜᴀʀᴀᴄᴛᴇʀ ꜱᴜᴄᴄᴇꜱꜱғᴜʟʟʏ ᴀᴅᴅᴇᴅ ɪɴ ʏᴏᴜʀ ʜᴀʀᴇᴍ ✅</b>"""

            keyboard = [[InlineKeyboardButton("🪼 ʜᴀʀᴇᴍ", switch_inline_query_current_chat=f"collection.{user_id}")]]

            await update.message.reply_text(
                success_message,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

            state['spawn_message_links'].pop(chat_id, None)

        else:
            keyboard = []
            if chat_id in state['spawn_message_links']:
                keyboard.append([InlineKeyboardButton("📍 ᴠɪᴇᴡ sᴘᴀᴡɴ ᴍᴇssᴀɢᴇ", url=state['spawn_message_links'][chat_id])])

            await update.message.reply_html(
                '<b>ᴘʟᴇᴀsᴇ ᴡʀɪᴛᴇ ᴀ ᴄᴏʀʀᴇᴄᴛ ɴᴀᴍᴇ..❌</b>',
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
            )

    except Exception as e:
        LOGGER.error(f"Guess error: {e}\n{traceback.format_exc()}")

async def main():
    try:
        for module_name in ALL_MODULES:
            try:
                importlib.import_module(f"shivu.modules.{module_name}")
                LOGGER.info(f"✅ Loaded: {module_name}")
            except Exception as e:
                LOGGER.error(f"❌ Failed: {module_name} - {e}")

        try:
            from shivu.modules.rarity import (
                spawn_settings_collection as ssc,
                group_rarity_collection as grc,
                get_spawn_settings as gss,
                get_group_exclusive as gge
            )
            global spawn_settings_collection, group_rarity_collection
            global get_spawn_settings, get_group_exclusive
            spawn_settings_collection = ssc
            group_rarity_collection = grc
            get_spawn_settings = gss
            get_group_exclusive = gge
            LOGGER.info("✅ Rarity system loaded")
        except Exception as e:
            LOGGER.warning(f"⚠️ Rarity system unavailable: {e}")

        try:
            from shivu.modules.backup import setup_backup_handlers
            setup_backup_handlers(application)
            LOGGER.info("✅ Backup system initialized")
        except Exception as e:
            LOGGER.warning(f"⚠️ Backup system unavailable: {e}")

        await shivuu.start()
        LOGGER.info("✅ Pyrogram started")

        application.add_handler(CommandHandler(["grab", "g"], guess, block=False))
        application.add_handler(MessageHandler(filters.ALL, message_counter, block=False))

        await application.initialize()
        await application.start()
        await application.updater.start_polling(drop_pending_updates=True)

        LOGGER.info("✅ Bot started successfully")

        while True:
            await asyncio.sleep(3600)

    except Exception as e:
        LOGGER.error(f"❌ Fatal error: {e}\n{traceback.format_exc()}")
    finally:
        try:
            await application.stop()
            await application.shutdown()
            await shivuu.stop()
        except Exception as e:
            LOGGER.error(f"Cleanup error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOGGER.info("Bot stopped by user")