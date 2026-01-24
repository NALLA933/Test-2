import shivu.mongodb_patch
import importlib
import asyncio
import random
import traceback
from html import escape
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import CommandHandler, CallbackContext, MessageHandler, filters, Application
from telegram.error import BadRequest
from pymongo.errors import DuplicateKeyError
from datetime import datetime

from shivu import db, shivuu, application, LOGGER
from shivu.modules import ALL_MODULES

# ==================== MONGODB INDEX SAFETY PATCH ====================
from pymongo import collection as pymongo_collection
from pymongo.errors import OperationFailure, DuplicateKeyError

# Store original methods
_orig_create_index = pymongo_collection.Collection.create_index

def _safe_create_index(self, keys, **kwargs):
    """
    Safe create_index that prevents crashes from duplicate/conflicting index creation.
    Handles both IndexKeySpecsConflict (code 86) and DuplicateKeyError (code 11000).
    """
    try:
        return _orig_create_index(self, keys, **kwargs)
    
    except (OperationFailure, DuplicateKeyError) as e:
        # Get error code for MongoDB errors
        error_code = getattr(e, 'code', None)
        
        # Handle IndexKeySpecsConflict (86) - index already exists with different specs
        if error_code == 86:
            LOGGER.debug(f"IndexKeySpecsConflict: Skipping index creation on {self.name} for keys {keys}. Index already exists with different specifications.")
            return None
        
        # Handle DuplicateKeyError (11000)
        elif error_code == 11000:
            LOGGER.debug(f"DuplicateKeyError: Skipping index creation on {self.name} for keys {keys}. Duplicate key constraint violation.")
            return None
        
        # Handle generic DuplicateKeyError (without code attribute)
        elif isinstance(e, DuplicateKeyError):
            LOGGER.debug(f"DuplicateKeyError: Skipping index creation on {self.name} for keys {keys}. {str(e)}")
            return None
        
        # Re-raise all other exceptions
        else:
            LOGGER.error(f"Unexpected error creating index on {self.name}: {type(e).__name__}: {str(e)}")
            raise

# Apply the monkey patch
pymongo_collection.Collection.create_index = _safe_create_index

# Patch create_indexes for bulk operations
if hasattr(pymongo_collection.Collection, 'create_indexes'):
    _orig_create_indexes = pymongo_collection.Collection.create_indexes
    
    def _safe_create_indexes(self, indexes, **kwargs):
        try:
            return _orig_create_indexes(self, indexes, **kwargs)
        except (OperationFailure, DuplicateKeyError) as e:
            error_code = getattr(e, 'code', None)
            if error_code in [86, 11000] or isinstance(e, DuplicateKeyError):
                LOGGER.debug(f"Suppressed create_indexes error on {self.name}")
                # Return empty list to indicate no indexes were created
                return []
            raise
    
    pymongo_collection.Collection.create_indexes = _safe_create_indexes

# Patch ensure_index for compatibility with older code
if hasattr(pymongo_collection.Collection, 'ensure_index'):
    _orig_ensure_index = pymongo_collection.Collection.ensure_index
    
    def _safe_ensure_index(self, keys, **kwargs):
        try:
            return _orig_ensure_index(self, keys, **kwargs)
        except (OperationFailure, DuplicateKeyError) as e:
            error_code = getattr(e, 'code', None)
            if error_code in [86, 11000] or isinstance(e, DuplicateKeyError):
                LOGGER.debug(f"Suppressed ensure_index error on {self.name}")
                return None
            raise
    
    pymongo_collection.Collection.ensure_index = _safe_ensure_index

# ==================== MIGRATION FUNCTIONS ====================

async def migrate_user_data():
    """Migrate user data from old collection to new collection with unique index"""
    
    # Define collection names
    OLD_COLLECTION_NAME = 'user_collection_lmaoooo'
    NEW_COLLECTION_NAME = 'users'
    
    # Get collections
    old_collection = db[OLD_COLLECTION_NAME]
    new_collection = db[NEW_COLLECTION_NAME]
    
    # Check if migration has already been done
    migration_check = await db.migration_metadata.find_one({'migration': 'user_collection_migration'})
    if migration_check and migration_check.get('completed'):
        LOGGER.info("✅ User data migration already completed")
        return True
    
    LOGGER.info("🔄 Starting user data migration...")
    
    try:
        # Create unique index on new collection (this will be safe due to our patch)
        await new_collection.create_index([('id', 1)], unique=True, name='unique_user_id')
        LOGGER.info("✅ Created unique index on 'id' field in new collection")
        
        # Get all documents from old collection
        total_docs = await old_collection.count_documents({})
        LOGGER.info(f"📊 Found {total_docs} documents in old collection")
        
        # Track migration stats
        migrated_count = 0
        duplicate_count = 0
        error_count = 0
        
        # Use aggregation to group by 'id' and get the first document for each user
        pipeline = [
            {
                '$sort': {'_id': 1}  # Sort by MongoDB _id to get oldest first
            },
            {
                '$group': {
                    '_id': '$id',
                    'doc': {'$first': '$$ROOT'}
                }
            }
        ]
        
        # Process unique documents
        async for group in old_collection.aggregate(pipeline):
            try:
                user_doc = group['doc']
                
                # Remove the _id field to let MongoDB generate a new one
                user_doc.pop('_id', None)
                
                # Insert into new collection
                await new_collection.insert_one(user_doc)
                migrated_count += 1
                
                if migrated_count % 100 == 0:
                    LOGGER.info(f"📈 Migrated {migrated_count}/{total_docs} users...")
                    
            except DuplicateKeyError:
                # This shouldn't happen due to aggregation grouping, but just in case
                duplicate_count += 1
                LOGGER.debug(f"⚠️ Skipped duplicate user_id: {group['_id']}")
            except Exception as e:
                error_count += 1
                LOGGER.error(f"❌ Error migrating user {group['_id']}: {e}")
        
        # Update migration metadata
        await db.migration_metadata.update_one(
            {'migration': 'user_collection_migration'},
            {
                '$set': {
                    'completed': True,
                    'migrated_count': migrated_count,
                    'duplicate_count': duplicate_count,
                    'error_count': error_count,
                    'migrated_at': datetime.utcnow(),
                    'source_collection': OLD_COLLECTION_NAME,
                    'target_collection': NEW_COLLECTION_NAME
                }
            },
            upsert=True
        )
        
        LOGGER.info(f"""
✅ Migration completed!
   Total migrated: {migrated_count}
   Duplicates skipped: {duplicate_count}
   Errors: {error_count}
        """)
        
        return True
        
    except Exception as e:
        LOGGER.error(f"❌ Migration failed: {e}")
        LOGGER.error(traceback.format_exc())
        return False

async def verify_migration():
    """Verify that migration was successful"""
    old_collection = db['user_collection_lmaoooo']
    new_collection = db['users']
    
    # Count unique users in old collection
    pipeline = [
        {'$group': {'_id': '$id'}},
        {'$count': 'unique_users'}
    ]
    
    old_unique_count = 0
    async for result in old_collection.aggregate(pipeline):
        old_unique_count = result['unique_users']
    
    # Count users in new collection
    new_count = await new_collection.count_documents({})
    
    LOGGER.info(f"""
🔍 Migration Verification:
   Unique users in old collection: {old_unique_count}
   Users in new collection: {new_count}
   Match: {old_unique_count == new_count}
    """)
    
    # Check for duplicate ids in new collection
    duplicate_check = await new_collection.aggregate([
        {'$group': {'_id': '$id', 'count': {'$sum': 1}}},
        {'$match': {'count': {'$gt': 1}}},
        {'$count': 'duplicates'}
    ]).to_list(length=1)
    
    duplicates = duplicate_check[0]['duplicates'] if duplicate_check else 0
    LOGGER.info(f"   Duplicate IDs in new collection: {duplicates}")
    
    return old_unique_count == new_count and duplicates == 0

async def cleanup_old_collection():
    """Safely remove old collection after verification"""
    migration_check = await db.migration_metadata.find_one({
        'migration': 'user_collection_migration',
        'completed': True
    })
    
    if not migration_check:
        LOGGER.warning("⚠️ Cannot cleanup - migration not completed")
        return False
    
    # Verify migration first
    verification_ok = await verify_migration()
    
    if verification_ok:
        LOGGER.info("✅ Migration verified successfully")
        
        # Optional: Backup old collection before deletion
        backup_name = f"user_collection_lmaoooo_backup_{datetime.utcnow().strftime('%Y%m%d')}"
        await db.command({
            'renameCollection': f"{db.name}.user_collection_lmaoooo",
            'to': f"{db.name}.{backup_name}"
        })
        
        LOGGER.info(f"📦 Old collection backed up as: {backup_name}")
        return True
    else:
        LOGGER.error("❌ Migration verification failed - not cleaning up")
        return False

# ==================== BOT CONFIGURATION ====================

# Small caps conversion function
def to_small_caps(text):
    """Convert normal text to small caps unicode"""
    if not text:
        return text
    
    # Mapping for common characters to small caps
    small_caps_map = {
        'A': 'ᴀ', 'B': 'ʙ', 'C': 'ᴄ', 'D': 'ᴅ', 'E': 'ᴇ', 'F': 'ғ', 'G': 'ɢ', 
        'H': 'ʜ', 'I': 'ɪ', 'J': 'ᴊ', 'K': 'ᴋ', 'L': 'ʟ', 'M': 'ᴍ', 'N': 'ɴ',
        'O': 'ᴏ', 'P': 'ᴘ', 'Q': 'ǫ', 'R': 'ʀ', 'S': 's', 'T': 'ᴛ', 'U': 'ᴜ',
        'V': 'ᴠ', 'W': 'ᴡ', 'X': 'x', 'Y': 'ʏ', 'Z': 'ᴢ',
        'a': 'ᴀ', 'b': 'ʙ', 'c': 'ᴄ', 'd': 'ᴅ', 'e': 'ᴇ', 'f': 'ғ', 'g': 'ɢ',
        'h': 'ʜ', 'i': 'ɪ', 'j': 'ᴊ', 'k': 'ᴋ', 'l': 'ʟ', 'm': 'ᴍ', 'n': 'ɴ',
        'o': 'ᴏ', 'p': 'ᴘ', 'q': 'ǫ', 'r': 'ʀ', 's': 's', 't': 'ᴛ', 'u': 'ᴜ',
        'v': 'ᴠ', 'w': 'ᴡ', 'x': 'x', 'y': 'ʏ', 'z': 'ᴢ',
        ' ': ' ', '!': '!', '?': '?', ':': ':', '-': '-', '.': '.', ',': ',',
        '0': '0', '1': '1', '2': '2', '3': '3', '4': '4', '5': '5', '6': '6',
        '7': '7', '8': '8', '9': '9', '(': '(', ')': ')', '[': '[', ']': ']',
        '{': '{', '}': '}', '@': '@', '#': '#', '$': '$', '%': '%', '^': '^',
        '&': '&', '*': '*', '_': '_', '+': '+', '=': '=', '<': '<', '>': '>',
        '/': '/', '\\': '\\', '|': '|', '~': '~', '`': '`', '"': '"', "'": "'"
    }
    
    result = []
    for char in str(text):
        result.append(small_caps_map.get(char, char))
    return ''.join(result)

# Database collections
collection = db['anime_characters_lol']
user_collection = db['users']  # CHANGED: Now using new collection
user_totals_collection = db['user_totals_lmaoooo']
group_user_totals_collection = db['group_user_totalsssssss']
top_global_groups_collection = db['top_global_groups']

# Bot constants
MESSAGE_FREQUENCY = 40
DESPAWN_TIME = 180
AMV_ALLOWED_GROUP_ID = -1003100468240

# Global dictionaries for state management
locks = {}
message_counts = {}
sent_characters = {}
last_characters = {}
first_correct_guesses = {}
spawn_messages = {}
spawn_message_links = {}
currently_spawning = {}

# Rarity system variables (will be imported later)
spawn_settings_collection = None
group_rarity_collection = None
get_spawn_settings = None
get_group_exclusive = None

# Import all modules
for module_name in ALL_MODULES:
    try:
        importlib.import_module("shivu.modules." + module_name)
        LOGGER.info(f"✅ Module loaded: {module_name}")
    except Exception as e:
        LOGGER.error(f"❌ Module failed: {module_name} - {e}")

# ==================== HELPER FUNCTIONS ====================

async def is_character_allowed(character, chat_id=None):
    """Check if a character can spawn in the given chat"""
    try:
        if character.get('removed', False):
            LOGGER.debug(f"Character {character.get('name')} is removed")
            return False

        char_rarity = character.get('rarity', '🟢 Common')
        rarity_emoji = char_rarity.split(' ')[0] if isinstance(char_rarity, str) and ' ' in char_rarity else char_rarity
        
        is_video = character.get('is_video', False)
        
        # AMV restriction
        if is_video and rarity_emoji == '🎥':
            if chat_id == AMV_ALLOWED_GROUP_ID:
                LOGGER.info(f"✅ AMV {character.get('name')} allowed in main group")
                return True
            else:
                LOGGER.debug(f"❌ AMV {character.get('name')} blocked in group {chat_id}")
                return False

        # Group exclusive rarity check
        if group_rarity_collection is not None and chat_id:
            try:
                current_group_exclusive = await group_rarity_collection.find_one({
                    'chat_id': chat_id,
                    'rarity_emoji': rarity_emoji
                })
                if current_group_exclusive:
                    return True

                other_group_exclusive = await group_rarity_collection.find_one({
                    'rarity_emoji': rarity_emoji,
                    'chat_id': {'$ne': chat_id}
                })
                if other_group_exclusive:
                    return False
            except Exception as e:
                LOGGER.error(f"Error checking group exclusivity: {e}")

        # Global rarity settings check
        if spawn_settings_collection is not None and get_spawn_settings is not None:
            try:
                settings = await get_spawn_settings()
                if settings and settings.get('rarities'):
                    rarities = settings['rarities']
                    if rarity_emoji in rarities:
                        is_enabled = rarities[rarity_emoji].get('enabled', True)
                        if not is_enabled:
                            return False
            except Exception as e:
                LOGGER.error(f"Error checking global rarity: {e}")

        return True

    except Exception as e:
        LOGGER.error(f"Error in is_character_allowed: {e}\n{traceback.format_exc()}")
        return True

async def get_chat_message_frequency(chat_id):
    """Get message frequency setting for a chat"""
    try:
        chat_frequency = await user_totals_collection.find_one({'chat_id': str(chat_id)})
        if chat_frequency:
            return chat_frequency.get('message_frequency', MESSAGE_FREQUENCY)
        else:
            await user_totals_collection.insert_one({
                'chat_id': str(chat_id),
                'message_frequency': MESSAGE_FREQUENCY
            })
            return MESSAGE_FREQUENCY
    except Exception as e:
        LOGGER.error(f"Error in get_chat_message_frequency: {e}")
        return MESSAGE_FREQUENCY

async def update_grab_task(user_id: int):
    """Update grab task count for a user"""
    try:
        user = await user_collection.find_one({'id': user_id})
        if user and 'pass_data' in user:
            await user_collection.update_one(
                {'id': user_id},
                {'$inc': {'pass_data.tasks.grabs': 1}}
            )
    except Exception as e:
        LOGGER.error(f"Error in update_grab_task: {e}")

async def despawn_character(chat_id, message_id, character, context):
    """Remove character after timeout if not grabbed"""
    try:
        await asyncio.sleep(DESPAWN_TIME)

        if chat_id in first_correct_guesses:
            last_characters.pop(chat_id, None)
            spawn_messages.pop(chat_id, None)
            spawn_message_links.pop(chat_id, None)
            currently_spawning.pop(str(chat_id), None)
            return

        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except BadRequest as e:
            LOGGER.warning(f"Could not delete spawn message: {e}")

        rarity = character.get('rarity', '🟢 Common')
        rarity_emoji = rarity.split(' ')[0] if isinstance(rarity, str) and ' ' in rarity else '🟢'

        is_video = character.get('is_video', False)
        media_url = character.get('img_url')

        # Escape character details for HTML
        char_name = escape(character.get('name', 'Unknown'))
        char_anime = escape(character.get('anime', 'Unknown'))
        char_rarity = escape(rarity)

        missed_caption = f"""⏰ ᴛɪᴍᴇ's ᴜᴘ! ʏᴏᴜ ᴀʟʟ ᴍɪssᴇᴅ ᴛʜɪs ᴡᴀɪғᴜ!

{rarity_emoji} ɴᴀᴍᴇ: <b>{char_name}</b>
⚡ ᴀɴɪᴍᴇ: <b>{char_anime}</b>
🎯 ʀᴀʀɪᴛʏ: <b>{char_rarity}</b>

💔 ʙᴇᴛᴛᴇʀ ʟᴜᴄᴋ ɴᴇxᴛ ᴛɪᴍᴇ!"""

        if is_video:
            missed_msg = await context.bot.send_video(
                chat_id=chat_id,
                video=media_url,
                caption=missed_caption,
                parse_mode='HTML',
                supports_streaming=True
            )
        else:
            missed_msg = await context.bot.send_photo(
                chat_id=chat_id,
                photo=media_url,
                caption=missed_caption,
                parse_mode='HTML'
            )

        await asyncio.sleep(10)
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=missed_msg.message_id)
        except BadRequest as e:
            LOGGER.warning(f"Could not delete missed message: {e}")

        last_characters.pop(chat_id, None)
        spawn_messages.pop(chat_id, None)
        spawn_message_links.pop(chat_id, None)
        currently_spawning.pop(str(chat_id), None)

    except Exception as e:
        LOGGER.error(f"Error in despawn_character: {e}")
        LOGGER.error(traceback.format_exc())

# ==================== MESSAGE HANDLERS ====================

async def message_counter(update: Update, context: CallbackContext) -> None:
    """Count messages and trigger character spawns"""
    try:
        if update.effective_chat.type not in ['group', 'supergroup']:
            return

        if not update.message and not update.edited_message:
            return

        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        chat_id_str = str(chat_id)

        if chat_id_str not in locks:
            locks[chat_id_str] = asyncio.Lock()
        lock = locks[chat_id_str]

        async with lock:
            if chat_id_str not in message_counts:
                message_counts[chat_id_str] = 0

            message_counts[chat_id_str] += 1
            
            msg_content = "unknown"
            if update.message:
                if update.message.text:
                    if update.message.text.startswith('/'):
                        msg_content = f"command: {update.message.text.split()[0]}"
                    else:
                        msg_content = "text"
                elif update.message.photo:
                    msg_content = "photo"
                elif update.message.video:
                    msg_content = "video"
                elif update.message.document:
                    msg_content = "document"
                elif update.message.sticker:
                    msg_content = "sticker"
                elif update.message.animation:
                    msg_content = "animation"
                elif update.message.voice:
                    msg_content = "voice"
                elif update.message.audio:
                    msg_content = "audio"
                elif update.message.video_note:
                    msg_content = "video_note"
                else:
                    msg_content = "other_media"
            
            sender_type = "🤖bot" if update.effective_user.is_bot else "👤user"
            
            LOGGER.info(f"📊 Chat {chat_id} | Count: {message_counts[chat_id_str]}/{MESSAGE_FREQUENCY} | {sender_type} {user_id} | {msg_content}")

            if message_counts[chat_id_str] >= MESSAGE_FREQUENCY:
                if chat_id_str not in currently_spawning or not currently_spawning[chat_id_str]:
                    LOGGER.info(f"🎯 Triggering spawn in chat {chat_id} after {message_counts[chat_id_str]} messages")
                    currently_spawning[chat_id_str] = True
                    message_counts[chat_id_str] = 0
                    asyncio.create_task(send_image(update, context))
                else:
                    LOGGER.debug(f"⏭️ Spawn already in progress for chat {chat_id}, skipping")

    except Exception as e:
        LOGGER.error(f"Error in message_counter: {e}")
        LOGGER.error(traceback.format_exc())

async def send_image(update: Update, context: CallbackContext) -> None:
    """Spawn a character in the chat"""
    chat_id = update.effective_chat.id
    chat_id_str = str(chat_id)

    try:
        all_characters = list(await collection.find({}).to_list(length=None))

        if not all_characters:
            LOGGER.warning(f"No characters available for spawn in chat {chat_id}")
            currently_spawning[chat_id_str] = False
            return

        if chat_id not in sent_characters:
            sent_characters[chat_id] = []

        if len(sent_characters[chat_id]) >= len(all_characters):
            sent_characters[chat_id] = []

        available_characters = [
            c for c in all_characters
            if 'id' in c and c.get('id') not in sent_characters[chat_id]
        ]

        if not available_characters:
            available_characters = all_characters
            sent_characters[chat_id] = []

        allowed_characters = []
        for char in available_characters:
            if await is_character_allowed(char, chat_id):
                allowed_characters.append(char)

        if not allowed_characters:
            LOGGER.warning(f"No allowed characters for spawn in chat {chat_id}")
            currently_spawning[chat_id_str] = False
            return

        character = None
        selected_rarity = None

        try:
            group_setting = None
            if group_rarity_collection is not None and get_group_exclusive is not None:
                group_setting = await get_group_exclusive(chat_id)

            global_rarities = {}
            if spawn_settings_collection is not None and get_spawn_settings is not None:
                settings = await get_spawn_settings()
                global_rarities = settings.get('rarities', {}) if settings else {}

            rarity_pools = {}
            for char in allowed_characters:
                char_rarity = char.get('rarity', '🟢 Common')
                emoji = char_rarity.split(' ')[0] if isinstance(char_rarity, str) and ' ' in char_rarity else char_rarity

                if emoji not in rarity_pools:
                    rarity_pools[emoji] = []
                rarity_pools[emoji].append(char)

            weighted_choices = []

            if group_setting:
                exclusive_emoji = group_setting['rarity_emoji']
                exclusive_chance = group_setting.get('chance', 10.0)

                if exclusive_emoji in rarity_pools and rarity_pools[exclusive_emoji]:
                    weighted_choices.append({
                        'emoji': exclusive_emoji,
                        'chars': rarity_pools[exclusive_emoji],
                        'chance': exclusive_chance,
                        'is_exclusive': True
                    })

            for emoji, rarity_data in global_rarities.items():
                if not rarity_data.get('enabled', True):
                    continue

                if group_setting and emoji == group_setting['rarity_emoji']:
                    continue

                if emoji in rarity_pools and rarity_pools[emoji]:
                    weighted_choices.append({
                        'emoji': emoji,
                        'chars': rarity_pools[emoji],
                        'chance': rarity_data.get('chance', 5.0),
                        'is_exclusive': False
                    })

            if weighted_choices:
                total_chance = sum(choice['chance'] for choice in weighted_choices)
                rand = random.uniform(0, total_chance)

                cumulative = 0
                for choice in weighted_choices:
                    cumulative += choice['chance']
                    if rand <= cumulative:
                        character = random.choice(choice['chars'])
                        selected_rarity = choice['emoji']
                        break

        except Exception as e:
            LOGGER.error(f"Error in weighted selection: {e}\n{traceback.format_exc()}")

        if not character:
            character = random.choice(allowed_characters)

        sent_characters[chat_id].append(character['id'])
        last_characters[chat_id] = character

        if chat_id in first_correct_guesses:
            del first_correct_guesses[chat_id]

        # SPAWN MESSAGE CAPTION IN HTML MODE
        caption = """✨ ʟᴏᴏᴋ! ᴀ ᴡᴀɪꜰᴜ ʜᴀꜱ ᴀᴘᴘᴇᴀʀᴇᴅ ✨
✦ ᴍᴀᴋᴇ ʜᴇʀ ʏᴏᴜʀꜱ — ᴛʏᴘᴇ /ɢʀᴀʙ &lt;ᴡᴀɪꜰᴜ_ɴᴀᴍᴇ&gt;

⏳ ᴛɪᴍᴇ ʟɪᴍɪᴛ: 3 ᴍɪɴᴜᴛᴇꜱ!"""

        is_video = character.get('is_video', False)
        media_url = character.get('img_url')

        if is_video:
            spawn_msg = await context.bot.send_video(
                chat_id=chat_id,
                video=media_url,
                caption=caption,
                parse_mode='HTML',
                supports_streaming=True,
                read_timeout=300,
                write_timeout=300,
                connect_timeout=60,
                pool_timeout=60
            )
        else:
            spawn_msg = await context.bot.send_photo(
                chat_id=chat_id,
                photo=media_url,
                caption=caption,
                parse_mode='HTML',
                read_timeout=180,
                write_timeout=180
            )

        spawn_messages[chat_id] = spawn_msg.message_id

        chat_username = update.effective_chat.username
        if chat_username:
            spawn_message_links[chat_id] = f"https://t.me/{chat_username}/{spawn_msg.message_id}"
        else:
            chat_id_str_num = str(chat_id).replace('-100', '')
            spawn_message_links[chat_id] = f"https://t.me/c/{chat_id_str_num}/{spawn_msg.message_id}"

        currently_spawning[chat_id_str] = False

        asyncio.create_task(despawn_character(chat_id, spawn_msg.message_id, character, context))

    except Exception as e:
        LOGGER.error(f"Error in send_image: {e}")
        LOGGER.error(traceback.format_exc())
        currently_spawning[chat_id_str] = False

async def guess(update: Update, context: CallbackContext) -> None:
    """Handle /grab command to claim a character"""
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    try:
        if chat_id not in last_characters:
            await update.message.reply_html('<b>ɴᴏ ᴄʜᴀʀᴀᴄᴛᴇʀ ʜᴀs sᴘᴀᴡɴᴇᴅ ʏᴇᴛ!</b>')
            return

        if chat_id in first_correct_guesses:
            await update.message.reply_html(
                '<b>🚫 ᴡᴀɪғᴜ ᴀʟʀᴇᴀᴅʏ ɢʀᴀʙʙᴇᴅ ʙʏ sᴏᴍᴇᴏɴᴇ ᴇʟsᴇ ⚡. ʙᴇᴛᴛᴇʀ ʟᴜᴄᴋ ɴᴇxᴛ ᴛɪᴍᴇ..!!</b>'
            )
            return

        guess_text = ' '.join(context.args).lower() if context.args else ''

        if not guess_text:
            await update.message.reply_html('<b>ᴘʟᴇᴀsᴇ ᴘʀᴏᴠɪᴅᴇ ᴀ ɴᴀᴍᴇ!</b>')
            return

        if "()" in guess_text or "&" in guess_text:
            await update.message.reply_html(
                "<b>ɴᴀʜʜ ʏᴏᴜ ᴄᴀɴ'ᴛ ᴜsᴇ ᴛʜɪs ᴛʏᴘᴇs ᴏғ ᴡᴏʀᴅs...❌</b>"
            )
            return

        character_name = last_characters[chat_id].get('name', '').lower()
        name_parts = character_name.split()

        is_correct = (
            sorted(name_parts) == sorted(guess_text.split()) or
            any(part == guess_text for part in name_parts) or
            guess_text == character_name
        )

        if is_correct:
            first_correct_guesses[chat_id] = user_id

            LOGGER.info(f"✅ User {user_id} grabbed {character_name} in chat {chat_id}")

            if chat_id in spawn_messages:
                try:
                    await context.bot.delete_message(chat_id=chat_id, message_id=spawn_messages[chat_id])
                except BadRequest as e:
                    LOGGER.warning(f"Could not delete spawn message: {e}")
                spawn_messages.pop(chat_id, None)

            user = await user_collection.find_one({'id': user_id})
            if user:
                update_fields = {}
                if hasattr(update.effective_user, 'username') and update.effective_user.username:
                    if update.effective_user.username != user.get('username'):
                        update_fields['username'] = update.effective_user.username
                if update.effective_user.first_name != user.get('first_name'):
                    update_fields['first_name'] = update.effective_user.first_name

                if update_fields:
                    await user_collection.update_one({'id': user_id}, {'$set': update_fields})

                await user_collection.update_one(
                    {'id': user_id},
                    {'$push': {'characters': last_characters[chat_id]}}
                )
            else:
                await user_collection.insert_one({
                    'id': user_id,
                    'username': getattr(update.effective_user, 'username', None),
                    'first_name': update.effective_user.first_name,
                    'characters': [last_characters[chat_id]],
                })

            await update_grab_task(user_id)

            group_user_total = await group_user_totals_collection.find_one({
                'user_id': user_id,
                'group_id': chat_id
            })

            if group_user_total:
                update_fields = {}
                if hasattr(update.effective_user, 'username') and update.effective_user.username:
                    if update.effective_user.username != group_user_total.get('username'):
                        update_fields['username'] = update.effective_user.username
                if update.effective_user.first_name != group_user_total.get('first_name'):
                    update_fields['first_name'] = update.effective_user.first_name

                if update_fields:
                    await group_user_totals_collection.update_one(
                        {'user_id': user_id, 'group_id': chat_id},
                        {'$set': update_fields}
                    )

                await group_user_totals_collection.update_one(
                    {'user_id': user_id, 'group_id': chat_id},
                    {'$inc': {'count': 1}}
                )
            else:
                await group_user_totals_collection.insert_one({
                    'user_id': user_id,
                    'group_id': chat_id,
                    'username': getattr(update.effective_user, 'username', None),
                    'first_name': update.effective_user.first_name,
                    'count': 1,
                })

            group_info = await top_global_groups_collection.find_one({'group_id': chat_id})
            if group_info:
                update_fields = {}
                if update.effective_chat.title != group_info.get('group_name'):
                    update_fields['group_name'] = update.effective_chat.title

                if update_fields:
                    await top_global_groups_collection.update_one(
                        {'group_id': chat_id},
                        {'$set': update_fields}
                    )

                await top_global_groups_collection.update_one(
                    {'group_id': chat_id},
                    {'$inc': {'count': 1}}
                )
            else:
                await top_global_groups_collection.insert_one({
                    'group_id': chat_id,
                    'group_name': update.effective_chat.title,
                    'count': 1,
                })

            character = last_characters[chat_id]
            keyboard = [[
                InlineKeyboardButton(
                    "🪼 ʜᴀʀᴇᴍ",
                    switch_inline_query_current_chat=f"collection.{user_id}"
                )
            ]]

            # Get character details
            character_name = character.get('name', 'Unknown')
            anime = character.get('anime', 'Unknown')
            rarity = character.get('rarity', '🟢 Common')
            character_id = character.get('id', 'Unknown')
            owner_name = update.effective_user.first_name

            # Convert to small caps
            small_caps_character_name = to_small_caps(character_name)
            small_caps_anime = to_small_caps(anime)
            small_caps_rarity = to_small_caps(rarity)
            small_caps_owner_name = to_small_caps(owner_name)

            # SUCCESS MESSAGE WITH BOXED DESIGN
            success_message = f"""🎊 ᴄᴏɴɢʀᴀᴛᴜʟᴀᴛɪᴏɴs! ɴᴇᴡ ᴄʜᴀʀᴀᴄᴛᴇʀ ᴜɴʟᴏᴄᴋᴇᴅ 🎊
╭════════•┈┈┈┈•════════╮
┃ ✦ ɴᴀᴍᴇ: 𓂃ࣰࣲ {escape(small_caps_character_name)}
┃ ✦ ʀᴀʀɪᴛʏ: {escape(small_caps_rarity)}
┃ ✦ ᴀɴɪᴍᴇ: {escape(small_caps_anime)}
┃ ✦ ɪᴅ: 🆔 {escape(str(character_id))}
┃ ✦ ꜱᴛᴀᴛᴜꜱ: ᴀᴅᴅᴇᴅ ᴛᴏ ʜᴀʀᴇᴍ ✅
┃ ✦ ᴏᴡɴᴇʀ: ✧ {escape(small_caps_owner_name)}
╰════════•┈┈┈┈•════════╯

✧ ᴄʜᴀʀᴀᴄᴛᴇʀ ꜱᴜᴄᴄᴇꜱꜰᴜʟʟʏ ᴀᴅᴅᴇᴅ ɪɴ ʏᴏᴜʀ ʜᴀʀᴇᴍ ✅"""

            await update.message.reply_text(
                success_message,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

            spawn_message_links.pop(chat_id, None)

        else:
            keyboard = []
            if chat_id in spawn_message_links:
                keyboard.append([
                    InlineKeyboardButton(
                        "📍 ᴠɪᴇᴡ sᴘᴀᴡɴ ᴍᴇssᴀɢᴇ",
                        url=spawn_message_links[chat_id]
                    )
                ])

            reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
            await update.message.reply_html(
                '<b>ᴘʟᴇᴀsᴇ ᴡʀɪᴛᴇ ᴀ ᴄᴏʀʀᴇᴄᴛ ɴᴀᴍᴇ..❌</b>',
                reply_markup=reply_markup
            )

    except Exception as e:
        LOGGER.error(f"Error in guess: {e}")
        LOGGER.error(traceback.format_exc())

# ==================== MAIN FUNCTION ====================

async def main():
    """Main async entry point - single event loop for everything"""
    try:
        # 0. Run user data migration
        LOGGER.info("🔄 Checking user data migration...")
        migration_success = await migrate_user_data()
        if not migration_success:
            LOGGER.warning("⚠️ User data migration failed or skipped")
        
        # Optional: Verify migration
        if migration_success:
            verification_ok = await verify_migration()
            if verification_ok:
                LOGGER.info("✅ Migration verified successfully")
                # Optional: Uncomment to auto-cleanup
                # await cleanup_old_collection()
            else:
                LOGGER.warning("⚠️ Migration verification failed")
        
        # 1. Load rarity system
        try:
            from shivu.modules.rarity import (
                spawn_settings_collection as ssc,
                group_rarity_collection as grc,
                get_spawn_settings,
                get_group_exclusive
            )
            global spawn_settings_collection, group_rarity_collection, get_spawn_settings, get_group_exclusive
            spawn_settings_collection = ssc
            group_rarity_collection = grc
            LOGGER.info("✅ Rarity system loaded")
        except Exception as e:
            LOGGER.warning(f"⚠️ Rarity system not available: {e}")

        # 2. Setup backup system
        try:
            from shivu.modules.backup import setup_backup_handlers
            setup_backup_handlers(application)
            LOGGER.info("✅ Backup system initialized")
        except Exception as e:
            LOGGER.warning(f"⚠️ Backup system not available: {e}")

        # 3. Start Pyrogram client
        await shivuu.start()
        LOGGER.info("✅ Pyrogram client started")

        # 4. Setup PTB handlers
        application.add_handler(CommandHandler(["grab", "g"], guess, block=False))
        application.add_handler(MessageHandler(filters.ALL, message_counter, block=False))

        # 5. Initialize and start PTB application
        await application.initialize()
        await application.start()
        await application.updater.start_polling(drop_pending_updates=True)
        
        LOGGER.info("✅ ʏᴏɪᴄʜɪ ʀᴀɴᴅɪ ʙᴏᴛ sᴛᴀʀᴛᴇᴅ")

        # 6. Keep bot running
        while True:
            await asyncio.sleep(3600)

    except Exception as e:
        LOGGER.error(f"❌ Fatal Error: {e}")
        traceback.print_exc()
    finally:
        # Cleanup on exit
        LOGGER.info("Cleaning up...")
        try:
            await application.stop()
            await application.shutdown()
            await shivuu.stop()
        except Exception as e:
            LOGGER.error(f"Error during cleanup: {e}")

if __name__ == "__main__":
    # Create fresh event loop
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        LOGGER.info("Bot stopped.")
    except Exception as e:
        LOGGER.error(f"Unexpected error: {e}")
        traceback.print_exc()
