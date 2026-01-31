import asyncio
import hashlib
import html
import io
import logging
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List, Tuple
from functools import wraps
from contextlib import asynccontextmanager

import aiohttp
from aiohttp import ClientSession, TCPConnector
from pymongo import ReturnDocument, ASCENDING
from telegram import Update, InputFile, Message, PhotoSize, Document, InputMediaPhoto, InputMediaDocument
from telegram.ext import CommandHandler, ContextTypes
from telegram.error import TelegramError, NetworkError, TimedOut, BadRequest

from shivu import application, collection, db, CHARA_CHANNEL_ID, SUPPORT_CHAT
from shivu.config import Config


# ===================== LOGGING CONFIGURATION =====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# ===================== SETUP FUNCTION =====================
async def setup_database_indexes():
    """Create database indexes for optimal performance"""
    try:
        # Unique index on character ID
        await collection.create_index([("id", ASCENDING)], unique=True, background=True)

        # Regular index on file_hash for fast lookups
        await collection.create_index([("file_hash", ASCENDING)], background=True)

        # Index on rarity for filtering
        await collection.create_index([("rarity", ASCENDING)], background=True)

        # Index on uploader_id for user queries
        await collection.create_index([("uploader_id", ASCENDING)], background=True)

        logger.info("✅ Database indexes created successfully")
    except Exception as e:
        logger.error(f"⚠️ Failed to create indexes: {e}")


# ===================== ENUMS =====================

class MediaType(Enum):
    """Allowed media types"""
    PHOTO = "photo"
    DOCUMENT = "document"
    VIDEO = "video"
    ANIMATION = "animation"

    @classmethod
    def from_telegram_message(cls, message) -> Optional['MediaType']:
        """Detect media type from Telegram message"""
        if message.photo:
            return cls.PHOTO
        elif message.document:
            mime_type = message.document.mime_type or ''
            if mime_type.startswith('image/'):
                return cls.DOCUMENT
        elif message.video:
            return cls.VIDEO
        elif message.animation:
            return cls.ANIMATION
        return None


class RarityLevel(Enum):
    """Rarity levels (1-15) matching Code A"""
    COMMON = (1, "⚪ ᴄᴏᴍᴍᴏɴ")
    RARE = (2, "🔵 ʀᴀʀᴇ")
    LEGENDARY = (3, "🟡 ʟᴇɢᴇɴᴅᴀʀʏ")
    SPECIAL = (4, "💮 ꜱᴘᴇᴄɪᴀʟ")
    ANCIENT = (5, "👹 ᴀɴᴄɪᴇɴᴛ")
    CELESTIAL = (6, "🎐 ᴄᴇʟᴇꜱᴛɪᴀʟ")
    EPIC = (7, "🔮 ᴇᴘɪᴄ")
    COSMIC = (8, "🪐 ᴄᴏꜱᴍɪᴄ")
    NIGHTMARE = (9, "⚰️ ɴɪɢʜᴛᴍᴀʀᴇ")
    FROSTBORN = (10, "🌬️ ꜰʀᴏꜱᴛʙᴏʀɴ")
    VALENTINE = (11, "💝 ᴠᴀʟᴇɴᴛɪɴᴇ")
    SPRING = (12, "🌸 ꜱᴘʀɪɴɢ")
    TROPICAL = (13, "🏖️ ᴛʀᴏᴘɪᴄᴀʟ")
    KAWAII = (14, "🍭 ᴋᴀᴡᴀɪɪ")
    HYBRID = (15, "🧬 ʜʏʙʀɪᴅ")

    def __init__(self, level: int, display: str):
        self._level = level
        self._display = display

    @property
    def level(self) -> int:
        return self._level

    @property
    def display_name(self) -> str:
        return self._display

    @classmethod
    def from_number(cls, num: int) -> Optional['RarityLevel']:
        for rarity in cls:
            if rarity.level == num:
                return rarity
        return None

    @classmethod
    def get_all(cls) -> Dict[int, str]:
        """Get all rarity levels as dict (matching Code A format)"""
        return {rarity.level: rarity.display_name for rarity in cls}


# ===================== DATACLASSES =====================

@dataclass(frozen=True)
class BotConfig:
    """Bot configuration"""
    MAX_FILE_SIZE: int = 20 * 1024 * 1024
    DOWNLOAD_TIMEOUT: int = 300
    UPLOAD_TIMEOUT: int = 300
    CHUNK_SIZE: int = 65536
    MAX_RETRIES: int = 3
    RETRY_DELAY: float = 1.0
    CONNECTION_LIMIT: int = 100
    CATBOX_API: str = "https://catbox.moe/user/api.php"
    TELEGRAPH_API: str = "https://telegra.ph/upload"
    ALLOWED_MIME_TYPES: Tuple[str, ...] = (
        'image/jpeg', 'image/png', 'image/webp', 'image/jpg'
    )


@dataclass
class MediaFile:
    """Represents a media file with efficient memory handling"""
    file_path: Optional[str] = None
    media_type: Optional[MediaType] = None
    filename: str = field(default="")
    mime_type: Optional[str] = None
    size: int = 0
    hash: str = field(default="")
    catbox_url: Optional[str] = None
    telegram_file_id: Optional[str] = None

    def __post_init__(self):
        if self.file_path and not self.hash:
            object.__setattr__(self, 'hash', self._compute_hash())
        if self.file_path and not self.size:
            import os
            object.__setattr__(self, 'size', os.path.getsize(self.file_path))

    def _compute_hash(self) -> str:
        """Compute SHA256 hash of file efficiently with optimized chunk size"""
        sha256_hash = hashlib.sha256()
        if self.file_path:
            with open(self.file_path, "rb") as f:
                # OPTIMIZATION: Increased chunk size from 4096 to 65536
                for byte_block in iter(lambda: f.read(65536), b""):
                    sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    @property
    def is_valid_image(self) -> bool:
        """Check if media is a valid image"""
        if self.media_type in [MediaType.VIDEO, MediaType.ANIMATION]:
            return False
        if self.mime_type:
            return self.mime_type.startswith('image/')
        return self.media_type in [MediaType.PHOTO, MediaType.DOCUMENT]

    @property
    def is_valid_size(self) -> bool:
        """Check if file size is within limits"""
        return self.size <= BotConfig.MAX_FILE_SIZE

    def cleanup(self):
        """Clean up temporary file"""
        if self.file_path:
            try:
                import os
                os.unlink(self.file_path)
            except Exception as e:
                logger.warning(f"Failed to cleanup file {self.file_path}: {e}")


@dataclass
class Character:
    """Represents a character entry with integer rarity storage"""
    character_id: str
    name: str
    anime: str
    rarity: int  # Store as integer (1-15)
    media_file: MediaFile
    uploader_id: int
    uploader_name: str
    message_id: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage"""
        return {
            'id': self.character_id,
            'name': self.name,
            'anime': self.anime,
            'rarity': self.rarity,  # Store as integer
            'img_url': self.media_file.catbox_url,
            'message_id': self.message_id,
            'uploader_id': self.uploader_id,
            'uploader_name': self.uploader_name,
            'file_hash': self.media_file.hash,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }

    def get_caption(self, action: str = "Added") -> str:
        """Generate caption for channel post"""
        rarity_obj = RarityLevel.from_number(self.rarity)
        display_name = rarity_obj.display_name if rarity_obj else f"Level {self.rarity}"

        return (
            f"{self.character_id}: {self.name}\n"
            f"{self.anime}\n"
            f"{rarity_obj.display_name.split()[0]} 𝙍𝘼𝙍𝙄𝙏𝙔: {rarity_obj.display_name.split()[1]}\n\n"
            f"{action} ʙʏ {self.uploader_name}"
        )


# ===================== SESSION MANAGEMENT =====================

class SessionManager:
    """Manages aiohttp session with connection pooling"""
    _session: Optional[ClientSession] = None

    @classmethod
    async def get_session(cls) -> ClientSession:
        """Get or create aiohttp session"""
        if cls._session is None or cls._session.closed:
            connector = TCPConnector(limit=BotConfig.CONNECTION_LIMIT)
            timeout = aiohttp.ClientTimeout(total=BotConfig.UPLOAD_TIMEOUT)
            cls._session = ClientSession(connector=connector, timeout=timeout)
        return cls._session

    @classmethod
    async def close(cls):
        """Close the aiohttp session"""
        if cls._session and not cls._session.closed:
            await cls._session.close()
            cls._session = None


# ===================== UTILITIES =====================

class SequenceGenerator:
    """Thread-safe ID generator using MongoDB atomic operations"""

    @staticmethod
    async def get_next_id() -> str:
        """
        FIXED: Atomic ID generation using find_one_and_update with $inc
        This prevents race conditions in concurrent uploads
        """
        try:
            result = await db.sequences.find_one_and_update(
                {'_id': 'character_id'},
                {'$inc': {'value': 1}},
                upsert=True,
                return_document=ReturnDocument.AFTER
            )
            return str(result['value'])
        except Exception as e:
            logger.error(f"Failed to generate ID: {e}")
            # Fallback to timestamp-based ID if atomic operation fails
            import time
            return str(int(time.time() * 1000))


class CharacterFactory:
    """Factory for creating Character objects with HTML escaping"""

    @staticmethod
    def format_name(name: str) -> str:
        """Format name with title case and HTML escaping"""
        # SECURITY FIX: HTML escape before storing
        return html.escape(name.strip().title())

    @staticmethod
    async def create_from_upload(
        name: str,
        anime: str,
        rarity: int,
        media_file: MediaFile,
        uploader_id: int,
        uploader_name: str
    ) -> Character:
        """Create character with auto-generated ID and HTML-escaped fields"""
        char_id = await SequenceGenerator.get_next_id()
        
        # SECURITY FIX: Apply HTML escaping to user inputs
        safe_name = CharacterFactory.format_name(name)
        safe_anime = CharacterFactory.format_name(anime)
        safe_uploader_name = html.escape(uploader_name.strip())
        
        return Character(
            character_id=char_id,
            name=safe_name,
            anime=safe_anime,
            rarity=rarity,
            media_file=media_file,
            uploader_id=uploader_id,
            uploader_name=safe_uploader_name,
            created_at=datetime.utcnow().isoformat()
        )


# ===================== MEDIA UPLOADER WITH FALLBACK =====================

class MediaUploader:
    """
    ENHANCED: Media uploader with Catbox primary and Telegraph fallback
    Handles both Catbox.moe and graph.org (Telegraph) uploads
    """

    @staticmethod
    async def upload(file_path: str, filename: str) -> Optional[str]:
        """
        Upload to Catbox first, fallback to Telegraph if it fails
        Returns the URL of the uploaded media or None if both fail
        """
        # Try Catbox first
        catbox_url = await MediaUploader._upload_to_catbox(file_path, filename)
        if catbox_url:
            logger.info(f"Successfully uploaded to Catbox: {catbox_url}")
            return catbox_url
        
        # Fallback to Telegraph
        logger.warning("Catbox upload failed, trying Telegraph fallback...")
        telegraph_url = await MediaUploader._upload_to_telegraph(file_path)
        if telegraph_url:
            logger.info(f"Successfully uploaded to Telegraph: {telegraph_url}")
            return telegraph_url
        
        logger.error("Both Catbox and Telegraph uploads failed")
        return None

    @staticmethod
    async def _upload_to_catbox(file_path: str, filename: str) -> Optional[str]:
        """Upload to Catbox.moe"""
        session = await SessionManager.get_session()
        
        try:
            with open(file_path, 'rb') as f:
                form_data = aiohttp.FormData()
                form_data.add_field('reqtype', 'fileupload')
                form_data.add_field('fileToUpload', f, filename=filename)

                async with session.post(BotConfig.CATBOX_API, data=form_data) as response:
                    if response.status == 200:
                        url = await response.text()
                        return url.strip() if url else None
                    else:
                        logger.error(f"Catbox upload failed with status {response.status}")
                        return None
        except Exception as e:
            logger.error(f"Catbox upload exception: {e}")
            return None

    @staticmethod
    async def _upload_to_telegraph(file_path: str) -> Optional[str]:
        """
        FALLBACK: Upload to Telegraph (graph.org)
        Returns the direct image URL from Telegraph
        """
        session = await SessionManager.get_session()
        
        try:
            with open(file_path, 'rb') as f:
                form_data = aiohttp.FormData()
                form_data.add_field('file', f, filename='image.jpg')

                async with session.post(BotConfig.TELEGRAPH_API, data=form_data) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result and isinstance(result, list) and len(result) > 0:
                            # Telegraph returns [{"src": "/file/..."}]
                            path = result[0].get('src')
                            if path:
                                return f"https://telegra.ph{path}"
                    
                    logger.error(f"Telegraph upload failed with status {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Telegraph upload exception: {e}")
            return None


# Backward compatibility alias
class CatboxUploader:
    """Alias for backward compatibility - redirects to MediaUploader"""
    
    @staticmethod
    async def upload(file_path: str, filename: str) -> Optional[str]:
        return await MediaUploader.upload(file_path, filename)


# ===================== TELEGRAM UPLOADER =====================

class TelegramUploader:
    """Handles uploads to Telegram channel with retry logic"""

    @staticmethod
    async def upload_to_channel(character: Character, context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
        """Upload character to channel and return message ID"""
        for attempt in range(BotConfig.MAX_RETRIES):
            try:
                message = await context.bot.send_photo(
                    chat_id=CHARA_CHANNEL_ID,
                    photo=character.media_file.catbox_url,
                    caption=character.get_caption(),
                    parse_mode='HTML'
                )
                logger.info(f"Successfully uploaded character {character.character_id} to channel")
                return message.message_id

            except (NetworkError, TimedOut) as e:
                logger.warning(f"Network error on attempt {attempt + 1}/{BotConfig.MAX_RETRIES}: {e}")
                if attempt < BotConfig.MAX_RETRIES - 1:
                    await asyncio.sleep(BotConfig.RETRY_DELAY * (attempt + 1))
                continue

            except TelegramError as e:
                logger.error(f"Telegram error uploading to channel: {e}")
                return None

        logger.error(f"Failed to upload character {character.character_id} after {BotConfig.MAX_RETRIES} attempts")
        return None

    @staticmethod
    async def update_channel_message(
        character: Character,
        context: ContextTypes.DEFAULT_TYPE,
        old_message_id: Optional[int]
    ) -> Optional[int]:
        """
        DO NOT MODIFY: Update channel message with retry/delete logic
        This method is kept as-is per constraints
        """
        new_message_id = None

        for attempt in range(BotConfig.MAX_RETRIES):
            try:
                message = await context.bot.send_photo(
                    chat_id=CHARA_CHANNEL_ID,
                    photo=character.media_file.catbox_url,
                    caption=character.get_caption(action="Updated"),
                    parse_mode='HTML'
                )
                new_message_id = message.message_id
                break

            except (NetworkError, TimedOut) as e:
                logger.warning(f"Network error on attempt {attempt + 1}: {e}")
                if attempt < BotConfig.MAX_RETRIES - 1:
                    await asyncio.sleep(BotConfig.RETRY_DELAY * (attempt + 1))
                continue

            except TelegramError as e:
                logger.error(f"Failed to send new channel message: {e}")
                break

        if old_message_id and new_message_id:
            try:
                await context.bot.delete_message(chat_id=CHARA_CHANNEL_ID, message_id=old_message_id)
            except Exception as e:
                logger.warning(f"Could not delete old message {old_message_id}: {e}")

        return new_message_id


# ===================== MEDIA HANDLER =====================

class MediaHandler:
    """Handles media extraction and download"""

    @staticmethod
    async def extract_from_reply(message: Message) -> Optional[MediaFile]:
        """Extract media from reply message with robust validation"""
        media_type = MediaType.from_telegram_message(message)
        if not media_type:
            return None

        try:
            if media_type == MediaType.PHOTO:
                photo: PhotoSize = message.photo[-1]
                file = await photo.get_file()
                filename = f"photo_{photo.file_unique_id}.jpg"
                mime_type = "image/jpeg"
                telegram_file_id = photo.file_id

            elif media_type == MediaType.DOCUMENT:
                doc: Document = message.document
                if not doc.mime_type or not doc.mime_type.startswith('image/'):
                    return None
                file = await doc.get_file()
                filename = doc.file_name or f"document_{doc.file_unique_id}"
                mime_type = doc.mime_type
                telegram_file_id = doc.file_id

            else:
                return None

            # Download to temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{filename}") as tmp:
                await file.download_to_drive(tmp.name)
                file_path = tmp.name

            media_file = MediaFile(
                file_path=file_path,
                media_type=media_type,
                filename=filename,
                mime_type=mime_type,
                telegram_file_id=telegram_file_id
            )

            if not media_file.is_valid_size:
                media_file.cleanup()
                return None

            return media_file

        except Exception as e:
            logger.error(f"Failed to extract media: {e}")
            return None


# ===================== UPLOAD HANDLER =====================

class UploadHandler:
    """Handles /upload command with improved input parsing"""

    @staticmethod
    def parse_upload_text(text: str) -> Optional[Tuple[str, str, int]]:
        """
        IMPROVED: Robust parsing of 3-line format with whitespace handling
        Returns (name, anime, rarity) or None
        """
        try:
            # Split by newlines and filter out empty lines
            lines = [line.strip() for line in text.strip().split('\n') if line.strip()]
            
            if len(lines) != 3:
                return None

            name = lines[0].strip()
            anime = lines[1].strip()
            
            # Validate name and anime are not empty
            if not name or not anime:
                return None

            try:
                rarity = int(lines[2].strip())
                if not (1 <= rarity <= 15):
                    return None
            except ValueError:
                return None

            return (name, anime, rarity)

        except Exception as e:
            logger.error(f"Failed to parse upload text: {e}")
            return None

    @staticmethod
    async def handle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /upload command with validation"""
        if update.effective_user.id not in Config.SUDO_USERS:
            await update.message.reply_text('🔒 ᴀꜱᴋ ᴍʏ ᴏᴡɴᴇʀ...')
            return

        if not update.message.reply_to_message or not (
            update.message.reply_to_message.photo or update.message.reply_to_message.document
        ):
            help_text = (
                "📝 **ʜᴏᴡ ᴛᴏ ᴜᴘʟᴏᴀᴅ:**\n\n"
                "1️⃣ ʀᴇᴘʟʏ ᴛᴏ ᴀ ᴘʜᴏᴛᴏ ᴡɪᴛʜ:\n"
                "   `/upload ᴄʜᴀʀᴀᴄᴛᴇʀ ɴᴀᴍᴇ`\n"
                "   `ᴀɴɪᴍᴇ ɴᴀᴍᴇ`\n"
                "   `ʀᴀʀɪᴛʏ (1-15)`\n\n"
                "**ᴇxᴀᴍᴘʟᴇ:**\n"
                "`/upload Naruto Uzumaki`\n"
                "`Naruto`\n"
                "`5`"
            )
            await update.message.reply_text(help_text, parse_mode='Markdown')
            return

        # Parse upload data
        upload_text = update.message.text.replace('/upload', '', 1).strip()
        parsed = UploadHandler.parse_upload_text(upload_text)

        if not parsed:
            await update.message.reply_text(
                "❌ **ɪɴᴠᴀʟɪᴅ ꜰᴏʀᴍᴀᴛ!**\n\n"
                "ᴘʟᴇᴀꜱᴇ ᴜꜱᴇ:\n"
                "`ᴄʜᴀʀᴀᴄᴛᴇʀ ɴᴀᴍᴇ`\n"
                "`ᴀɴɪᴍᴇ ɴᴀᴍᴇ`\n"
                "`ʀᴀʀɪᴛʏ (1-15)`",
                parse_mode='Markdown'
            )
            return

        name, anime, rarity = parsed

        # Validate rarity
        if not RarityLevel.from_number(rarity):
            await update.message.reply_text(
                f"❌ ɪɴᴠᴀʟɪᴅ ʀᴀʀɪᴛʏ! ᴜꜱᴇ 1-15.\n\n"
                f"**ᴀᴠᴀɪʟᴀʙʟᴇ ʀᴀʀɪᴛɪᴇꜱ:**\n" +
                "\n".join([f"{k}: {v}" for k, v in RarityLevel.get_all().items()])
            )
            return

        processing_msg = await update.message.reply_text("🔄 **ᴘʀᴏᴄᴇꜱꜱɪɴɢ...**")

        try:
            # Extract media
            media_file = await MediaHandler.extract_from_reply(update.message.reply_to_message)

            if not media_file or not media_file.is_valid_image:
                await processing_msg.edit_text("❌ ɪɴᴠᴀʟɪᴅ ᴍᴇᴅɪᴀ! ᴏɴʟʏ ᴘʜᴏᴛᴏꜱ ᴀɴᴅ ɪᴍᴀɢᴇ ᴅᴏᴄᴜᴍᴇɴᴛꜱ ᴀʟʟᴏᴡᴇᴅ.")
                return

            # DO NOT MODIFY: Keep existing hash check logic as-is
            existing = await collection.find_one({'file_hash': media_file.hash})
            if existing:
                await processing_msg.edit_text(
                    f"⚠️ **ᴅᴜᴘʟɪᴄᴀᴛᴇ!**\n\n"
                    f"ᴛʜɪꜱ ɪᴍᴀɢᴇ ᴀʟʀᴇᴀᴅʏ ᴇxɪꜱᴛꜱ:\n"
                    f"**ɪᴅ:** `{existing['id']}`\n"
                    f"**ɴᴀᴍᴇ:** {existing['name']}\n"
                    f"**ᴀɴɪᴍᴇ:** {existing['anime']}"
                )
                media_file.cleanup()
                return

            # Create character with HTML-escaped inputs
            character = await CharacterFactory.create_from_upload(
                name=name,
                anime=anime,
                rarity=rarity,
                media_file=media_file,
                uploader_id=update.effective_user.id,
                uploader_name=update.effective_user.first_name
            )

            await processing_msg.edit_text("☁️ **ᴜᴘʟᴏᴀᴅɪɴɢ ᴛᴏ ᴄʟᴏᴜᴅ...**")

            # Upload to cloud (Catbox/Telegraph) and channel in parallel
            catbox_url, message_id = await asyncio.gather(
                MediaUploader.upload(media_file.file_path, media_file.filename),
                TelegramUploader.upload_to_channel(character, context)
            )

            if not catbox_url:
                await processing_msg.edit_text("❌ ꜰᴀɪʟᴇᴅ ᴛᴏ ᴜᴘʟᴏᴀᴅ ᴛᴏ ᴄʟᴏᴜᴅ ꜱᴛᴏʀᴀɢᴇ.")
                media_file.cleanup()
                return

            # Update character with URLs and message ID
            character.media_file.catbox_url = catbox_url
            character.message_id = message_id

            # Save to database
            await collection.insert_one(character.to_dict())

            # Cleanup
            media_file.cleanup()

            # Success message
            success_text = (
                f"✅ **ᴄʜᴀʀᴀᴄᴛᴇʀ ᴀᴅᴅᴇᴅ!**\n\n"
                f"**ɪᴅ:** `{character.character_id}`\n"
                f"**ɴᴀᴍᴇ:** {character.name}\n"
                f"**ᴀɴɪᴍᴇ:** {character.anime}\n"
                f"**ʀᴀʀɪᴛʏ:** {RarityLevel.from_number(rarity).display_name}"
            )
            await processing_msg.edit_text(success_text)

            logger.info(f"Character {character.character_id} uploaded successfully by {update.effective_user.id}")

        except Exception as e:
            logger.error(f"Upload failed: {e}", exc_info=True)
            await processing_msg.edit_text(f"❌ **ᴇʀʀᴏʀ:** {str(e)}")
            if 'media_file' in locals():
                media_file.cleanup()


# ===================== DELETE HANDLER =====================

class DeleteHandler:
    """Handles /delete command"""

    @staticmethod
    async def handle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /delete command"""
        if update.effective_user.id not in Config.SUDO_USERS:
            await update.message.reply_text('🔒 ᴀꜱᴋ ᴍʏ ᴏᴡɴᴇʀ...')
            return

        if not context.args:
            await update.message.reply_text(
                "📝 **ᴜꜱᴀɢᴇ:**\n`/delete character_id`\n\n"
                "**ᴇxᴀᴍᴘʟᴇ:**\n`/delete 42`"
            )
            return

        char_id = context.args[0]
        character = await collection.find_one({'id': char_id})

        if not character:
            await update.message.reply_text('❌ ᴄʜᴀʀᴀᴄᴛᴇʀ ɴᴏᴛ ꜰᴏᴜɴᴅ.')
            return

        # Delete from database
        await collection.delete_one({'id': char_id})

        # Try to delete from channel
        try:
            if 'message_id' in character and character['message_id']:
                await context.bot.delete_message(
                    chat_id=CHARA_CHANNEL_ID,
                    message_id=character['message_id']
                )
            await update.message.reply_text('✅ ᴄʜᴀʀᴀᴄᴛᴇʀ ᴅᴇʟᴇᴛᴇᴅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ!')

        except BadRequest as e:
            error_msg = str(e).lower()
            if "message to delete not found" in error_msg:
                await update.message.reply_text('✅ ᴄʜᴀʀᴀᴄᴛᴇʀ ᴅᴇʟᴇᴛᴇᴅ ꜰʀᴏᴍ ᴅᴀᴛᴀʙᴀꜱᴇ (ᴄʜᴀɴɴᴇʟ ᴍᴇꜱꜱᴀɢᴇ ᴡᴀꜱ ᴀʟʀᴇᴀᴅʏ ɢᴏɴᴇ).')
            else:
                await update.message.reply_text(
                    f'✅ ᴄʜᴀʀᴀᴄᴛᴇʀ ᴅᴇʟᴇᴛᴇᴅ ꜰʀᴏᴍ ᴅᴀᴛᴀʙᴀꜱᴇ.\n\n⚠️ ᴄᴏᴜʟᴅ ɴᴏᴛ ᴅᴇʟᴇᴛᴇ ꜰʀᴏᴍ ᴄʜᴀɴɴᴇʟ: {str(e)}'
                )
        except Exception as e:
            await update.message.reply_text(
                f'✅ ᴄʜᴀʀᴀᴄᴛᴇʀ ᴅᴇʟᴇᴛᴇᴅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ ꜰʀᴏᴍ ᴅᴀᴛᴀʙᴀꜱᴇ.'
            )


# ===================== UPDATE HANDLER =====================

class UpdateHandler:
    """Handles /update command with multi-word argument support"""

    VALID_FIELDS = ['img_url', 'name', 'anime', 'rarity']

    @staticmethod
    def format_update_help() -> str:
        """Format update command help message"""
        return (
            "📝 ᴜᴘᴅᴀᴛᴇ ᴄᴏᴍᴍᴀɴᴅ ᴜꜱᴀɢᴇ:\n\n"
            "ᴜᴘᴅᴀᴛᴇ ᴡɪᴛʜ ᴠᴀʟᴜᴇ:\n"
            "/update ɪᴅ ꜰɪᴇʟᴅ ɴᴇᴡᴠᴀʟᴜᴇ\n\n"
            "ᴜᴘᴅᴀᴛᴇ ɪᴍᴀɢᴇ (ʀᴇᴘʟʏ ᴛᴏ ᴘʜᴏᴛᴏ):\n"
            "/update ɪᴅ ɪᴍɢ_ᴜʀʟ\n\n"
            "ᴠᴀʟɪᴅ ꜰɪᴇʟᴅꜱ:\n"
            "ɪᴍɢ_ᴜʀʟ, ɴᴀᴍᴇ, ᴀɴɪᴍᴇ, ʀᴀʀɪᴛʏ\n\n"
            "ᴇxᴀᴍᴘʟᴇꜱ:\n"
            "/update 12 ɴᴀᴍᴇ ɴᴇᴢᴜᴋᴏ ᴋᴀᴍᴀᴅᴏ\n"
            "/update 12 ᴀɴɪᴍᴇ ᴅᴇᴍᴏɴ ꜱʟᴀʏᴇʀ\n"
            "/update 12 ʀᴀʀɪᴛʏ 5\n"
            "/update 12 ɪᴍɢ_ᴜʀʟ ʀᴇᴘʟʏ_ɪᴍɢ"
        )

    @staticmethod
    async def handle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /update command with multi-word argument support"""
        if update.effective_user.id not in Config.SUDO_USERS:
            await update.message.reply_text('🔒 ᴀꜱᴋ ᴍʏ ᴏᴡɴᴇʀ...')
            return

        if not context.args or len(context.args) < 2:
            await update.message.reply_text(UpdateHandler.format_update_help())
            return

        char_id = context.args[0]
        field = context.args[1]

        if field not in UpdateHandler.VALID_FIELDS:
            await update.message.reply_text(
                f'❌ ɪɴᴠᴀʟɪᴅ ꜰɪᴇʟᴅ. ᴠᴀʟɪᴅ ꜰɪᴇʟᴅꜱ: {", ".join(UpdateHandler.VALID_FIELDS)}'
            )
            return

        character = await collection.find_one({'id': char_id})
        if not character:
            await update.message.reply_text('❌ ᴄʜᴀʀᴀᴄᴛᴇʀ ɴᴏᴛ ꜰᴏᴜɴᴅ.')
            return

        update_data = {}

        if field == 'img_url':
            if len(context.args) == 2:
                if not (update.message.reply_to_message and 
                       (update.message.reply_to_message.photo or 
                        update.message.reply_to_message.document)):
                    await update.message.reply_text(
                        '📸 ʀᴇᴘʟʏ ᴛᴏ ᴀ ᴘʜᴏᴛᴏ ʀᴇǫᴜɪʀᴇᴅ!\n\nʀᴇᴘʟʏ ᴛᴏ ᴀ ᴘʜᴏᴛᴏ ᴀɴᴅ ᴜꜱᴇ: /update id img_url'
                    )
                    return

                processing_msg = await update.message.reply_text("🔄 **Processing new image...**")

                try:
                    media_file = await MediaHandler.extract_from_reply(update.message.reply_to_message)

                    if not media_file or not media_file.is_valid_image:
                        await processing_msg.edit_text("❌ Invalid media! Only photos and image documents are allowed.")
                        return

                    # Create character for parallel upload with HTML-escaped data
                    char_for_upload = Character(
                        character_id=character['id'],
                        name=html.escape(character['name']),
                        anime=html.escape(character['anime']),
                        rarity=character['rarity'],  # Already integer
                        media_file=media_file,
                        uploader_id=update.effective_user.id,
                        uploader_name=html.escape(update.effective_user.first_name)
                    )

                    await processing_msg.edit_text("🔄 **Uploading new image and updating channel...**")

                    # Run both operations concurrently
                    catbox_url, new_message_id = await asyncio.gather(
                        MediaUploader.upload(media_file.file_path, media_file.filename),
                        TelegramUploader.update_channel_message(
                            char_for_upload, 
                            context, 
                            character.get('message_id')
                        )
                    )

                    if not catbox_url:
                        await processing_msg.edit_text("❌ Failed to upload to cloud storage.")
                        media_file.cleanup()
                        return

                    update_data['img_url'] = catbox_url
                    update_data['file_hash'] = media_file.hash
                    update_data['message_id'] = new_message_id

                    media_file.cleanup()
                    await processing_msg.edit_text('✅ ɪᴍᴀɢᴇ ᴜᴘᴅᴀᴛᴇᴅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ!')

                except Exception as e:
                    logger.error(f"Failed to update image: {e}", exc_info=True)
                    await update.message.reply_text(f'❌ Failed to update image: {str(e)}')
                    return

            else:
                # Validate context.args length before accessing
                if len(context.args) < 3:
                    await update.message.reply_text('❌ Missing image URL. Usage: /update id img_url URL')
                    return

                new_value = context.args[2]
                update_data['img_url'] = new_value

        elif field in ['name', 'anime']:
            # FIXED: Join all remaining arguments for multi-word support
            if len(context.args) < 3:
                await update.message.reply_text(
                    f'❌ Missing value. Usage: /update id {field} new_value'
                )
                return

            # Join all arguments from index 2 onwards for multi-word support
            new_value = " ".join(context.args[2:])
            # Apply HTML escaping and title case formatting
            update_data[field] = CharacterFactory.format_name(new_value)

        elif field == 'rarity':
            # Validate context.args length
            if len(context.args) < 3:
                await update.message.reply_text(
                    f'❌ Missing rarity value. Usage: /update id rarity 1-15'
                )
                return

            new_value = context.args[2]
            try:
                rarity_num = int(new_value)
                rarity = RarityLevel.from_number(rarity_num)
                if not rarity:
                    await update.message.reply_text(
                        f'❌ Invalid rarity. Please use a number between 1 and 15.'
                    )
                    return
                update_data['rarity'] = rarity_num  # Store as integer
            except ValueError:
                await update.message.reply_text(f'❌ Rarity must be a number (1-15).')
                return

        # Update timestamp
        update_data['updated_at'] = datetime.utcnow().isoformat()

        # Update in database
        updated_character = await collection.find_one_and_update(
            {'id': char_id},
            {'$set': update_data},
            return_document=ReturnDocument.AFTER
        )

        if not updated_character:
            await update.message.reply_text('❌ Failed to update character in database.')
            return

        # Update channel message (if not img_url which was already handled)
        if field != 'img_url' and 'message_id' in updated_character:
            try:
                # Create character object for channel update with HTML-escaped data
                channel_char = Character(
                    character_id=updated_character['id'],
                    name=html.escape(updated_character['name']),
                    anime=html.escape(updated_character['anime']),
                    rarity=updated_character['rarity'],
                    media_file=MediaFile(catbox_url=updated_character['img_url']),
                    uploader_id=update.effective_user.id,
                    uploader_name=html.escape(update.effective_user.first_name)
                )

                await TelegramUploader.update_channel_message(
                    channel_char,
                    context,
                    updated_character['message_id']
                )
            except Exception as e:
                logger.warning(f"Failed to update channel message: {e}")
                pass  # Channel update is optional

        await update.message.reply_text('✅ ᴄʜᴀʀᴀᴄᴛᴇʀ ᴜᴘᴅᴀᴛᴇᴅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ!')


# ===================== APPLICATION SETUP =====================

async def post_init(application):
    """Initialize database indexes after application starts"""
    await setup_database_indexes()


# Register command handlers with non-blocking option
application.add_handler(CommandHandler("upload", UploadHandler.handle, block=False))
application.add_handler(CommandHandler("delete", DeleteHandler.handle, block=False))
application.add_handler(CommandHandler("update", UpdateHandler.handle, block=False))

# Set up post_init to run setup_database_indexes
application.post_init = post_init


# ===================== CLEANUP =====================

async def cleanup():
    """Cleanup on shutdown"""
    await SessionManager.close()
    logger.info("Bot shutdown complete")
