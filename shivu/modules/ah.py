import re
import asyncio
from pathlib import Path
import yt_dlp
from telegram import Update
from telegram.ext import MessageHandler, ContextTypes, filters
from shivu import application

RGX = re.compile(r'instagram\.com/(?:p|reel|reels)/([A-Za-z0-9_-]+)')

async def dl(url: str) -> str | None:
    return await asyncio.get_event_loop().run_in_executor(None, _dl, url)

def _dl(url: str) -> str | None:
    try:
        opts = {
            'format': 'best',
            'outtmpl': '/tmp/%(id)s.%(ext)s',
            'quiet': True,
            'no_warnings': True
        }
        with yt_dlp.YoutubeDL(opts) as ydl:
            i = ydl.extract_info(url, download=True)
            return ydl.prepare_filename(i)
    except:
        return None

async def ig(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not u.message or not u.message.text or not RGX.search(u.message.text):
        return

    m = await u.message.reply_text("<b>⏳ DOWNLOADING...</b>", parse_mode='HTML')
    v = await dl(u.message.text)

    if v and Path(v).exists():
        try:
            with open(v, 'rb') as f:
                await u.message.reply_video(f, caption="<b>✅ DOWNLOADED</b>", parse_mode='HTML', read_timeout=60, write_timeout=60)
            Path(v).unlink(missing_ok=True)
            await m.delete()
        except:
            Path(v).unlink(missing_ok=True)
            await m.edit_text("<b>❌ UPLOAD FAILED</b>", parse_mode='HTML')
    else:
        await m.edit_text("<b>❌ DOWNLOAD FAILED</b>", parse_mode='HTML')

application.add_handler(MessageHandler(filters.TEXT & filters.Regex(RGX), ig))