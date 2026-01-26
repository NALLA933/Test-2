import asyncio
import random
import traceback
import importlib
from html import escape
from contextlib import asynccontextmanager
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import CommandHandler, MessageHandler, filters, ContextTypes
from telegram.error import BadRequest

from shivu import db, shivuu, application, LOGGER
from shivu.modules import ALL_MODULES

col = db['anime_characters_lol']
u_col = db['user_collection_lmaoooo']
ut_col = db['user_totals_lmaoooo']
gut_col = db['group_user_totalsssssss']
tg_col = db['top_global_groups']

MSG_FREQ = 40
DESPAWN_T = 180
AMV_GRP = -1003100468240

st = {
    'msg_cnt': {},
    'sent_ch': {},
    'last_ch': {},
    'first_g': {},
    'sp_msg': {},
    'sp_link': {},
    'sp_now': {},
    'locks': {}
}

sp_set_col = None
gr_col = None
get_sp_set = None
get_gr_ex = None

@asynccontextmanager
async def get_lock(cid: str):
    if cid not in st['locks']:
        st['locks'][cid] = asyncio.Lock()
    async with st['locks'][cid]:
        yield

async def is_char_ok(ch: dict, cid: int = None) -> bool:
    try:
        if ch.get('removed', False):
            return False

        r = ch.get('rarity', '🟢 Common')
        r_e = r.split()[0] if ' ' in str(r) else r
        vid = ch.get('is_video', False)

        if vid and r_e == '🎥':
            return cid == AMV_GRP

        if gr_col and cid:
            ex = await gr_col.find_one({'chat_id': cid, 'rarity_emoji': r_e})
            if ex:
                return True

            o_ex = await gr_col.find_one({'rarity_emoji': r_e, 'chat_id': {'$ne': cid}})
            if o_ex:
                return False

        if sp_set_col and get_sp_set:
            sets = await get_sp_set()
            rars = sets.get('rarities', {}) if sets else {}
            if r_e in rars and not rars[r_e].get('enabled', True):
                return False

        return True
    except Exception as e:
        LOGGER.error(f"Filter err: {e}")
        return True

async def upd_user(uid: int, uname: str, fname: str, ch: dict) -> None:
    u = await u_col.find_one({'id': uid})
    
    upd_f = {
        'username': uname,
        'first_name': fname
    }
    
    if u:
        await u_col.update_one(
            {'id': uid},
            {
                '$set': {k: v for k, v in upd_f.items() if v != u.get(k)},
                '$push': {'characters': ch}
            }
        )
        if 'pass_data' in u:
            await u_col.update_one(
                {'id': uid},
                {'$inc': {'pass_data.tasks.grabs': 1}}
            )
    else:
        await u_col.insert_one({
            'id': uid,
            **upd_f,
            'characters': [ch]
        })

async def upd_grp_stats(uid: int, cid: int, uname: str, fname: str, gname: str) -> None:
    await gut_col.update_one(
        {'user_id': uid, 'group_id': cid},
        {
            '$set': {'username': uname, 'first_name': fname},
            '$inc': {'count': 1}
        },
        upsert=True
    )
    
    await tg_col.update_one(
        {'group_id': cid},
        {
            '$set': {'group_name': gname},
            '$inc': {'count': 1}
        },
        upsert=True
    )

async def despawn_ch(cid: int, mid: int, ch: dict, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        await asyncio.sleep(DESPAWN_T)

        if cid in st['first_g']:
            for k in ['last_ch', 'sp_msg', 'sp_link', 'sp_now']:
                st[k].pop(cid, None)
            return

        try:
            await ctx.bot.delete_message(chat_id=cid, message_id=mid)
        except BadRequest:
            pass

        r = ch.get('rarity', '🟢 Common')
        r_e = r.split()[0] if ' ' in str(r) else '🟢'

        cap = f"""<b><u>⏰ TIME'S UP! YOU ALL MISSED THIS WAIFU!</u>

{r_e} NAME: <b>{escape(ch.get('name', 'Unknown'))}</b>
⚡ ANIME: <b>{escape(ch.get('anime', 'Unknown'))}</b>
🎯 RARITY: <b>{escape(r)}</b>

💔 BETTER LUCK NEXT TIME!</b>"""

        send_m = ctx.bot.send_video if ch.get('is_video') else ctx.bot.send_photo
        mk = 'video' if ch.get('is_video') else 'photo'
        
        miss_msg = await send_m(
            chat_id=cid,
            **{mk: ch.get('img_url')},
            caption=cap,
            parse_mode='HTML'
        )

        await asyncio.sleep(10)
        try:
            await ctx.bot.delete_message(chat_id=cid, message_id=miss_msg.message_id)
        except BadRequest:
            pass

        for k in ['last_ch', 'sp_msg', 'sp_link', 'sp_now']:
            st[k].pop(cid, None)

    except Exception as e:
        LOGGER.error(f"Despawn err: {e}\n{traceback.format_exc()}")

async def msg_counter(upd: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if upd.effective_chat.type not in ['group', 'supergroup']:
        return

    cid = str(upd.effective_chat.id)
    
    async with get_lock(cid):
        st['msg_cnt'][cid] = st['msg_cnt'].get(cid, 0) + 1

        if st['msg_cnt'][cid] >= MSG_FREQ:
            if not st['sp_now'].get(cid, False):
                st['sp_now'][cid] = True
                st['msg_cnt'][cid] = 0
                asyncio.create_task(send_img(upd, ctx))

async def sel_char(allowed: list, cid: int) -> dict:
    try:
        g_set = None
        if gr_col and get_gr_ex:
            g_set = await get_gr_ex(cid)

        gl_rars = {}
        if sp_set_col and get_sp_set:
            sets = await get_sp_set()
            gl_rars = sets.get('rarities', {}) if sets else {}

        r_pools = {}
        for c in allowed:
            e = c.get('rarity', '🟢 Common').split()[0] if ' ' in str(c.get('rarity', '')) else '🟢'
            r_pools.setdefault(e, []).append(c)

        w_ch = []

        if g_set and g_set['rarity_emoji'] in r_pools:
            w_ch.append({
                'chars': r_pools[g_set['rarity_emoji']],
                'chance': g_set.get('chance', 10.0)
            })

        for e, r_d in gl_rars.items():
            if not r_d.get('enabled', True):
                continue
            if g_set and e == g_set['rarity_emoji']:
                continue
            if e in r_pools:
                w_ch.append({
                    'chars': r_pools[e],
                    'chance': r_d.get('chance', 5.0)
                })

        if w_ch:
            tot = sum(c['chance'] for c in w_ch)
            rnd = random.uniform(0, tot)
            cum = 0
            
            for ch in w_ch:
                cum += ch['chance']
                if rnd <= cum:
                    return random.choice(ch['chars'])

    except Exception as e:
        LOGGER.error(f"Char sel err: {e}")

    return random.choice(allowed)

async def send_img(upd: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    cid = upd.effective_chat.id

    try:
        all_ch = await col.find({}).to_list(length=None)
        
        if not all_ch:
            st['sp_now'][str(cid)] = False
            return

        if cid not in st['sent_ch']:
            st['sent_ch'][cid] = []

        if len(st['sent_ch'][cid]) >= len(all_ch):
            st['sent_ch'][cid] = []

        avail = [c for c in all_ch if c.get('id') not in st['sent_ch'][cid]]
        
        if not avail:
            avail = all_ch
            st['sent_ch'][cid] = []

        allowed = [c for c in avail if await is_char_ok(c, cid)]
        
        if not allowed:
            st['sp_now'][str(cid)] = False
            return

        ch = await sel_char(allowed, cid)

        st['sent_ch'][cid].append(ch['id'])
        st['last_ch'][cid] = ch
        st['first_g'].pop(cid, None)

        cap = """<b><u>✨ LOOK! A WAIFU HAS APPEARED ✨</u>
✦ MAKE HER YOURS — TYPE /grab &lt;waifu_name&gt;

⏳ TIME LIMIT: 3 MINUTES!</b>"""

        send_m = ctx.bot.send_video if ch.get('is_video') else ctx.bot.send_photo
        mk = 'video' if ch.get('is_video') else 'photo'
        
        sp_msg = await send_m(
            chat_id=cid,
            **{mk: ch.get('img_url')},
            caption=cap,
            parse_mode='HTML'
        )

        st['sp_msg'][cid] = sp_msg.message_id

        uname = upd.effective_chat.username
        if uname:
            st['sp_link'][cid] = f"https://t.me/{uname}/{sp_msg.message_id}"
        else:
            cid_str = str(cid).replace('-100', '')
            st['sp_link'][cid] = f"https://t.me/c/{cid_str}/{sp_msg.message_id}"

        st['sp_now'][str(cid)] = False

        asyncio.create_task(despawn_ch(cid, sp_msg.message_id, ch, ctx))

    except Exception as e:
        LOGGER.error(f"Spawn err: {e}\n{traceback.format_exc()}")
        st['sp_now'][str(cid)] = False

async def guess(upd: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    cid = upd.effective_chat.id
    uid = upd.effective_user.id

    try:
        if cid not in st['last_ch']:
            await upd.message.reply_html('<b>NO CHARACTER HAS SPAWNED YET!</b>')
            return

        if cid in st['first_g']:
            await upd.message.reply_html('<b>🚫 WAIFU ALREADY GRABBED BY SOMEONE ELSE ⚡</b>')
            return

        g_txt = ' '.join(ctx.args).lower() if ctx.args else ''

        if not g_txt:
            await upd.message.reply_html('<b>PLEASE PROVIDE A NAME!</b>')
            return

        if "()" in g_txt or "&" in g_txt:
            await upd.message.reply_html("<b>NAHH YOU CAN'T USE THIS TYPES OF WORDS...❌</b>")
            return

        ch_name = st['last_ch'][cid].get('name', '').lower()
        n_parts = ch_name.split()

        is_ok = (
            sorted(n_parts) == sorted(g_txt.split()) or
            any(p == g_txt for p in n_parts) or
            g_txt == ch_name
        )

        if is_ok:
            st['first_g'][cid] = uid

            if cid in st['sp_msg']:
                try:
                    await ctx.bot.delete_message(chat_id=cid, message_id=st['sp_msg'][cid])
                except BadRequest:
                    pass
                st['sp_msg'].pop(cid, None)

            ch = st['last_ch'][cid]
            
            await upd_user(
                uid,
                getattr(upd.effective_user, 'username', None),
                upd.effective_user.first_name,
                ch
            )

            await upd_grp_stats(
                uid,
                cid,
                getattr(upd.effective_user, 'username', None),
                upd.effective_user.first_name,
                upd.effective_chat.title
            )

            c_name = ch.get('name', 'Unknown')
            anime = ch.get('anime', 'Unknown')
            rarity = ch.get('rarity', '🟢 Common')
            o_name = upd.effective_user.first_name

            msg = f"""<b><u>🎊 CONGRATULATIONS! NEW CHARACTER UNLOCKED 🎊</u>
╭════════•┈┈┈┈•════════╮
┃ ✦ NAME: 𓂃ࣰࣲ <b>{escape(c_name)}</b>
┃ ✦ RARITY: <b>{escape(rarity)}</b>
┃ ✦ ANIME: <b>{escape(anime)}</b>
┃ ✦ ID: 🆔 <b>{escape(str(ch.get('id', 'Unknown')))}</b>
┃ ✦ STATUS: <b>ADDED TO HAREM ✅</b>
┃ ✦ OWNER: ✧ <b>{escape(o_name)}</b>
╰════════•┈┈┈┈•════════╯

✧ CHARACTER SUCCESSFULLY ADDED IN YOUR HAREM ✅</b>"""

            kb = [[InlineKeyboardButton("🪼 HAREM", switch_inline_query_current_chat=f"collection.{uid}")]]

            await upd.message.reply_text(
                msg,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(kb)
            )

            st['sp_link'].pop(cid, None)

        else:
            kb = []
            if cid in st['sp_link']:
                kb.append([InlineKeyboardButton("📍 VIEW SPAWN MESSAGE", url=st['sp_link'][cid])])

            await upd.message.reply_html(
                '<b>PLEASE WRITE A CORRECT NAME..❌</b>',
                reply_markup=InlineKeyboardMarkup(kb) if kb else None
            )

    except Exception as e:
        LOGGER.error(f"Guess err: {e}\n{traceback.format_exc()}")

async def main():
    try:
        for mod in ALL_MODULES:
            try:
                importlib.import_module(f"shivu.modules.{mod}")
                LOGGER.info(f"✅ Loaded: {mod}")
            except Exception as e:
                LOGGER.error(f"❌ Failed: {mod} - {e}")

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
        application.add_handler(MessageHandler(filters.ALL, msg_counter, block=False))

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