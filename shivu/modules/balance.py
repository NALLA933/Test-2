import asyncio
from datetime import datetime, timedelta
from html import escape
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes
from shivu import application, db, LOGGER

bal_col = db['user_balance']
pay_cd = {}
pend_pay = {}

def fmt_t(s):
    h, r = divmod(int(s), 3600)
    m, s = divmod(r, 60)
    return f"{h}h {m}m {s}s" if h else f"{m}m {s}s"

async def g_usr(uid):
    u = await bal_col.find_one({'id': uid})
    if not u:
        await bal_col.insert_one({'id': uid, 'balance': 0, 'last_daily': None, 'transactions': []})
        u = await bal_col.find_one({'id': uid})
    return u

async def add_tx(uid, t, amt, d=""):
    await bal_col.update_one({'id': uid}, {'$push': {'transactions': {'type': t, 'amount': amt, 'description': d, 'timestamp': datetime.utcnow()}}})

async def bal_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    usr = await g_usr(u.effective_user.id)
    bal = int(usr.get('balance', 0))
    await u.message.reply_text(f"<b>💰 Balance</b>\n\nGold: <code>{bal:,}</code>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄", callback_data=f"b_{u.effective_user.id}")]]))

async def claim_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    uid = u.effective_user.id
    usr = await g_usr(uid)
    lst = usr.get('last_daily')
    now = datetime.utcnow()
    
    if lst:
        if isinstance(lst, str):
            lst = datetime.fromisoformat(lst)
        if lst.date() == now.date():
            rem = timedelta(days=1) - (now - lst)
            await u.message.reply_text(f"⚠️ Already claimed\nNext: {fmt_t(rem.total_seconds())}")
            return
    
    await bal_col.update_one({'id': uid}, {'$inc': {'balance': 2000}, '$set': {'last_daily': now}})
    await add_tx(uid, 'daily', 2000, "Daily")
    await u.message.reply_text("<b>✅ Daily Reward</b>\n\nClaimed: <code>2,000</code> gold", parse_mode="HTML")

async def pay_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not u.message.reply_to_message:
        await u.message.reply_text("⚠️ Reply to user")
        return
    
    sid = u.effective_user.id
    rec = u.message.reply_to_message.from_user
    
    if rec.id == sid or rec.is_bot:
        await u.message.reply_text("⚠️ Invalid recipient")
        return
    
    if sid in pay_cd:
        cd_time = pay_cd[sid]
        if isinstance(cd_time, str):
            cd_time = datetime.fromisoformat(cd_time)
        elapsed = (datetime.utcnow() - cd_time).total_seconds()
        if elapsed < 600:
            await u.message.reply_text(f"⚠️ Cooldown: {fmt_t(600 - elapsed)}")
            return
    
    try:
        amt = int(c.args[0])
        if amt <= 0 or amt > 1000000:
            raise ValueError
    except:
        await u.message.reply_text("Usage: /pay <amount>\nMax: 1,000,000")
        return
    
    sndr = await g_usr(sid)
    if sndr.get('balance', 0) < amt:
        await u.message.reply_text("⚠️ Insufficient balance")
        return
    
    pid = f"{sid}_{rec.id}_{int(datetime.utcnow().timestamp())}"
    pend_pay[pid] = {'s': sid, 'r': rec.id, 'a': amt}
    
    await u.message.reply_text(f"<b>Confirm</b>\n\nTo: <b>{escape(rec.first_name)}</b>\nAmount: <code>{amt:,}</code>\n\n⏱ 30s", parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✓", callback_data=f"c_{pid}"), InlineKeyboardButton("✗", callback_data=f"x_{pid}")]]))
    asyncio.create_task(exp_pay(pid))

async def exp_pay(pid):
    await asyncio.sleep(30)
    pend_pay.pop(pid, None)

async def hist_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    usr = await g_usr(u.effective_user.id)
    txs = usr.get('transactions', [])
    
    if not txs:
        await u.message.reply_text("⚠️ No transactions")
        return
    
    msg = "<b>📜 History</b>\n\n"
    for t in reversed(txs[-10:]):
        amt = t.get('amount', 0)
        tp = t.get('type', 'unknown')
        ts = t.get('timestamp')
        if ts and isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        dt = ts.strftime('%d/%m %H:%M') if ts else 'N/A'
        msg += f"{'💰' if amt > 0 else '💸'} <code>{amt:+,}</code> • {tp}\n   {dt}\n\n"
    
    await u.message.reply_text(msg, parse_mode="HTML")

async def cb_handler(u: Update, c: ContextTypes.DEFAULT_TYPE):
    q = u.callback_query
    await q.answer()
    d = q.data
    uid = q.from_user.id
    
    if d.startswith("b_"):
        if uid != int(d.split("_")[1]):
            return
        usr = await g_usr(uid)
        await q.edit_message_text(f"<b>💰 Balance</b>\n\nGold: <code>{usr.get('balance', 0):,}</code>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄", callback_data=f"b_{uid}")]]))
    
    elif d.startswith("c_"):
        pid = d[2:]
        if pid not in pend_pay or uid != pend_pay[pid]['s']:
            await q.edit_message_text("⚠️ Invalid", parse_mode="HTML")
            return
        
        p = pend_pay[pid]
        sndr = await g_usr(p['s'])
        if sndr.get('balance', 0) < p['a']:
            await q.edit_message_text("⚠️ Insufficient", parse_mode="HTML")
            del pend_pay[pid]
            return
        
        await g_usr(p['r'])
        await bal_col.update_one({'id': p['s']}, {'$inc': {'balance': -p['a']}})
        await bal_col.update_one({'id': p['r']}, {'$inc': {'balance': p['a']}})
        await add_tx(p['s'], 'sent', -p['a'], "Payment")
        await add_tx(p['r'], 'received', p['a'], "Payment")
        pay_cd[p['s']] = datetime.utcnow()
        
        try:
            rec_u = await c.bot.get_chat(p['r'])
            rec_n = escape(rec_u.first_name)
        except:
            rec_n = "Unknown"
        
        del pend_pay[pid]
        await q.edit_message_text(f"<b>✅ Sent</b>\n\nTo: <b>{rec_n}</b>\nAmount: <code>{p['a']:,}</code>", parse_mode="HTML")
    
    elif d.startswith("x_"):
        pid = d[2:]
        if pid in pend_pay and uid == pend_pay[pid]['s']:
            del pend_pay[pid]
        await q.edit_message_text("<b>✗ Cancelled</b>", parse_mode="HTML")

application.add_handler(CommandHandler("bal", bal_cmd))
application.add_handler(CommandHandler("cclaim", claim_cmd))
application.add_handler(CommandHandler("pay", pay_cmd))
application.add_handler(CommandHandler("history", hist_cmd))
application.add_handler(CallbackQueryHandler(cb_handler))

LOGGER.info("✅ Balance system loaded")