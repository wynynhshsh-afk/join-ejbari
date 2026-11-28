"""
فروش خودکار جوین اجباری.

معماری:
- همه‌ی داده‌های فروش (رزروها، پرداخت‌ها، کاربران) اول داخل حافظه (dict) تغییر می‌کنند و
  بعد توسط یک writer پس‌زمینه به‌صورت دسته‌ای (upsert) در Supabase ذخیره می‌شوند.
  بررسی ظرفیت روزها هیچ تماس دیتابیسی ندارد و فقط از حافظه خوانده می‌شود.
- پرداخت‌ها بلافاصله بعد از ثبت flush می‌شوند و هنگام خاموش‌شدن سرویس هم flush نهایی انجام می‌شود.
- آیتم فروخته‌شده بعد از فعال‌سازی در همان جدول channels (بدون تغییر ساختار) ثبت می‌شود،
  پس موتور جوین اجباری گروه بدون هیچ تغییری آن را اعمال می‌کند.
- جدول‌های قبلی و آیتم‌های موجود در جوین اجباری دست‌نخورده می‌مانند؛ فقط جدول‌های جدید sale_* اضافه شده‌اند.
"""

from __future__ import annotations

import asyncio
import html
import io
import json
import logging
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from telegram import (
    Chat,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    LinkPreviewOptions,
    Message,
    ReplyKeyboardMarkup,
    Update,
    User,
)
from telegram.constants import ChatMemberStatus, ParseMode
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TelegramError, TimedOut
from telegram.ext import ContextTypes

import bot as core

logger = logging.getLogger("forcejoin_bot.sales")

# ---------------------------------------------------------------------------
# ثابت‌ها
# ---------------------------------------------------------------------------

TEHRAN_TZ = timezone(timedelta(hours=3, minutes=30))   # ایران از ۱۴۰۱ ساعت تابستانی ندارد
DAYS_AHEAD = 14                                        # چند روز آینده برای رزرو نمایش داده می‌شود
PAYMENT_HOLD = timedelta(minutes=30)                   # مدت نگه‌داشتن رزرو تا ارسال فیش
MAX_TARIFFS = 10
MAX_LANES = 50
MAX_REPORT_TRIES = 5
FLUSH_INTERVAL_SECONDS = 0.7
RETRY_ACTIVATION_SECONDS = 300

# وضعیت‌هایی که یک لاین از روزِ شروع را اشغال می‌کنند
CAPACITY_STATUSES = {"awaiting_payment", "pending_review", "approved", "active", "finished"}
OPEN_STATUSES = {"awaiting_payment", "pending_review", "approved", "active"}
UNBOUND_OK_STATUSES = {"awaiting_payment", "pending_review", "approved"}

STATUS_LABEL = {
    "awaiting_payment": "💳 در انتظار پرداخت",
    "pending_review": "⏳ در انتظار تایید فیش",
    "approved": "✅ تایید شد، منتظر روز جوین",
    "active": "🟢 در حال جوین اجباری",
    "finished": "🏁 پایان یافته",
    "rejected": "❌ فیش تایید نشد",
    "cancelled": "⛔️ لغو شد",
    "expired": "⌛️ مهلت پرداخت تمام شد",
}

BTN_BUY = "🛒 خرید جوین"
BTN_MY = "📋 رزروهای من"
BTN_BACK = "🔙 بازگشت"
BTN_CANCEL = "❌ انصراف از رزرو"
BTN_TYPE_CHAT = "📢 کانال / گروه"
BTN_TYPE_BOT = "🤖 ربات"
TYPE_LABEL = {"chat": "📢 کانال/گروه", "bot": "🤖 ربات"}
SKIP_WORDS = {"-", "—", "–", "0", "ندارم", "خیر", "هیچ"}

NO_PREVIEW = LinkPreviewOptions(is_disabled=True)

# ---------------------------------------------------------------------------
# تاریخ شمسی و زمان
# ---------------------------------------------------------------------------

WEEKDAYS_FA = {5: "شنبه", 6: "یکشنبه", 0: "دوشنبه", 1: "سه‌شنبه", 2: "چهارشنبه", 3: "پنجشنبه", 4: "جمعه"}
_TO_FA = str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹")


def to_fa(value) -> str:
    return str(value).translate(_TO_FA)


def gregorian_to_jalali(gy: int, gm: int, gd: int) -> tuple:
    g_d_m = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    if gy > 1600:
        jy = 979
        gy -= 1600
    else:
        jy = 0
        gy -= 621
    gy2 = gy + 1 if gm > 2 else gy
    days = 365 * gy + (gy2 + 3) // 4 - (gy2 + 99) // 100 + (gy2 + 399) // 400 - 80 + gd + g_d_m[gm - 1]
    jy += 33 * (days // 12053)
    days %= 12053
    jy += 4 * (days // 1461)
    days %= 1461
    if days > 365:
        jy += (days - 1) // 365
        days = (days - 1) % 365
    if days < 186:
        jm = 1 + days // 31
        jd = 1 + days % 31
    else:
        jm = 7 + (days - 186) // 30
        jd = 1 + (days - 186) % 30
    return jy, jm, jd


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None


def tehran_today(now: Optional[datetime] = None) -> date:
    return (now or utcnow()).astimezone(TEHRAN_TZ).date()


def day_start_utc(day_iso: str) -> datetime:
    d = date.fromisoformat(day_iso)
    return datetime(d.year, d.month, d.day, tzinfo=TEHRAN_TZ).astimezone(timezone.utc)


def fa_date(d: date) -> str:
    jy, jm, jd = gregorian_to_jalali(d.year, d.month, d.day)
    return to_fa(f"{jy}/{jm:02d}/{jd:02d}")


def fa_day_full(day_iso: str) -> str:
    d = date.fromisoformat(day_iso)
    return f"{fa_date(d)} ({WEEKDAYS_FA[d.weekday()]})"


def fa_datetime(dt: datetime) -> str:
    local = dt.astimezone(TEHRAN_TZ)
    return f"{fa_date(local.date())} ساعت {to_fa(local.strftime('%H:%M'))}"


def fmt_hours(hours: int) -> str:
    text = f"{hours} ساعته"
    if hours >= 24 and hours % 24 == 0:
        text += f" ({hours // 24} روزه)"
    return text


def fmt_money(amount: int) -> str:
    return f"{int(amount):,} تومان"


def fmt_card(number: str) -> str:
    return " ".join(number[i:i + 4] for i in range(0, len(number), 4))


def e(text) -> str:
    return html.escape(str(text if text is not None else ""))


def mention_html(user: User) -> str:
    name = e(user.first_name or "کاربر")
    return f'<a href="tg://user?id={user.id}">{name}</a>'


def user_label(uid: int) -> str:
    row = USERS.get(uid)
    if not row:
        return f'<a href="tg://user?id={uid}">{uid}</a>'
    name = " ".join(p for p in (row.get("first_name"), row.get("last_name")) if p) or str(uid)
    return f'<a href="tg://user?id={uid}">{e(name)}</a>'


def _retry_seconds(exc: RetryAfter) -> float:
    wait = exc.retry_after
    return wait.total_seconds() if hasattr(wait, "total_seconds") else float(wait)


# ---------------------------------------------------------------------------
# حافظه + ذخیره‌ی تاخیری در Supabase (write-behind)
# ---------------------------------------------------------------------------

RES: dict = {}      # reservation_id -> row (دقیقاً ستون‌های sale_reservations)
PAY: dict = {}      # payment_id -> row (دقیقاً ستون‌های sale_payments)
USERS: dict = {}    # user_id -> row (دقیقاً ستون‌های sale_users)

CFG: dict = {"enabled": False, "tariffs": [], "bot_tariffs": [], "card_number": "", "card_name": "", "lanes": 0}

TABLE_PK = {"sale_reservations": "id", "sale_payments": "id", "sale_users": "user_id"}
_DIRTY: dict = {}
_flush_lock = asyncio.Lock()
_last_id = 0
_writer_task: Optional[asyncio.Task] = None
_flush_failed_logged = 0.0


def new_id() -> int:
    global _last_id
    nid = time.time_ns() // 1_000_000
    if nid <= _last_id:
        nid = _last_id + 1
    _last_id = nid
    return nid


def mark_dirty(table: str, row: dict) -> None:
    _DIRTY[(table, row[TABLE_PK[table]])] = dict(row)


def _db_upsert(table: str, rows: list) -> None:
    pk = TABLE_PK[table]
    for i in range(0, len(rows), 100):
        core.supabase.table(table).upsert(rows[i:i + 100], on_conflict=pk).execute()


async def flush() -> bool:
    """همه‌ی تغییرات در انتظار را در دیتابیس می‌نویسد. در صورت خطا، تغییرات برای تلاش بعدی نگه داشته می‌شوند."""
    global _flush_failed_logged
    if not _DIRTY:
        return True
    async with _flush_lock:
        if not _DIRTY:
            return True
        batch = dict(_DIRTY)
        _DIRTY.clear()
        by_table: dict = {}
        for (table, _key), row in batch.items():
            by_table.setdefault(table, []).append(row)
        ok = True
        for table, rows in by_table.items():
            try:
                await asyncio.to_thread(_db_upsert, table, rows)
            except Exception as exc:  # noqa: BLE001
                ok = False
                for row in rows:
                    _DIRTY.setdefault((table, row[TABLE_PK[table]]), row)  # نسخه‌ی جدیدتر (اگر هست) برنده است
                if time.monotonic() - _flush_failed_logged > 60:
                    _flush_failed_logged = time.monotonic()
                    logger.error("ذخیره‌ی %s در دیتابیس ناموفق بود (جدول‌های فروش ساخته شده؟): %s", table, exc)
        return ok


async def writer_loop() -> None:
    while True:
        try:
            await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
            ok = await flush()
            if not ok:
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error("خطا در writer دیتابیس فروش: %s", exc, exc_info=exc)
            await asyncio.sleep(5)


def _db_select_all(table: str) -> list:
    rows: list = []
    start = 0
    while True:
        res = core.supabase.table(table).select("*").range(start, start + 999).execute()
        chunk = res.data or []
        rows.extend(chunk)
        if len(chunk) < 1000:
            return rows
        start += 1000


async def load_all() -> None:
    try:
        res_rows = await asyncio.to_thread(_db_select_all, "sale_reservations")
        pay_rows = await asyncio.to_thread(_db_select_all, "sale_payments")
        user_rows = await asyncio.to_thread(_db_select_all, "sale_users")
    except Exception as exc:  # noqa: BLE001
        logger.warning("خواندن جدول‌های فروش ممکن نشد (فایل supabase_schema.sql اجرا شده؟): %s", exc)
        return
    for row in res_rows:
        RES[int(row["id"])] = row
    for row in pay_rows:
        PAY[int(row["id"])] = row
    for row in user_rows:
        USERS[int(row["user_id"])] = row
    logger.info("فروش: %d رزرو، %d پرداخت، %d کاربر از دیتابیس بارگذاری شد.", len(RES), len(PAY), len(USERS))


async def schema_error() -> Optional[str]:
    """اگر جدول‌های فروش وجود نداشته باشند متن خطا، وگرنه None."""

    def _run():
        for table in TABLE_PK:
            core.supabase.table(table).select("*").limit(1).execute()

    try:
        await asyncio.to_thread(_run)
        return None
    except Exception as exc:  # noqa: BLE001
        return str(exc)


# ---------------------------------------------------------------------------
# تنظیمات فروش (جدول settings موجود؛ بدون تغییر ساختار)
# ---------------------------------------------------------------------------


async def load_config() -> None:
    enabled, tariffs, bot_tariffs, card, lanes = await asyncio.gather(
        core.get_setting("sale_enabled"),
        core.get_setting("sale_tariffs"),
        core.get_setting("sale_bot_tariffs"),
        core.get_setting("sale_card"),
        core.get_setting("sale_lanes"),
    )
    CFG["enabled"] = enabled == "1"

    def _tariffs(raw) -> list:
        try:
            parsed = json.loads(raw) if raw else []
            return [{"hours": int(t["hours"]), "price": int(t["price"])} for t in parsed if int(t["hours"]) > 0]
        except (ValueError, TypeError, KeyError):
            return []

    CFG["tariffs"] = _tariffs(tariffs)
    CFG["bot_tariffs"] = _tariffs(bot_tariffs)
    try:
        card_obj = json.loads(card) if card else {}
        CFG["card_number"] = str(card_obj.get("number", ""))
        CFG["card_name"] = str(card_obj.get("name", ""))
    except (ValueError, TypeError, AttributeError):
        CFG["card_number"], CFG["card_name"] = "", ""
    try:
        CFG["lanes"] = max(int(lanes), 0) if lanes else 0
    except ValueError:
        CFG["lanes"] = 0


async def persist_config(*keys: str) -> list:
    values = {
        "enabled": ("sale_enabled", "1" if CFG["enabled"] else "0"),
        "tariffs": ("sale_tariffs", json.dumps(CFG["tariffs"], ensure_ascii=False)),
        "bot_tariffs": ("sale_bot_tariffs", json.dumps(CFG["bot_tariffs"], ensure_ascii=False)),
        "card": ("sale_card", json.dumps({"number": CFG["card_number"], "name": CFG["card_name"]}, ensure_ascii=False)),
        "lanes": ("sale_lanes", str(CFG["lanes"])),
    }
    errors = []
    for key in keys:
        setting_key, value = values[key]
        err = await core.set_setting(setting_key, value)
        if err:
            errors.append(err)
    return errors


def sale_enabled() -> bool:
    return bool(CFG["enabled"])


def sale_ready() -> bool:
    return bool(CFG["enabled"] and config_complete())


def config_complete() -> bool:
    return bool((CFG["tariffs"] or CFG["bot_tariffs"]) and CFG["card_number"] and CFG["card_name"] and CFG["lanes"] > 0)


def tariffs_for(kind: str) -> list:
    return CFG["bot_tariffs"] if kind == "bot" else CFG["tariffs"]


def available_kinds() -> list:
    kinds = []
    if CFG["tariffs"]:
        kinds.append("chat")
    if CFG["bot_tariffs"]:
        kinds.append("bot")
    return kinds


def res_kind(res: dict) -> str:
    return "bot" if res.get("chat_kind") == "bot" else "chat"


# ---------------------------------------------------------------------------
# پارس ورودی‌های مالک
# ---------------------------------------------------------------------------

_TARIFF_RE = re.compile(r"^(\d{1,5})\s*[=:＝：\-–]\s*([\d,٬.\s]+?)\s*(?:تومان|ت)?$")
_CARD_RE = re.compile(r"(?:\d[\s\-]?){16}")


def norm_digits(text: str) -> str:
    return (text or "").translate(core._PERSIAN_DIGITS)


def parse_tariffs(text: str) -> tuple:
    """خروجی: (لیست تعرفه‌ها، خطوط نامعتبر). هر خط: «ساعت = مبلغ»."""
    prices: dict = {}
    bad: list = []
    for raw in (text or "").splitlines():
        line = norm_digits(raw).strip()
        if not line:
            continue
        m = _TARIFF_RE.match(line)
        if not m:
            bad.append(raw.strip())
            continue
        hours = int(m.group(1))
        price = int(re.sub(r"\D", "", m.group(2)) or 0)
        if not 1 <= hours <= 8760 or price < 1:
            bad.append(raw.strip())
            continue
        prices[hours] = price
    if len(prices) > MAX_TARIFFS:
        bad.append(f"حداکثر {MAX_TARIFFS} تعرفه مجاز است")
    tariffs = [{"hours": h, "price": p} for h, p in sorted(prices.items())]
    return tariffs, bad


def parse_card(text: str) -> Optional[tuple]:
    t = norm_digits(text or "")
    m = _CARD_RE.search(t)
    if not m:
        return None
    number = re.sub(r"\D", "", m.group(0))
    rest = f"{t[:m.start()]} {t[m.end():]}"
    rest = re.sub(r"[\n|،,:؛;\-–]+", " ", rest)
    name = " ".join(rest.split())
    if not 2 <= len(name) <= 60:
        return None
    return number, name


# ---------------------------------------------------------------------------
# ارسال امن پیام
# ---------------------------------------------------------------------------


async def safe_send(bot, chat_id: int, text: str, **kwargs) -> Optional[Message]:
    for _ in range(2):
        try:
            return await bot.send_message(
                chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW, **kwargs
            )
        except RetryAfter as exc:
            await asyncio.sleep(_retry_seconds(exc) + 0.2)
        except (BadRequest, Forbidden, TimedOut, NetworkError) as exc:
            logger.warning("ارسال پیام به %s ناموفق: %s", chat_id, exc)
            return None
    return None


async def reply(update: Update, text: str, **kwargs) -> Optional[Message]:
    message = update.effective_message
    for _ in range(2):
        try:
            return await message.reply_text(
                text, parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW, **kwargs
            )
        except RetryAfter as exc:
            await asyncio.sleep(_retry_seconds(exc) + 0.2)
        except (BadRequest, Forbidden, TimedOut, NetworkError) as exc:
            logger.warning("پاسخ به %s ناموفق: %s", update.effective_user.id if update.effective_user else "?", exc)
            return None
    return None


async def notify_admins(bot, text: str, **kwargs) -> list:
    sent = []
    for admin_id in core.ADMIN_IDS:
        msg = await safe_send(bot, admin_id, text, **kwargs)
        if msg is not None:
            sent.append((admin_id, msg.message_id))
    return sent


# ---------------------------------------------------------------------------
# کاربران
# ---------------------------------------------------------------------------


def touch_user(user: User) -> None:
    """مشخصات کاربر در حافظه به‌روز و (فقط در صورت تغییر یا هر یک ساعت) برای ذخیره در دیتابیس علامت‌گذاری می‌شود."""
    now = utcnow()
    row = USERS.get(user.id)
    fields = {"first_name": user.first_name, "last_name": user.last_name, "username": user.username}
    if row is None:
        row = {"user_id": user.id, **fields, "started_at": iso(now), "last_seen_at": iso(now)}
        USERS[user.id] = row
        mark_dirty("sale_users", row)
        return
    changed = any(row.get(k) != v for k, v in fields.items())
    last_seen = core._parse_ts(row.get("last_seen_at"))
    if changed or last_seen is None or now - last_seen > timedelta(hours=1):
        row.update(fields)
        row["last_seen_at"] = iso(now)
        mark_dirty("sale_users", row)


# ---------------------------------------------------------------------------
# رزروها: ظرفیت روزها
# ---------------------------------------------------------------------------


def _holds_lane(res: dict, now: datetime) -> bool:
    status = res.get("status")
    if status not in CAPACITY_STATUSES:
        return False
    if status == "awaiting_payment":
        hold = core._parse_ts(res.get("hold_expires_at"))
        if hold is not None and hold <= now:
            return False
    return True


def _res_interval(res: dict) -> tuple:
    start = day_start_utc(res["day"])
    end = start + timedelta(hours=res["hours"])
    if res.get("status") in ("active", "finished"):
        actual = core._parse_ts(res.get("expires_at"))
        if actual is not None and actual > end:
            end = actual
    return start, end


def usage_for_day(day_iso: str, now: datetime, items: Optional[list]) -> int:
    """
    تعداد لاین‌های اشغال‌شده‌ی یک روز (به وقت تهران) = فروش‌های خودکاری که آن روز در جوین اجباری هستند
    + آیتم‌هایی که ادمین دستی ثبت کرده و آن روز فعال‌اند. آیتم‌های دستی همیشه قابل افزودن‌اند؛
    فقط ظرفیتِ فروش خودکار را کم می‌کنند.
    """
    d0 = day_start_utc(day_iso)
    d1 = d0 + timedelta(days=1)
    sold_item_ids = {r["item_id"] for r in RES.values() if r.get("item_id")}
    used = 0
    for res in RES.values():
        if _holds_lane(res, now):
            start, end = _res_interval(res)
            if start < d1 and end > d0:
                used += 1
    for item in items or []:
        if item.get("id") in sold_item_ids or item.get("quota_done"):
            continue
        expires = core._parse_ts(item.get("expires_at"))
        if expires is None or expires > d0:
            used += 1
    return used


def build_days(now: datetime, items: Optional[list]) -> list:
    """[(date, iso, is_full)] برای امروز تا DAYS_AHEAD روز آینده."""
    today = tehran_today(now)
    lanes = CFG["lanes"]
    days = []
    for i in range(DAYS_AHEAD):
        d = today + timedelta(days=i)
        key = d.isoformat()
        days.append((d, key, usage_for_day(key, now, items) >= lanes))
    return days


def day_is_full(day_iso: str, now: datetime, items: Optional[list]) -> bool:
    return usage_for_day(day_iso, now, items) >= CFG["lanes"]


def open_hold(user_id: int, now: Optional[datetime] = None) -> Optional[dict]:
    now = now or utcnow()
    for res in RES.values():
        if res["user_id"] == user_id and res["status"] == "awaiting_payment" and _holds_lane(res, now):
            return res
    return None


def find_unbound(user_id: int, kind: str = "chat") -> Optional[dict]:
    rows = [
        r for r in RES.values()
        if r["user_id"] == user_id and not r.get("chat_id") and r["status"] in UNBOUND_OK_STATUSES
        and res_kind(r) == kind
    ]
    rows.sort(key=lambda r: (r.get("created_at") or "", r["id"]))
    return rows[0] if rows else None


def create_reservation(user_id: int, day_iso: str, tariff: dict, kind: str, items: Optional[list]) -> Optional[dict]:
    """بررسی ظرفیت و ثبت رزرو بدون هیچ await بین این دو؛ در نتیجه دو خریدار هم‌زمان یک لاین را نمی‌گیرند."""
    now = utcnow()
    if day_is_full(day_iso, now, items):
        return None
    res = {
        "id": new_id(),
        "user_id": user_id,
        "day": day_iso,
        "hours": int(tariff["hours"]),
        "amount": int(tariff["price"]),
        "status": "awaiting_payment",
        "chat_id": None,
        "chat_kind": "bot" if kind == "bot" else None,
        "chat_title": None,
        "item_id": None,
        "payment_id": None,
        "hold_expires_at": iso(now + PAYMENT_HOLD),
        "approved_at": None,
        "bound_at": None,
        "activated_at": None,
        "expires_at": None,
        "finished_at": None,
        "report_sent": False,
        "created_at": iso(now),
    }
    RES[res["id"]] = res
    mark_dirty("sale_reservations", res)
    return res


def set_status(res: dict, status: str, **extra) -> None:
    res["status"] = status
    res.update(extra)
    mark_dirty("sale_reservations", res)


# ---------------------------------------------------------------------------
# کیبوردها و متن‌های خریدار
# ---------------------------------------------------------------------------


def _kb(rows: list, placeholder: Optional[str] = None) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        rows, resize_keyboard=True, is_persistent=True, input_field_placeholder=placeholder
    )


def _btn(text: str, style: Optional[str] = None) -> KeyboardButton:
    return KeyboardButton(text, style=style)


def buyer_menu_kb() -> ReplyKeyboardMarkup:
    return _kb([[_btn(BTN_BUY, "success")], [_btn(BTN_MY, "primary")]])


def _day_label(d: date, today: date, full: bool) -> str:
    extra = "امروز" if d == today else "فردا" if d == today + timedelta(days=1) else WEEKDAYS_FA[d.weekday()]
    return f"{'🔴' if full else '🟢'} {fa_date(d)} {extra}"


def tariff_label(t: dict) -> str:
    return f"⏱ {fmt_hours(t['hours'])} — {fmt_money(t['price'])}"


def add_bot_markup(bot_username: str) -> InlineKeyboardMarkup:
    base = f"https://t.me/{bot_username}"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("➕ افزودن ربات به گروه", url=f"{base}?startgroup=join&admin=invite_users", style="success")],
            [InlineKeyboardButton("➕ افزودن ربات به کانال", url=f"{base}?startchannel=join&admin=invite_users", style="primary")],
        ]
    )


_group_display_cache: dict = {}


async def group_display(bot) -> str:
    """منشن گروه اصلی (لینک‌دار در صورت امکان)."""
    group_id = await core.get_group_id()
    if group_id is None:
        return "<b>ما</b>"
    hit = _group_display_cache.get(group_id)
    if hit and time.monotonic() - hit[0] < 600:
        return hit[1]
    text = "<b>ما</b>"
    try:
        chat = await bot.get_chat(group_id)
        title = e(chat.title or "گروه")
        link = f"https://t.me/{chat.username}" if chat.username else chat.invite_link
        text = f'<a href="{e(link)}">{title}</a>' if link else f"<b>{title}</b>"
    except (BadRequest, Forbidden, TimedOut, NetworkError) as exc:
        logger.warning("دریافت اطلاعات گروه اصلی ممکن نشد: %s", exc)
    _group_display_cache[group_id] = (time.monotonic(), text)
    return text


def guide_steps(bot_username: str) -> list:
    example = "12 ساعته مبلغ 250000"
    kinds = available_kinds()
    if kinds:
        t = tariffs_for(kinds[0])[0]
        example = f"{t['hours']} ساعته مبلغ {t['price']}"
    return [
        "روی دکمه «خرید جوین» بزنید، نوع (کانال/گروه یا ربات) را مشخص کنید و یک روز خالی را رزرو کنید (روزهای خالی به رنگ سبز 🟢 و روزهای رزرو شده به رنگ قرمز 🔴 هستند).",
        f"سپس تایم ماندگاری رزرو را انتخاب کنید (مثال: {example}).",
        "سپس مبلغ را پرداخت کنید و فیش پرداخت را برای ربات ارسال کنید.",
        f"اگر کانال یا گروه دارید، ربات @{bot_username} را با دکمه «افزودن به گروه/کانال» ادمین کنید. اگر ربات دارید، یوزرنیم ربات را برای من بفرستید.",
        "سپس ربات به صورت خودکار گروه/کانال/ربات شما را تشخیص می‌دهد و شما را وارد رزروها می‌کند.",
        "پس از تایید فیش توسط مالک گروه، شما آماده می‌شوید تا روز جوین اجباری و به صورت خودکار وارد لیست می‌شوید.",
        "پس از پایان جوین، ربات به صورت خودکار عکس قبل و بعد را برایتان می‌فرستد.",
    ]


async def welcome_text(bot, user: User) -> str:
    group = await group_display(bot)
    steps = "\n".join(f"{to_fa(i)}- {e(s)}" for i, s in enumerate(guide_steps(bot.username or ""), start=1))
    return (
        f"با سلام خدمت {mention_html(user)}\n"
        f"به ربات جوین اجباری گروه {group} خوش آمدید\n\n"
        "<b>راهنمای خرید از جوین اجباری از ربات</b>\n"
        f"<blockquote>{steps}</blockquote>"
    )


def payment_text(res: dict) -> str:
    hold = core._parse_ts(res.get("hold_expires_at"))
    minutes = int(PAYMENT_HOLD.total_seconds() // 60)
    kind = res_kind(res)
    text = (
        "✅ <b>رزرو شما ثبت شد</b>\n\n"
        f"🏷 نوع: {TYPE_LABEL[kind]}\n"
        f"📅 روز: {fa_day_full(res['day'])}\n"
        f"⏱ مدت: {fmt_hours(res['hours'])}\n"
        f"💰 مبلغ: <b>{fmt_money(res['amount'])}</b>\n\n"
        f"💳 شماره کارت:\n<code>{fmt_card(CFG['card_number'])}</code>\n"
        f"👤 به نام: <b>{e(CFG['card_name'])}</b>\n\n"
        "پس از پرداخت، <b>تصویر فیش</b> (یا کد پیگیری) را همین‌جا ارسال کنید.\n"
    )
    if kind == "bot" and not res.get("chat_id"):
        text += "🤖 همچنین <b>یوزرنیم ربات</b> خود را بفرستید (مثال: <code>@MyBot</code>).\n"
    text += f"⏳ این رزرو تا {to_fa(minutes)} دقیقه برای شما نگه داشته می‌شود"
    if hold:
        text += f" (تا ساعت {to_fa(hold.astimezone(TEHRAN_TZ).strftime('%H:%M'))})"
    return text + "."


# ---------------------------------------------------------------------------
# جریان خریدار
# ---------------------------------------------------------------------------

BUYER: dict = {}   # user_id -> {"step": ..., ...}


def _state(user_id: int) -> dict:
    if len(BUYER) > 20000:
        BUYER.clear()
    return BUYER.setdefault(user_id, {"step": "menu"})


async def buyer_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    touch_user(user)
    if not sale_enabled():
        await reply(update, "این ربات فقط توسط ادمین‌ها قابل مدیریت است.")
        return
    BUYER[user.id] = {"step": "menu"}
    await reply(update, await welcome_text(context.bot, user), reply_markup=buyer_menu_kb())


async def _show_menu(update: Update, text: str = "یکی از گزینه‌ها را انتخاب کنید:") -> None:
    BUYER[update.effective_user.id] = {"step": "menu"}
    await reply(update, text, reply_markup=buyer_menu_kb())


async def _show_days(update: Update, kind: str) -> None:
    user_id = update.effective_user.id
    now = utcnow()
    items = await core.list_all_items()
    days = build_days(now, items)
    today = tehran_today(now)
    if all(full for _d, _k, full in days):
        await _show_menu(update, "😔 فعلاً همه‌ی روزها پر شده‌اند. بعداً دوباره سر بزنید.")
        return
    labels: dict = {}
    buttons = []
    for d, key, full in days:
        label = _day_label(d, today, full)
        labels[label] = (key, full)
        buttons.append(_btn(label, "danger" if full else "success"))
    rows = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
    rows.append([_btn(BTN_BACK)])
    BUYER[user_id] = {"step": "days", "type": kind, "days": labels}
    await reply(
        update,
        f"{TYPE_LABEL[kind]}\n📅 <b>یک روز خالی را انتخاب کنید</b>\n🟢 خالی  |  🔴 پر",
        reply_markup=_kb(rows, "روز مورد نظر را انتخاب کنید"),
    )


async def _show_types(update: Update) -> None:
    kinds = available_kinds()
    if len(kinds) == 1:
        await _show_days(update, kinds[0])
        return
    BUYER[update.effective_user.id] = {"step": "type"}
    await reply(
        update,
        "🏷 <b>چه چیزی را می‌خواهید در جوین اجباری قرار دهید؟</b>",
        reply_markup=_kb(
            [[_btn(BTN_TYPE_CHAT, "primary"), _btn(BTN_TYPE_BOT, "primary")], [_btn(BTN_BACK)]],
            "نوع را انتخاب کنید",
        ),
    )


async def _show_tariffs(update: Update, day_iso: str, kind: str) -> None:
    labels: dict = {}
    rows = []
    for idx, t in enumerate(tariffs_for(kind)):
        label = tariff_label(t)
        labels[label] = idx
        rows.append([_btn(label, "primary")])
    rows.append([_btn(BTN_BACK)])
    BUYER[update.effective_user.id] = {"step": "tariffs", "type": kind, "day": day_iso, "tariffs": labels}
    await reply(
        update,
        f"{TYPE_LABEL[kind]}\n📅 روز انتخابی: <b>{fa_day_full(day_iso)}</b>\n\n⏱ <b>مدت ماندگاری جوین اجباری را انتخاب کنید:</b>",
        reply_markup=_kb(rows, "مدت را انتخاب کنید"),
    )


async def _show_payment(update: Update, res: dict) -> None:
    BUYER[update.effective_user.id] = {"step": "receipt", "res_id": res["id"]}
    await reply(
        update,
        payment_text(res),
        reply_markup=_kb([[_btn(BTN_CANCEL, "danger")], [_btn(BTN_BACK)]], "فیش پرداخت را ارسال کنید"),
    )


async def _start_buy(update: Update) -> None:
    if not sale_ready():
        await _show_menu(update, "⚠️ خرید جوین هم‌اکنون فعال نیست. بعداً دوباره تلاش کنید.")
        return
    hold = open_hold(update.effective_user.id)
    if hold is not None:
        await _show_payment(update, hold)  # رزروِ منتظر پرداخت دارد؛ ادامه‌ی همان
        return
    await _show_types(update)


async def _pick_day(update: Update, state: dict, text: str) -> None:
    kind = state.get("type", "chat")
    entry = state.get("days", {}).get(text)
    if entry is None:
        await _show_days(update, kind)
        return
    day_iso, _was_full = entry
    items = await core.list_all_items()
    if day_is_full(day_iso, utcnow(), items):
        await reply(update, "🔴 این روز پر شده است. روز دیگری را انتخاب کنید.")
        await _show_days(update, kind)
        return
    await _show_tariffs(update, day_iso, kind)


async def _pick_tariff(update: Update, state: dict, text: str) -> None:
    kind = state.get("type", "chat")
    tariffs = tariffs_for(kind)
    idx = state.get("tariffs", {}).get(text)
    day_iso = state.get("day")
    if idx is None or day_iso is None or idx >= len(tariffs):
        await _start_buy(update)
        return
    items = await core.list_all_items()   # await قبل از بررسیِ ظرفیت؛ بررسی و ثبت بعدش پشت‌سرهم و بدون await هستند
    res = create_reservation(update.effective_user.id, day_iso, tariffs[idx], kind, items)
    if res is None:
        await reply(update, "🔴 همین الان آخرین لاین این روز پر شد. روز دیگری را انتخاب کنید.")
        await _show_days(update, kind)
        return
    await _show_payment(update, res)


async def _cancel_hold(update: Update) -> None:
    hold = open_hold(update.effective_user.id)
    if hold is not None:
        set_status(hold, "cancelled")
        await _show_menu(update, "رزرو شما لغو شد.")
    else:
        await _show_menu(update)


async def _show_my(update: Update) -> None:
    uid = update.effective_user.id
    rows = [r for r in RES.values() if r["user_id"] == uid and r["status"] != "cancelled"]
    rows.sort(key=lambda r: r.get("created_at") or "", reverse=True)
    if not rows:
        await _show_menu(update, "هنوز رزروی ندارید.")
        return
    lines = []
    for r in rows[:10]:
        kind = res_kind(r)
        line = (
            f"{TYPE_LABEL[kind]} — 📅 {fa_day_full(r['day'])} — ⏱ {fmt_hours(r['hours'])}\n"
            f"{STATUS_LABEL.get(r['status'], r['status'])}"
        )
        if r["status"] in ("approved", "pending_review", "awaiting_payment") and not r.get("chat_id"):
            line += (
                "\n⚠️ هنوز یوزرنیم ربات شما ثبت نشده است."
                if kind == "bot"
                else "\n⚠️ هنوز ربات به‌عنوان ادمین گروه/کانال شما ثبت نشده است."
            )
        elif r["status"] == "active" and r.get("expires_at"):
            line += f"\n🕓 پایان: {fa_datetime(core._parse_ts(r['expires_at']))}"
        if r.get("chat_title"):
            line += f"\n📌 {e(r['chat_title'])}"
        lines.append(line)
    await _show_menu(update, "📋 <b>رزروهای شما</b>\n\n" + "\n\n".join(lines))


_BOT_NAME_RE = re.compile(r"^[A-Za-z0-9_]{2,29}[Bb][Oo][Tt]$")


def extract_bot_username(text: str) -> Optional[str]:
    """از @MyBot ، MyBot یا https://t.me/MyBot?start=... یوزرنیم ربات را درمی‌آورد (نام ربات باید به bot ختم شود)."""
    t = (text or "").strip()
    m = re.match(r"^(?:https?://)?(?:t|telegram)\.me/([A-Za-z0-9_]{5,32})(?:/\S*)?(?:\?.*)?$", t, re.IGNORECASE)
    name = m.group(1) if m else t.lstrip("@")
    return name if len(name) >= 5 and _BOT_NAME_RE.match(name) else None


async def bind_bot_username(bot, res: dict, username: str) -> None:
    title = username
    try:
        info = await bot.get_chat(f"@{username}")
        title = info.first_name or info.title or username
    except (BadRequest, Forbidden, TimedOut, NetworkError) as exc:
        logger.info("گرفتن اطلاعات ربات %s ممکن نشد: %s", username, exc)
    set_status(res, res["status"], chat_id=username, chat_kind="bot", chat_title=title, bound_at=iso(utcnow()))
    text = f"✅ ربات «<b>{e(title)}</b>» (@{e(username)}) ثبت شد و به رزرو {fa_day_full(res['day'])} شما متصل شد."
    if res["status"] == "approved":
        text += "\nدر روز مقرر به‌صورت خودکار وارد جوین اجباری می‌شود."
    elif res["status"] == "pending_review":
        text += "\nپس از تایید فیش توسط مالک، در روز مقرر وارد جوین اجباری می‌شود."
    else:
        text += "\nفیش پرداخت را هم ارسال کنید."
    await safe_send(bot, res["user_id"], text)
    await notify_admins(
        bot,
        f"🔗 ربات @{e(username)} برای رزرو {user_label(res['user_id'])} "
        f"({fa_day_full(res['day'])}، {fmt_hours(res['hours'])}) ثبت شد.",
    )
    await activate_due(bot)


_CHAT_REF_RE = re.compile(r"^(@[A-Za-z0-9_]{5,32}|(?:https?://)?(?:t|telegram)\.me/\S+|-100\d{5,})$", re.IGNORECASE)


def looks_like_chat_ref(text: str) -> bool:
    return bool(_CHAT_REF_RE.match((text or "").strip()))


async def bind_chat(bot, res: dict, chat_id: int, chat_type: str, title: str, source_note: str = "") -> None:
    kind = "channel" if chat_type == Chat.CHANNEL else "group"
    set_status(
        res, res["status"], chat_id=str(chat_id), chat_kind=kind, chat_title=title, bound_at=iso(utcnow())
    )
    label = "کانال" if kind == "channel" else "گروه"
    text = f"✅ {label} «<b>{e(title)}</b>» شناسایی شد و به رزرو {fa_day_full(res['day'])} شما متصل شد."
    if res["status"] == "approved":
        text += "\nدر روز مقرر به‌صورت خودکار وارد جوین اجباری می‌شود."
    elif res["status"] == "pending_review":
        text += "\nپس از تایید فیش توسط مالک، در روز مقرر وارد جوین اجباری می‌شود."
    else:
        text += "\nفیش پرداخت را هم ارسال کنید."
    await safe_send(bot, res["user_id"], text)
    await notify_admins(
        bot,
        f"🔗 {label} «<b>{e(title)}</b>» برای رزرو {user_label(res['user_id'])} "
        f"({fa_day_full(res['day'])}، {fmt_hours(res['hours'])}) متصل شد.",
    )
    await activate_due(bot)


async def _try_bind_by_ref(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> bool:
    user = update.effective_user
    res = find_unbound(user.id, "chat")
    if res is None:
        return False
    ref, private_link = core.parse_chat_ref(text)
    if ref is None:
        await reply(
            update,
            "لینک دعوت خصوصی قابل شناسایی نیست. ربات را با دکمه‌ی «افزودن» ادمین کنید یا آیدی عددی/یوزرنیم را بفرستید.",
        )
        return True
    try:
        chat = await context.bot.get_chat(ref)
    except (BadRequest, Forbidden) as exc:
        logger.info("get_chat برای اتصال رزرو ناموفق: %s", exc)
        await reply(update, "گروه/کانال پیدا نشد. اول ربات را ادمین آن کنید، بعد دوباره بفرستید.")
        return True
    if chat.type not in (Chat.GROUP, Chat.SUPERGROUP, Chat.CHANNEL):
        await reply(update, "این یک گروه یا کانال نیست.")
        return True
    if chat.id == await core.get_group_id():
        await reply(update, "این همان گروه اصلی است و قابل فروش نیست.")
        return True
    try:
        bot_member = await context.bot.get_chat_member(chat.id, context.bot.id)
        user_member = await context.bot.get_chat_member(chat.id, user.id)
    except (BadRequest, Forbidden) as exc:
        logger.info("بررسی ادمین بودن برای اتصال رزرو ناموفق: %s", exc)
        await reply(update, "ربات باید ادمین آن گروه/کانال باشد. ابتدا ادمین کنید و دوباره بفرستید.")
        return True
    admin_states = (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    if bot_member.status not in admin_states:
        await reply(update, "ربات باید ادمین آن گروه/کانال باشد. ابتدا ادمین کنید و دوباره بفرستید.")
        return True
    if user_member.status not in admin_states:
        await reply(update, "فقط ادمین یا مالک آن گروه/کانال می‌تواند آن را ثبت کند.")
        return True
    await bind_chat(context.bot, res, chat.id, chat.type, chat.title or str(chat.id))
    return True


async def _receive_receipt(update: Update, context: ContextTypes.DEFAULT_TYPE, res: dict) -> None:
    message = update.effective_message
    user = update.effective_user
    now = utcnow()

    if res["status"] != "awaiting_payment" or not _holds_lane(res, now):
        if res["status"] == "awaiting_payment":
            set_status(res, "expired")
        await _show_menu(update, "⌛️ مهلت این رزرو تمام شده است. دوباره از «خرید جوین» رزرو کنید.")
        return

    if message.photo:
        rtype, file_id, rtext = "photo", message.photo[-1].file_id, message.caption
    elif message.document:
        rtype, file_id, rtext = "document", message.document.file_id, message.caption
    elif message.text:
        rtype, file_id, rtext = "text", None, message.text
    else:
        await reply(update, "فقط عکس فیش، فایل یا متن (کد پیگیری) قابل قبول است.")
        return

    pay = {
        "id": new_id(),
        "reservation_id": res["id"],
        "user_id": user.id,
        "amount": res["amount"],
        "hours": res["hours"],
        "receipt_type": rtype,
        "receipt_file_id": file_id,
        "receipt_text": (rtext or "")[:1000] or None,
        "receipt_chat_id": message.chat_id,
        "receipt_message_id": message.message_id,
        "status": "pending",
        "created_at": iso(now),
        "reviewed_at": None,
        "reviewed_by": None,
    }

    info = (
        "💳 <b>فیش پرداخت جدید</b>\n\n"
        f"👤 خریدار: {mention_html(user)}"
        f"{' (@' + e(user.username) + ')' if user.username else ''} — <code>{user.id}</code>\n"
        f"🏷 نوع: {TYPE_LABEL[res_kind(res)]}\n"
        f"📅 روز جوین: {fa_day_full(res['day'])}\n"
        f"⏱ مدت: {fmt_hours(res['hours'])}\n"
        f"💰 مبلغ: <b>{fmt_money(res['amount'])}</b>"
    )
    if res.get("chat_title"):
        info += f"\n📌 {e(res['chat_title'])}"
    markup = InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("✅ تایید", callback_data=f"sale_ok_{res['id']}", style="success"),
            InlineKeyboardButton("❌ رد", callback_data=f"sale_no_{res['id']}", style="danger"),
        ]]
    )

    delivered = []
    for admin_id in core.ADMIN_IDS:
        copied = None
        try:
            copied = await context.bot.copy_message(
                chat_id=admin_id, from_chat_id=message.chat_id, message_id=message.message_id
            )
        except TelegramError as exc:
            logger.warning("کپی فیش برای ادمین %s ناموفق: %s", admin_id, exc)
        kwargs = {"reply_markup": markup}
        if copied is not None:
            kwargs["reply_to_message_id"] = copied.message_id
        sent = await safe_send(context.bot, admin_id, info, **kwargs)
        if sent is None and copied is not None:
            sent = await safe_send(context.bot, admin_id, info, reply_markup=markup)
        if sent is not None:
            delivered.append((admin_id, sent.message_id))

    if not delivered:
        await reply(update, "❌ ارسال فیش برای مالک ناموفق بود. چند دقیقه بعد دوباره فیش را بفرستید.")
        return

    PAY[pay["id"]] = pay
    REVIEW_MSGS[res["id"]] = delivered
    set_status(res, "pending_review", payment_id=pay["id"], hold_expires_at=None)
    mark_dirty("sale_payments", pay)
    await flush()  # پرداخت بلافاصله در دیتابیس ثبت می‌شود

    BUYER[user.id] = {"step": "menu"}
    text = "✅ فیش شما دریافت شد و برای مالک ارسال شد. بعد از تایید، اطلاع می‌دهیم."
    if not res.get("chat_id") and res_kind(res) == "bot":
        text += "\n\nحالا <b>یوزرنیم ربات</b> خود را بفرستید (مثال: <code>@MyBot</code>) تا به رزرو شما متصل شود."
        await reply(update, text, reply_markup=buyer_menu_kb())
    elif not res.get("chat_id"):
        text += (
            "\n\nدر همین فاصله، ربات را با دکمه‌های زیر ادمین گروه/کانال خودتان کنید "
            "تا خودکار شناسایی و به رزرو شما متصل شود."
        )
        await reply(update, text, reply_markup=add_bot_markup(context.bot.username or ""))
        await reply(update, "منوی اصلی 👇", reply_markup=buyer_menu_kb())
    else:
        await reply(update, text, reply_markup=buyer_menu_kb())


async def buyer_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """هر پیام خصوصی کاربر غیرادمین."""
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None or user.is_bot:
        return
    touch_user(user)
    if not sale_enabled():
        return

    text = (message.text or "").strip()
    state = _state(user.id)

    if text == BTN_BUY:
        await _start_buy(update)
        return
    if text == BTN_MY:
        await _show_my(update)
        return
    if text == BTN_CANCEL:
        await _cancel_hold(update)
        return
    if text == BTN_BACK:
        await _show_menu(update)
        return

    step = state.get("step")
    if step == "type":
        kinds = available_kinds()
        if text == BTN_TYPE_CHAT and "chat" in kinds:
            await _show_days(update, "chat")
        elif text == BTN_TYPE_BOT and "bot" in kinds:
            await _show_days(update, "bot")
        else:
            await _show_types(update)
        return
    if step == "days":
        await _pick_day(update, state, text)
        return
    if step == "tariffs":
        await _pick_tariff(update, state, text)
        return

    hold = open_hold(user.id)

    if text:
        bot_res = find_unbound(user.id, "bot")
        bot_name = extract_bot_username(text) if bot_res is not None else None
        if bot_res is not None and bot_name:
            await bind_bot_username(context.bot, bot_res, bot_name)
            return
        if looks_like_chat_ref(text) and find_unbound(user.id, "chat") is not None:
            if await _try_bind_by_ref(update, context, text):
                return

    if hold is not None and (step == "receipt" or message.photo or message.document):
        await _receive_receipt(update, context, hold)
        return
    if hold is not None:
        await reply(update, "برای ادامه‌ی پرداخت «🛒 خرید جوین» را بزنید یا رزرو را لغو کنید.", reply_markup=buyer_menu_kb())
        return

    pending = [r for r in RES.values() if r["user_id"] == user.id and r["status"] == "pending_review"]
    if pending and (message.photo or message.document):
        await reply(update, "فیش شما قبلاً دریافت شده و در حال بررسی است ⏳")
        return

    await _show_menu(update, "از دکمه‌های زیر استفاده کنید 👇")


# ---------------------------------------------------------------------------
# بررسی فیش توسط مالک
# ---------------------------------------------------------------------------

REVIEW_MSGS: dict = {}   # reservation_id -> [(admin_chat_id, message_id)]


async def _close_review_messages(bot, res_id: int, note: str, clicked_message=None) -> None:
    targets = list(REVIEW_MSGS.pop(res_id, []))
    if clicked_message is not None and not any(
        cid == clicked_message.chat_id and mid == clicked_message.message_id for cid, mid in targets
    ):
        targets.append((clicked_message.chat_id, clicked_message.message_id))
    for chat_id, message_id in targets:
        try:
            if clicked_message is not None and chat_id == clicked_message.chat_id and message_id == clicked_message.message_id:
                base = clicked_message.text_html or ""
            else:
                base = ""
            if base:
                await bot.edit_message_text(
                    chat_id=chat_id, message_id=message_id, text=f"{base}\n\n{note}", parse_mode=ParseMode.HTML
                )
            else:
                await bot.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=None)
                await safe_send(bot, chat_id, note, reply_to_message_id=message_id)
        except (BadRequest, Forbidden, TimedOut, NetworkError) as exc:
            logger.info("به‌روزرسانی پیام بررسی فیش ممکن نشد: %s", exc)


async def review_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    admin = query.from_user
    if not core.is_admin(admin.id):
        await query.answer("شما ادمین ربات نیستید.", show_alert=True)
        return
    m = re.match(r"^sale_(ok|no)_(\d+)$", query.data or "")
    if not m:
        await query.answer()
        return
    approve = m.group(1) == "ok"
    res = RES.get(int(m.group(2)))
    if res is None or res["status"] != "pending_review":
        await query.answer("این فیش قبلاً بررسی شده است.", show_alert=True)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except BadRequest:
            pass
        return

    # از همین‌جا تا تغییر وضعیت هیچ await وجود ندارد؛ پس بررسی دوبار (دو ادمین هم‌زمان) ممکن نیست.
    now = utcnow()
    pay = PAY.get(res.get("payment_id") or 0)
    if approve:
        set_status(res, "approved", approved_at=iso(now))
    else:
        set_status(res, "rejected")
    if pay is not None:
        pay.update({"status": "approved" if approve else "rejected", "reviewed_at": iso(now), "reviewed_by": admin.id})
        mark_dirty("sale_payments", pay)
    await query.answer("تایید شد ✅" if approve else "رد شد ❌")
    await flush()  # تصمیم نهایی بلافاصله در دیتابیس ثبت می‌شود

    who = e(admin.full_name or admin.id)
    note = f"✅ تایید شد توسط {who}" if approve else f"❌ رد شد توسط {who}"
    await _close_review_messages(context.bot, res["id"], note, clicked_message=query.message)

    buyer_id = res["user_id"]
    if not approve:
        await safe_send(
            context.bot,
            buyer_id,
            "❌ <b>فیش شما تایید نشد.</b>\nدر صورت پرداخت صحیح، دوباره از «خرید جوین» رزرو کنید و فیش معتبر ارسال نمایید.",
            reply_markup=buyer_menu_kb(),
        )
        return

    if res.get("chat_id"):
        text = (
            f"✅ <b>فیش شما تایید شد.</b>\n«{e(res.get('chat_title'))}» در تاریخ {fa_day_full(res['day'])} "
            f"به‌صورت خودکار وارد جوین اجباری می‌شود."
        )
        await safe_send(context.bot, buyer_id, text, reply_markup=buyer_menu_kb())
    elif res_kind(res) == "bot":
        await safe_send(
            context.bot,
            buyer_id,
            "✅ <b>فیش شما تایید شد.</b>\nحالا <b>یوزرنیم ربات</b> خود را بفرستید (مثال: <code>@MyBot</code>) "
            f"تا به رزرو {fa_day_full(res['day'])} شما متصل شود.",
            reply_markup=buyer_menu_kb(),
        )
    else:
        text = (
            f"✅ <b>فیش شما تایید شد.</b>\n"
            f"حالا ربات @{context.bot.username} را با دکمه‌های زیر ادمین گروه/کانال خودتان کنید. "
            "ربات خودکار آن را تشخیص می‌دهد و به رزرو "
            f"{fa_day_full(res['day'])} شما متصل می‌کند."
        )
        await safe_send(context.bot, buyer_id, text, reply_markup=add_bot_markup(context.bot.username or ""))
    await activate_due(context.bot)


# ---------------------------------------------------------------------------
# شناسایی خودکار گروه/کانال (وقتی ربات ادمین می‌شود)
# ---------------------------------------------------------------------------


async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cmu = update.my_chat_member
    if cmu is None or cmu.chat.type == Chat.PRIVATE:
        return
    if cmu.new_chat_member.status != ChatMemberStatus.ADMINISTRATOR:
        return
    adder = cmu.from_user
    if adder is None or adder.is_bot:
        return
    if cmu.chat.id == await core.get_group_id():
        return
    res = find_unbound(adder.id, "chat")
    if res is None:
        return
    chat = cmu.chat
    await bind_chat(context.bot, res, chat.id, chat.type, chat.title or str(chat.id))
    can_invite = getattr(cmu.new_chat_member, "can_invite_users", True)
    if not can_invite and not chat.username:
        await safe_send(
            context.bot,
            res["user_id"],
            "⚠️ ربات دسترسی «دعوت کاربران از طریق لینک» را ندارد. برای ساخت لینک جوین، این دسترسی را برای ربات فعال کنید.",
        )


# ---------------------------------------------------------------------------
# فعال‌سازی، پایان و گزارش
# ---------------------------------------------------------------------------

_ACTIVATING: set = set()
_NEXT_TRY: dict = {}
_FAIL_NOTIFIED: set = set()
_REPORT_TRIES: dict = {}


def _upsert_item_sync(payload: dict) -> None:
    table = core.supabase.table("channels")
    try:
        table.upsert(payload, on_conflict="chat_id").execute()
    except Exception:  # noqa: BLE001
        legacy = {k: v for k, v in payload.items() if k not in ("count_from", "report_sent")}  # دیتابیس migrate نشده
        table.upsert(legacy, on_conflict="chat_id").execute()


async def _activation_failed(bot, res: dict, reason: str) -> None:
    _NEXT_TRY[res["id"]] = time.monotonic() + RETRY_ACTIVATION_SECONDS
    logger.warning("فعال‌سازی رزرو %s ناموفق: %s", res["id"], reason)
    if res["id"] in _FAIL_NOTIFIED:
        return
    _FAIL_NOTIFIED.add(res["id"])
    await safe_send(
        bot,
        res["user_id"],
        f"⚠️ فعال‌سازی جوین اجباری «{e(res.get('chat_title'))}» انجام نشد:\n{e(reason)}\n"
        "پس از رفع مشکل، خودکار دوباره تلاش می‌شود.",
    )
    await notify_admins(
        bot,
        f"⚠️ فعال‌سازی رزرو {user_label(res['user_id'])} ({fa_day_full(res['day'])}) ناموفق:\n{e(reason)}",
    )


async def _activate(bot, res: dict) -> None:
    rid = res["id"]
    if rid in _ACTIVATING or time.monotonic() < _NEXT_TRY.get(rid, 0):
        return
    _ACTIVATING.add(rid)
    try:
        chat_key = str(res["chat_id"])

        existing = await core.get_item_by_chat_id(chat_key)
        if existing is not None and not core.item_is_expired(existing):
            await _activation_failed(bot, res, "این مورد هم‌اکنون در لیست جوین اجباری فعال است؛ بعد از پایان آن شروع می‌شود.")
            return

        if res_kind(res) == "bot":
            kind = "bot"
            title = res.get("chat_title") or chat_key
            link = f"https://t.me/{chat_key}?start=verify_join"
            baseline = await core.count_bot_starts(chat_key)
        else:
            chat_id = int(chat_key)
            admin_states = (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
            try:
                me = await bot.get_chat_member(chat_id, bot.id)
                chat = await bot.get_chat(chat_id)
            except (BadRequest, Forbidden) as exc:
                await _activation_failed(bot, res, f"دسترسی ربات به گروه/کانال ممکن نیست ({exc}). ربات را دوباره ادمین کنید.")
                return
            if me.status not in admin_states:
                await _activation_failed(bot, res, "ربات دیگر ادمین این گروه/کانال نیست.")
                return
            kind = "channel" if chat.type == Chat.CHANNEL else "group"
            title = chat.title or res.get("chat_title") or chat_key
            link = f"https://t.me/{chat.username}" if chat.username else chat.invite_link
            if not link:
                try:
                    link = await bot.export_chat_invite_link(chat_id)
                except (BadRequest, Forbidden) as exc:
                    await _activation_failed(bot, res, f"ساخت لینک دعوت ممکن نشد؛ دسترسی «دعوت کاربران» را به ربات بدهید ({exc}).")
                    return
            try:
                baseline = await bot.get_chat_member_count(chat_id)
            except (BadRequest, Forbidden, TimedOut, NetworkError):
                baseline = None

        now = utcnow()
        expires = now + timedelta(hours=res["hours"])
        payload = {
            "kind": kind,
            "chat_id": chat_key,
            "title": title,
            "invite_link": link,
            "expires_at": expires.isoformat(),
            "start_members": baseline,
            "peak_members": 0,
            "quota_done": False,
            "member_limit": 0,
            "count_from": now.isoformat(),
            "report_sent": False,
        }
        try:
            await asyncio.to_thread(_upsert_item_sync, payload)
        except Exception as exc:  # noqa: BLE001
            await _activation_failed(bot, res, f"ذخیره‌ی آیتم در دیتابیس ناموفق بود: {exc}")
            return
        core._cache_clear()
        core._member_count_cache.clear()
        item = await core.get_item_by_chat_id(chat_key)
        core._attr_count_cache.pop(item["id"] if item else None, None)

        set_status(
            res, "active",
            item_id=item["id"] if item else None,
            chat_kind="bot" if kind == "bot" else kind,
            chat_title=title,
            activated_at=iso(now),
            expires_at=iso(expires),
        )
        _FAIL_NOTIFIED.discard(rid)
        _NEXT_TRY.pop(rid, None)
        await flush()
        await safe_send(
            bot,
            res["user_id"],
            f"🟢 <b>جوین اجباری «{e(title)}» فعال شد.</b>\n"
            f"⏱ مدت: {fmt_hours(res['hours'])}\n🕓 پایان: {fa_datetime(expires)}\n\n"
            "بعد از پایان، عکس قبل و بعد را برایتان می‌فرستم.",
        )
        await notify_admins(
            bot,
            f"🟢 جوین فروخته‌شده فعال شد: «{e(title)}» — {user_label(res['user_id'])} "
            f"({fmt_hours(res['hours'])}، تا {fa_datetime(expires)})",
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("خطای پیش‌بینی‌نشده در فعال‌سازی رزرو %s: %s", rid, exc, exc_info=exc)
        _NEXT_TRY[rid] = time.monotonic() + 60
    finally:
        _ACTIVATING.discard(rid)


async def activate_due(bot) -> None:
    now = utcnow()
    due = [
        r for r in RES.values()
        if r["status"] == "approved" and r.get("chat_id") and day_start_utc(r["day"]) <= now
    ]
    for res in due:
        await _activate(bot, res)


async def _expire_holds(bot) -> None:
    now = utcnow()
    for res in list(RES.values()):
        if res["status"] != "awaiting_payment":
            continue
        hold = core._parse_ts(res.get("hold_expires_at"))
        if hold is not None and hold <= now:
            set_status(res, "expired")
            if BUYER.get(res["user_id"], {}).get("res_id") == res["id"]:
                BUYER[res["user_id"]] = {"step": "menu"}
            await safe_send(
                bot, res["user_id"],
                "⌛️ مهلت پرداخت رزرو شما تمام شد و لاین آزاد شد. برای رزرو دوباره، «خرید جوین» را بزنید.",
                reply_markup=buyer_menu_kb(),
            )


async def _finalize_active() -> None:
    for res in list(RES.values()):
        if res["status"] != "active":
            continue
        item = await core.get_item(res["item_id"]) if res.get("item_id") else None
        if item is None or core.item_is_expired(item):
            set_status(res, "finished", finished_at=iso(utcnow()))


async def _send_report(bot, res: dict) -> None:
    item = await core.get_item(res["item_id"]) if res.get("item_id") else None
    if item is None:
        set_status(res, "finished", report_sent=True)
        return
    png = await core.build_item_report_png(bot, item)
    joined = await core.fetch_member_count(bot, item, force=True) or 0
    caption = (
        "📊 <b>گزارش پایان جوین اجباری</b>\n"
        f"📌 {e(item['title'])}\n"
        f"✅ ممبر جذب‌شده از طریق ربات: <b>{joined:,}</b>\n"
        "🏁 زمان جوین اجباری شما به پایان رسید. از اعتماد شما سپاسگزاریم 🙏"
    )
    await bot.send_photo(chat_id=res["user_id"], photo=io.BytesIO(png), caption=caption, parse_mode=ParseMode.HTML)
    set_status(res, "finished", report_sent=True)
    await flush()


async def _send_pending_reports(bot) -> None:
    for res in list(RES.values()):
        if res["status"] != "finished" or res.get("report_sent"):
            continue
        tries = _REPORT_TRIES.get(res["id"], 0)
        if tries >= MAX_REPORT_TRIES:
            set_status(res, "finished", report_sent=True)
            continue
        _REPORT_TRIES[res["id"]] = tries + 1
        try:
            await _send_report(bot, res)
        except RetryAfter as exc:
            await asyncio.sleep(_retry_seconds(exc) + 0.2)
        except Exception as exc:  # noqa: BLE001
            logger.warning("ارسال گزارش تصویری به خریدار %s ناموفق (تلاش %d): %s", res["user_id"], tries + 1, exc)


async def sweep(bot) -> None:
    """هر ~۳۰ ثانیه از حلقه‌ی پس‌زمینه‌ی اصلی صدا زده می‌شود."""
    for step in (lambda: _expire_holds(bot), lambda: activate_due(bot), _finalize_active, lambda: _send_pending_reports(bot)):
        try:
            await step()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error("خطا در sweep فروش: %s", exc, exc_info=exc)


# ---------------------------------------------------------------------------
# پنل مالک: تنظیم تعرفه و روشن/خاموش
# ---------------------------------------------------------------------------

CFG_STATE: dict = {}   # admin_id -> {"step": "tariffs"|"card"|"lanes"|"confirm", ...}


def clear_admin_state(user_id: int) -> None:
    CFG_STATE.pop(user_id, None)


def _cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ انصراف", callback_data="sale_cfg_cancel", style="danger")]])


def _tariff_lines(title: str, tariffs: list) -> list:
    if not tariffs:
        return []
    return [f"\n<b>{title}:</b>"] + [f"• {fmt_hours(t['hours'])} = {fmt_money(t['price'])}" for t in tariffs]


def sale_panel_text(schema_err: Optional[str] = None) -> str:
    lines = ["💰 <b>فروش جوین اجباری خودکار</b>", ""]
    lines.append(f"وضعیت: {'🟢 روشن' if CFG['enabled'] else '🔴 خاموش'}")
    lines += _tariff_lines("تعرفه‌ی کانال و گروه", CFG["tariffs"])
    lines += _tariff_lines("تعرفه‌ی ربات", CFG["bot_tariffs"])
    if CFG["card_number"]:
        lines.append(f"\n💳 <code>{fmt_card(CFG['card_number'])}</code> — {e(CFG['card_name'])}")
    if CFG["lanes"]:
        lines.append(f"🛣 لاین در هر ۲۴ ساعت: {to_fa(CFG['lanes'])}")
        lines.append("ℹ️ آیتم‌هایی که خودتان دستی اضافه می‌کنید هم از همین لاین‌ها کم می‌شود (ولی دستی همیشه قابل افزودن است).")
    if not config_complete():
        lines.append("\n⚠️ برای شروع فروش، ابتدا «تنظیم تعرفه» را کامل کنید.")
    if schema_err:
        lines.append(
            "\n❌ جدول‌های فروش در Supabase ساخته نشده‌اند. فایل <code>supabase_schema.sql</code> را در SQL Editor اجرا کنید.\n"
            f"<code>{e(schema_err[:200])}</code>"
        )
    return "\n".join(lines)


def sale_panel_kb() -> InlineKeyboardMarkup:
    toggle = (
        InlineKeyboardButton("🔴 خاموش کردن فروش", callback_data="sale_off", style="danger")
        if CFG["enabled"]
        else InlineKeyboardButton("🟢 روشن کردن فروش", callback_data="sale_on", style="success")
    )
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("1️⃣ تنظیم تعرفه", callback_data="sale_cfg", style="primary")],
            [InlineKeyboardButton("📅 رزروها", callback_data="sale_list", style="primary")],
            [toggle],
            [InlineKeyboardButton("🔙 بازگشت", callback_data="st_main")],
        ]
    )


def _summary_text(draft: dict) -> str:
    lines = ["📝 <b>خلاصه‌ی تنظیمات فروش</b>"]
    lines += _tariff_lines("تعرفه‌ی کانال و گروه", draft["tariffs"]) or ["\nتعرفه‌ی کانال و گروه: ندارد"]
    lines += _tariff_lines("تعرفه‌ی ربات", draft["bot_tariffs"]) or ["\nتعرفه‌ی ربات: ندارد"]
    lines.append(f"\n💳 <code>{fmt_card(draft['card_number'])}</code>\n👤 {e(draft['card_name'])}")
    lines.append(f"🛣 لاین در هر ۲۴ ساعت: {to_fa(draft['lanes'])}")
    lines.append("\nبرای ذخیره روی «ثبت» بزنید.")
    return "\n".join(lines)


TARIFF_PROMPT = (
    "💰 <b>تنظیم تعرفه — مرحله ۱ از ۴</b>\n\n"
    "اول تعرفه‌ی <b>کانال و گروه</b> را بفرستید؛ هر خط یک تعرفه، به این شکل:\n"
    "<code>ساعت = مبلغ به تومان</code>\n\n"
    "مثال:\n<code>12 = 25000\n48 = 50000</code>\n\n"
    "(ساعت و مبلغ هر چیزی که بخواهید.) اگر کانال/گروه نمی‌فروشید فقط <code>-</code> بفرستید."
)
BOT_TARIFF_PROMPT = (
    "🤖 <b>تنظیم تعرفه — مرحله ۲ از ۴</b>\n\n"
    "حالا تعرفه‌ی ساعتیِ <b>ربات</b> را با همان فرمت بفرستید:\n"
    "<code>12 = 15000\n48 = 30000</code>\n\n"
    "اگر ربات نمی‌فروشید فقط <code>-</code> بفرستید."
)
CARD_PROMPT = (
    "💳 <b>مرحله ۳ از ۴</b>\n\n"
    "<b>شماره کارت</b> را همراه با <b>نام و نام خانوادگی</b> صاحب کارت بفرستید.\n\n"
    "مثال:\n<code>6037991234567890\nعلی رضایی</code>"
)
LANES_PROMPT = (
    "🛣 <b>مرحله ۴ از ۴ — تعداد لاین هر ۲۴ ساعت</b>\n\n"
    "یعنی در یک روز چند کانال/گروه/ربات می‌توانند وارد جوین اجباری شوند.\n"
    "اگر مثلاً ۳ لاین بگذارید و خودتان ۲ تا را دستی اضافه کرده باشید، آن روز فقط ۱ فروش خودکار ممکن است؛ "
    "وقتی لاین‌ها (دستی یا خودکار) پر شوند، آن روز برای خریدار قرمز می‌شود ولی شما همچنان می‌توانید دستی اضافه کنید.\n\n"
    "مثال: <code>3</code>"
)


async def sale_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    data = query.data or ""
    if re.match(r"^sale_(ok|no)_\d+$", data):
        await review_callback(update, context)
        return

    user_id = query.from_user.id
    if not core.is_admin(user_id):
        await query.answer("شما ادمین ربات نیستید.", show_alert=True)
        return
    await query.answer()

    async def show(text: str, markup=None) -> None:
        try:
            await query.edit_message_text(
                text, parse_mode=ParseMode.HTML, reply_markup=markup, link_preview_options=NO_PREVIEW
            )
        except BadRequest as exc:
            if "not modified" not in str(exc).lower():
                raise

    if data == "sale_open":
        CFG_STATE.pop(user_id, None)
        await show(sale_panel_text(await schema_error()), sale_panel_kb())

    elif data == "sale_on":
        err = await schema_error()
        if err:
            await show(sale_panel_text(err), sale_panel_kb())
            return
        CFG["enabled"] = True
        errors = await persist_config("enabled")
        text = sale_panel_text()
        if errors:
            text += f"\n\n❌ ذخیره در دیتابیس ناموفق بود:\n<code>{e(errors[0][:200])}</code>"
        else:
            text = "🟢 <b>فروش جوین اجباری روشن شد.</b>\n\n" + text
        await show(text, sale_panel_kb())

    elif data == "sale_off":
        CFG["enabled"] = False
        errors = await persist_config("enabled")
        text = "🔴 <b>فروش جوین اجباری خاموش شد.</b>\n\n" + sale_panel_text()
        if errors:
            text += f"\n\n❌ ذخیره در دیتابیس ناموفق بود:\n<code>{e(errors[0][:200])}</code>"
        await show(text, sale_panel_kb())

    elif data == "sale_cfg":
        CFG_STATE[user_id] = {"step": "tariffs"}
        await show(TARIFF_PROMPT, _cancel_kb())

    elif data == "sale_cfg_cancel":
        CFG_STATE.pop(user_id, None)
        await show("تنظیمات لغو شد.\n\n" + sale_panel_text(), sale_panel_kb())

    elif data == "sale_cfg_save":
        state = CFG_STATE.get(user_id)
        if not state or state.get("step") != "confirm":
            await show(sale_panel_text(), sale_panel_kb())
            return
        draft = state["draft"]
        CFG_STATE.pop(user_id, None)
        CFG["tariffs"] = draft["tariffs"]
        CFG["bot_tariffs"] = draft["bot_tariffs"]
        CFG["card_number"] = draft["card_number"]
        CFG["card_name"] = draft["card_name"]
        CFG["lanes"] = draft["lanes"]
        errors = await persist_config("tariffs", "bot_tariffs", "card", "lanes")
        if errors:
            await show(
                f"❌ ذخیره در دیتابیس ناموفق بود (تا ری‌استارت سرور اعمال می‌ماند):\n<code>{e(errors[0][:200])}</code>",
                sale_panel_kb(),
            )
            return
        await show("✅ <b>تعرفه ثبت شد.</b>\n\n" + sale_panel_text(), sale_panel_kb())

    elif data == "sale_list":
        rows = [r for r in RES.values() if r["status"] in OPEN_STATUSES]
        rows.sort(key=lambda r: (r["day"], r.get("created_at") or ""))
        if not rows:
            await show("📅 هیچ رزرو بازی وجود ندارد.", InlineKeyboardMarkup([[InlineKeyboardButton("🔙 بازگشت", callback_data="sale_open")]]))
            return
        lines = ["📅 <b>رزروهای باز</b>", ""]
        for r in rows[:25]:
            lines.append(
                f"• {TYPE_LABEL[res_kind(r)]} — {fa_day_full(r['day'])} — {fmt_hours(r['hours'])} — {user_label(r['user_id'])}\n"
                f"   {STATUS_LABEL.get(r['status'], r['status'])}"
                + (f" — {e(r['chat_title'])}" if r.get("chat_title") else "")
            )
        if len(rows) > 25:
            lines.append(f"\nو {len(rows) - 25} مورد دیگر…")
        await show("\n".join(lines), InlineKeyboardMarkup([[InlineKeyboardButton("🔙 بازگشت", callback_data="sale_open")]]))


def _is_skip(text: str) -> bool:
    return (text or "").strip() in SKIP_WORDS


async def admin_sale_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """پیام‌های متنی مراحل تنظیم تعرفه. اگر پیام مصرف شد True برمی‌گرداند."""
    user_id = update.effective_user.id
    state = CFG_STATE.get(user_id)
    if state is None:
        return False
    message = update.effective_message
    text = (message.text or "").strip()
    step = state["step"]

    if step in ("tariffs", "bot_tariffs"):
        if _is_skip(text):
            tariffs, bad = [], []
        else:
            tariffs, bad = parse_tariffs(text)
            if not tariffs and not bad:
                bad = ["(خالی)"]
        if bad:
            detail = "\n".join(f"• <code>{e(b)}</code>" for b in bad[:8])
            await reply(
                update,
                "❌ فرمت تعرفه درست نیست.\n\nخطوط نامعتبر:\n" + detail +
                "\n\nهر خط باید مثل <code>12 = 25000</code> باشد (یا <code>-</code> برای «ندارم»). دوباره بفرستید.",
                reply_markup=_cancel_kb(),
            )
            return True
        if step == "tariffs":
            state["draft"] = {"tariffs": tariffs}
            state["step"] = "bot_tariffs"
            await reply(update, BOT_TARIFF_PROMPT, reply_markup=_cancel_kb())
            return True
        state["draft"]["bot_tariffs"] = tariffs
        if not state["draft"]["tariffs"] and not tariffs:
            state["step"] = "tariffs"
            await reply(
                update,
                "❌ حداقل یکی از دو تعرفه (کانال/گروه یا ربات) باید ثبت شود. از اول شروع می‌کنیم.\n\n" + TARIFF_PROMPT,
                reply_markup=_cancel_kb(),
            )
            return True
        state["step"] = "card"
        await reply(update, CARD_PROMPT, reply_markup=_cancel_kb())
        return True

    if step == "card":
        parsed = parse_card(text)
        if parsed is None:
            await reply(
                update,
                "❌ شماره کارت ۱۶ رقمی و نام صاحب کارت را با هم بفرستید.\nمثال:\n<code>6037991234567890\nعلی رضایی</code>",
                reply_markup=_cancel_kb(),
            )
            return True
        state["draft"]["card_number"], state["draft"]["card_name"] = parsed
        state["step"] = "lanes"
        await reply(update, LANES_PROMPT, reply_markup=_cancel_kb())
        return True

    if step == "lanes":
        digits = norm_digits(text).strip()
        if not digits.isdigit() or not 1 <= int(digits) <= MAX_LANES:
            await reply(update, f"❌ یک عدد بین 1 تا {MAX_LANES} بفرستید.", reply_markup=_cancel_kb())
            return True
        state["draft"]["lanes"] = int(digits)
        state["step"] = "confirm"
        markup = InlineKeyboardMarkup(
            [[
                InlineKeyboardButton("✅ ثبت", callback_data="sale_cfg_save", style="success"),
                InlineKeyboardButton("❌ انصراف", callback_data="sale_cfg_cancel", style="danger"),
            ]]
        )
        await reply(update, _summary_text(state["draft"]), reply_markup=markup)
        return True

    await reply(update, "برای ثبت یا انصراف از دکمه‌های پیام بالا استفاده کنید.")
    return True


# ---------------------------------------------------------------------------
# راه‌اندازی / خاموشی
# ---------------------------------------------------------------------------


async def on_startup(application) -> None:
    global _writer_task
    await load_config()
    await load_all()
    _writer_task = asyncio.create_task(writer_loop())
    logger.info("ماژول فروش آماده است (فروش %s).", "روشن" if CFG["enabled"] else "خاموش")


async def on_shutdown() -> None:
    if _writer_task is not None:
        _writer_task.cancel()
    await flush()
