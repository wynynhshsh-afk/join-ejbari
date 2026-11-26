"""
ساخت تصویر گزارش «مقایسه‌ی اعضای گروه (قبل و بعد)» روی قالب assets/report_template.png

- عکس پروفایل گروه/کانال داخل هر دو دایره
- نام دقیق گروه زیر هر دایره (فارسی/عربی با شکل‌دهی و راست‌به‌چپ، حروف تزئینی یونیکد، سمبل و ایموجی رنگی)
- تعداد ممبرهای جذب‌شده با رنگ سبز و درشت در بخش پایین

تابع اصلی: render_report(...) -> bytes (PNG). کاملاً سینک است؛ از asyncio.to_thread صدایش بزنید.
"""

import io
import os
from functools import lru_cache
from typing import Optional

import arabic_reshaper
import regex
from bidi.algorithm import get_display
from fontTools.ttLib import TTFont
from PIL import Image, ImageDraw, ImageFilter, ImageFont

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join(BASE_DIR, "assets", "report_template.png")
FONT_DIR = os.path.join(BASE_DIR, "fonts")

# ترتیب مهم است: برای هر حرف، اولین فونتی که آن را دارد انتخاب می‌شود
TEXT_FONT_FILES = [
    os.path.join(FONT_DIR, "NotoSans-Bold.ttf"),
    os.path.join(FONT_DIR, "NotoSansArabic-Bold.ttf"),
    os.path.join(FONT_DIR, "NotoSansMath-Regular.ttf"),
    os.path.join(FONT_DIR, "NotoSansSymbols-Bold.ttf"),
    os.path.join(FONT_DIR, "NotoSansSymbols2-Regular.ttf"),
    # فونت‌های چینی/ژاپنی/کره‌ای در صورت وجود روی سرور (اختیاری، حجمشان زیاد است و داخل پروژه نیستند)
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
]
EMOJI_FONT_FILE = os.path.join(FONT_DIR, "NotoColorEmoji.ttf")
EMOJI_BITMAP_SIZE = 109  # NotoColorEmoji فقط در همین اندازه‌ی bitmap رندر می‌شود

# هندسه‌ی قالب (پیکسل، روی تصویر 1672x941 - قالب جدید قرمز/سفید یوتیوبی)
CIRCLE_CENTERS = ((464, 373), (1206, 373))
CIRCLE_PASTE_RADIUS = 204

# دو کپسول قرمز بالای دایره‌ها: برچسب BEFORE / AFTER
TOP_BAR_CENTERS = ((442, 94), (1229, 93))
TOP_BAR_MAX_W = 360
TOP_BAR_MAX_H = 64

# کادر کوچک قرمزِ وسط: عدد ممبر جذب‌شده (+N)
JOINED_CENTER = (833, 620)
JOINED_MAX_WIDTH = 400
JOINED_MAX_HEIGHT = 74

# نوار سفید پایین: اسم گروه + (اختیاری) جمع اعضای قبل/بعد
NAME_CENTERS_X = (834,)  # فقط یک نوار مشترک؛ اسم یک‌بار وسط‌چین می‌شود
NAME_BASELINE_Y = 758
NAME_BASELINE_Y_WITH_CAPTION = 718
NAME_MAX_WIDTH = 820
CAPTION_BASELINE_Y = 812
CAPTION_MAX_WIDTH = 820
CAPTION_MAX_HEIGHT = 34

NAME_FILL = (183, 18, 18)
NAME_STROKE = (255, 255, 255)
GREEN = (18, 160, 68)
CAPTION_FILL = (110, 110, 122)
TOPBAR_FILL = (255, 255, 255)
TOPBAR_STROKE = (128, 0, 0)

_RTL_RE = regex.compile(r"[\u0590-\u08FF\uFB1D-\uFDFF\uFE70-\uFEFF]")
_HARAKAT_RE = regex.compile(r"[\u064B-\u065F\u0670\u06D6-\u06ED]")
_EMOJI_FORCE_RE = regex.compile(
    r"\p{Emoji_Presentation}|\uFE0F|\u200D|\p{Regional_Indicator}|\u20E3|[\U0001F3FB-\U0001F3FF]"
)
_PICTO_RE = regex.compile(r"\p{Extended_Pictographic}")

_reshaper = arabic_reshaper.ArabicReshaper(configuration={"delete_harakat": True, "support_ligatures": True})


@lru_cache(maxsize=None)
def _cmap(path: str) -> frozenset:
    try:
        font = TTFont(path, fontNumber=0, lazy=True)
        return frozenset(font.getBestCmap().keys())
    except Exception:  # noqa: BLE001
        return frozenset()


@lru_cache(maxsize=256)
def _font(path: str, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(path, size, layout_engine=ImageFont.Layout.BASIC)


def _available_text_fonts() -> list:
    return [p for p in TEXT_FONT_FILES if os.path.exists(p)]


def _pick_font(cluster: str) -> Optional[str]:
    base = cluster[0]
    for path in _available_text_fonts():
        if ord(base) in _cmap(path):
            return path
    return None


def _is_emoji_cluster(cluster: str) -> bool:
    if _EMOJI_FORCE_RE.search(cluster):
        return True
    if _PICTO_RE.search(cluster) and _pick_font(cluster) is None:
        return True
    return False


@lru_cache(maxsize=512)
def _emoji_image(cluster: str) -> Optional[Image.Image]:
    if not os.path.exists(EMOJI_FONT_FILE):
        return None
    try:
        font = ImageFont.truetype(EMOJI_FONT_FILE, EMOJI_BITMAP_SIZE, layout_engine=ImageFont.Layout.BASIC)
        canvas = Image.new("RGBA", (EMOJI_BITMAP_SIZE * 3, EMOJI_BITMAP_SIZE * 2), (0, 0, 0, 0))
        ImageDraw.Draw(canvas).text((10, 10), cluster, font=font, embedded_color=True)
        box = canvas.getbbox()
        if box is None:
            return None
        return canvas.crop(box)
    except Exception:  # noqa: BLE001
        return None


def _visual_text(text: str) -> str:
    """متن را به ترتیب نمایشی (چپ‌به‌راست روی تصویر) تبدیل می‌کند: شکل‌دهی عربی + bidi."""
    text = text.replace("\r", " ").replace("\n", " ")
    if not _RTL_RE.search(text):
        return text
    text = _HARAKAT_RE.sub("", text)
    return get_display(_reshaper.reshape(text))


def _layout(text: str, size: int) -> list:
    """لیست آیتم‌ها: (kind, payload, width). kind = 'text' | 'emoji' | 'space'."""
    items = []
    for cluster in regex.findall(r"\X", _visual_text(text)):
        if cluster.isspace():
            items.append(("space", cluster, size * 0.30))
            continue
        if _is_emoji_cluster(cluster):
            img = _emoji_image(cluster)
            if img is not None:
                height = size * 1.0
                width = img.width * height / img.height
                items.append(("emoji", (cluster, height), width))
                continue
        path = _pick_font(cluster)
        if path is None:
            continue  # حرفی که هیچ فونتی ندارد (مثلاً CJK بدون فونت CJK) نادیده گرفته می‌شود
        font = _font(path, size)
        items.append(("text", (cluster, path), font.getlength(cluster)))
    return items


def _items_width(items: list) -> float:
    return sum(w for _, _, w in items)


def _draw_items(img: Image.Image, items: list, left: float, baseline: int, size: int) -> None:
    draw = ImageDraw.Draw(img)
    x = left
    stroke = max(2, size // 16)
    for kind, payload, width in items:
        if kind == "text":
            cluster, path = payload
            font = _font(path, size)
            draw.text((x + 2, baseline + 3), cluster, font=font, fill=(0, 0, 0), anchor="ls")
            draw.text((x, baseline), cluster, font=font, fill=NAME_FILL, anchor="ls",
                      stroke_width=stroke, stroke_fill=NAME_STROKE)
            # بار دوم بدون استروک تا لبه‌ی حروف تمیز بماند
            draw.text((x, baseline), cluster, font=font, fill=NAME_FILL, anchor="ls")
        elif kind == "emoji":
            cluster, height = payload
            emoji = _emoji_image(cluster)
            target_h = max(1, int(height))
            target_w = max(1, int(emoji.width * target_h / emoji.height))
            small = emoji.resize((target_w, target_h), Image.LANCZOS)
            top = int(baseline - target_h * 0.86)
            img.paste(small, (int(x), top), small)
        x += width


def _fit_name(name: str):
    """بزرگ‌ترین اندازه‌ای که اسم در یک خط جا شود؛ در غیر این‌صورت دو خط؛ در نهایت کوتاه‌سازی با «…»."""
    name = " ".join(name.split()) or "—"
    for size in range(46, 25, -2):
        items = _layout(name, size)
        if _items_width(items) <= NAME_MAX_WIDTH:
            return [items], size
    # دو خط: تقسیم روی نزدیک‌ترین فاصله به وسط، با اندازه‌های کوچک‌تر
    words = name.split(" ")
    if len(words) > 1:
        for size in (32, 28, 24, 22):
            best = None
            for i in range(1, len(words)):
                first, second = " ".join(words[:i]), " ".join(words[i:])
                w1, w2 = _items_width(_layout(first, size)), _items_width(_layout(second, size))
                if max(w1, w2) <= NAME_MAX_WIDTH and (best is None or max(w1, w2) < best[0]):
                    best = (max(w1, w2), first, second)
            if best:
                return [_layout(best[1], size), _layout(best[2], size)], size
    # کوتاه‌سازی
    text = name
    while len(text) > 1:
        text = text[:-1]
        items = _layout(text.rstrip() + "…", 28)
        if _items_width(items) <= NAME_MAX_WIDTH:
            return [items], 28
    return [_layout("…", 28)], 28


def _erase_placeholder(img: Image.Image, box: tuple) -> None:
    """کلمه‌ی «Group» قالب را با درون‌یابی افقیِ پس‌زمینه‌ی همان ردیف پاک می‌کند."""
    x0, x1, y0, y1 = box
    margin = 12
    left, right = x0 - margin, x1 + margin
    px = img.load()
    for y in range(y0 - 6, y1 + 7):
        lc, rc = px[left, y], px[right, y]
        span = right - left
        for x in range(left + 1, right):
            t = (x - left) / span
            px[x, y] = tuple(int(lc[i] * (1 - t) + rc[i] * t) for i in range(3))


def _circle_photo(photo: Image.Image, radius: int) -> Image.Image:
    side = radius * 2
    photo = photo.convert("RGBA")
    w, h = photo.size
    m = min(w, h)
    photo = photo.crop(((w - m) // 2, (h - m) // 2, (w - m) // 2 + m, (h - m) // 2 + m)).resize((side, side), Image.LANCZOS)
    scale = 4
    mask = Image.new("L", (side * scale, side * scale), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, side * scale - 1, side * scale - 1), fill=255)
    mask = mask.resize((side, side), Image.LANCZOS)
    photo.putalpha(mask)
    return photo


def _placeholder_photo(name: str, radius: int) -> Image.Image:
    side = radius * 2
    img = Image.new("RGBA", (side, side), (40, 8, 8, 255))
    d = ImageDraw.Draw(img)
    for i in range(side):
        shade = int(40 + 110 * i / side)
        d.line([(0, i), (side, i)], fill=(min(255, 70 + shade), 10, 10, 255))
    letter = next((c for c in name if c.isalnum()), "?").upper()
    items = _layout(letter, 110)
    width = _items_width(items)
    _draw_items(img, items, (side - width) / 2, int(side * 0.66), 110)
    scale = 4
    mask = Image.new("L", (side * scale, side * scale), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, side * scale - 1, side * scale - 1), fill=255)
    mask = mask.resize((side, side), Image.LANCZOS)
    img.putalpha(mask)
    return img


def _center_text(img: Image.Image, text: str, center_x: int, baseline: int, size: int, fill, font_path: str) -> None:
    font = _font(font_path, size)
    ImageDraw.Draw(img).text((center_x, baseline), text, font=font, fill=fill, anchor="ms",
                             stroke_width=2, stroke_fill=(6, 2, 10))


def _fit_box_font(text: str, max_w: int, max_h: int, start: int = 90, minimum: int = 18) -> ImageFont.FreeTypeFont:
    bold = os.path.join(FONT_DIR, "NotoSans-Bold.ttf")
    size = start
    while size > minimum:
        font = _font(bold, size)
        l, t, r, b = font.getbbox(text)
        if (r - l) <= max_w and (b - t) <= max_h:
            return font
        size -= 2
    return _font(bold, minimum)


def _draw_box_label(img: Image.Image, text: str, center: tuple, max_w: int, max_h: int,
                     fill, stroke_fill, start: int = 90, stroke_width: int = 3) -> None:
    """متن تک‌فونتی (لاتین/عددی) که داخل یک کادر مشخص جا می‌شود؛ برای برچسب‌های BEFORE/AFTER و کپشن اعداد."""
    font = _fit_box_font(text, max_w, max_h, start=start)
    ImageDraw.Draw(img).text(center, text, font=font, fill=fill, anchor="mm",
                             stroke_width=stroke_width, stroke_fill=stroke_fill)


def _glow_number(img: Image.Image, text: str, center: tuple, max_w: int, max_h: int) -> None:
    font_path = os.path.join(FONT_DIR, "NotoSans-Bold.ttf")
    size = max_h
    while size > 24:
        font = _font(font_path, size)
        l, t, r, b = font.getbbox(text, anchor="ls")
        if (r - l) <= max_w and (b - t) <= max_h:
            break
        size -= 2
    font = _font(font_path, size)
    l, t, r, b = font.getbbox(text, anchor="ls")
    cx, cy = center
    x = cx - (l + r) / 2
    baseline = cy - (t + b) / 2

    glow = Image.new("RGBA", img.size, (0, 0, 0, 0))
    ImageDraw.Draw(glow).text((x, baseline), text, font=font, fill=GREEN + (255,), anchor="ls")
    blurred = glow.filter(ImageFilter.GaussianBlur(14))
    img.alpha_composite(blurred)
    img.alpha_composite(blurred)
    top = Image.new("RGBA", img.size, (0, 0, 0, 0))
    ImageDraw.Draw(top).text((x, baseline), text, font=font, fill=GREEN + (255,), anchor="ls",
                             stroke_width=3, stroke_fill=(255, 255, 255, 255))
    img.alpha_composite(top)


def render_report(
    name: str,
    photo_bytes: Optional[bytes],
    joined: int,
    before_total: Optional[int] = None,
    after_total: Optional[int] = None,
) -> bytes:
    base = Image.open(TEMPLATE_PATH).convert("RGB")
    img = base.convert("RGBA")

    # عکس پروفایل داخل هر دو دایره (قبل/بعد روی عکس گروه تاثیری ندارد، فقط برچسب‌ها فرق دارند)
    photo = None
    if photo_bytes:
        try:
            photo = Image.open(io.BytesIO(photo_bytes))
            photo.load()
        except Exception:  # noqa: BLE001
            photo = None
    circle = _circle_photo(photo, CIRCLE_PASTE_RADIUS) if photo is not None else _placeholder_photo(name, CIRCLE_PASTE_RADIUS)
    for cx, cy in CIRCLE_CENTERS:
        img.alpha_composite(circle, (cx - CIRCLE_PASTE_RADIUS, cy - CIRCLE_PASTE_RADIUS))

    # برچسب‌های BEFORE / AFTER داخل دو کپسول قرمز بالا
    for text, center in zip(("BEFORE", "AFTER"), TOP_BAR_CENTERS):
        _draw_box_label(img, text, center, TOP_BAR_MAX_W, TOP_BAR_MAX_H,
                         TOPBAR_FILL, TOPBAR_STROKE, start=80)

    # تعداد ممبرهای عضوشده: سبز، درشت، داخل کادر قرمز کوچک وسط
    _glow_number(img, f"+{joined:,}", JOINED_CENTER, JOINED_MAX_WIDTH, JOINED_MAX_HEIGHT)

    # اسم دقیق گروه + (اختیاری) جمع اعضای قبل/بعد، داخل نوار سفید پایین
    lines, size = _fit_name(name)
    has_caption = isinstance(before_total, int) and isinstance(after_total, int) and len(lines) == 1
    name_baseline = NAME_BASELINE_Y_WITH_CAPTION if has_caption else NAME_BASELINE_Y
    for center_x in NAME_CENTERS_X:
        for idx, items in enumerate(lines):
            width = _items_width(items)
            baseline = name_baseline + idx * int(size * 1.15) - (int(size * 0.55) if len(lines) > 1 else 0)
            _draw_items(img, items, center_x - width / 2, baseline, size)

    if has_caption:
        caption = f"{before_total:,}  ->  {after_total:,}  MEMBERS"
        _draw_box_label(img, caption, (NAME_CENTERS_X[0], CAPTION_BASELINE_Y - CAPTION_MAX_HEIGHT // 2),
                         CAPTION_MAX_WIDTH, CAPTION_MAX_HEIGHT, CAPTION_FILL, None, start=32, stroke_width=0)

    out = io.BytesIO()
    img.convert("RGB").save(out, format="PNG", optimize=True)
    return out.getvalue()
