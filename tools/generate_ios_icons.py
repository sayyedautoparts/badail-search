#!/usr/bin/env python3
"""
Generate a full iOS AppIcon.appiconset from a source logo (PNG with transparency).

Usage:
  python tools/generate_ios_icons.py [path/to/logo.png]

Default source: static/app-icon.png (project root relative)

Output: ios_app_icons/AppIcon.appiconset/
  - AppIcon-1024.png (App Store, no alpha — RGB on light background)
  - All required iPhone / iPad PNGs
  - Contents.json for Xcode

Requires: pip install -r tools/requirements-icons.txt
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

try:
    from PIL import Image, ImageEnhance, ImageFilter, ImageOps
except ImportError as e:
    raise SystemExit("Install Pillow: pip install -r tools/requirements-icons.txt") from e

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT / "static" / "app-icon.png"
OUT_DIR = ROOT / "ios_app_icons" / "AppIcon.appiconset"

# Light background (not black) — works on light & dark home screen
BG_RGB = (252, 252, 254)
# Inner safe area: logo max fraction of canvas (padding for iOS rounded mask)
LOGO_MAX_FRACT = 0.80
MASTER = 1024


def _trim_transparent(im: Image.Image) -> Image.Image:
    if im.mode != "RGBA":
        im = im.convert("RGBA")
    bbox = im.getbbox()
    return im.crop(bbox) if bbox else im


def _build_master_1024(logo: Image.Image) -> Image.Image:
    logo = _trim_transparent(logo)
    lw, lh = logo.size
    max_side = int(MASTER * LOGO_MAX_FRACT)
    scale = min(max_side / lw, max_side / lh)
    nw, nh = max(1, int(round(lw * scale))), max(1, int(round(lh * scale)))
    logo_s = logo.resize((nw, nh), Image.Resampling.LANCZOS)
    # Slightly bolder / clearer at small sizes
    logo_s = ImageEnhance.Contrast(logo_s).enhance(1.06)
    logo_s = ImageEnhance.Sharpness(logo_s).enhance(1.12)

    canvas = Image.new("RGBA", (MASTER, MASTER), (*BG_RGB, 255))
    ox = (MASTER - nw) // 2
    oy = (MASTER - nh) // 2

    # Subtle drop shadow (professional, not heavy)
    shadow_offset = (0, max(3, nh // 80))
    blur = max(6, min(18, nw // 50))
    alpha = logo_s.split()[3]
    shadow_rgba = Image.new("RGBA", logo_s.size, (40, 45, 60, 0))
    shadow_rgba.putalpha(alpha.point(lambda p: int(p * 0.35)))
    sw = Image.new("RGBA", (MASTER, MASTER), (0, 0, 0, 0))
    sx = ox + shadow_offset[0]
    sy = oy + shadow_offset[1]
    sw.paste(shadow_rgba, (sx, sy), shadow_rgba)
    sw = sw.filter(ImageFilter.GaussianBlur(blur))
    canvas = Image.alpha_composite(canvas, sw)
    canvas.paste(logo_s, (ox, oy), logo_s)

    # App Store 1024 must not use transparency — flat RGB
    return canvas.convert("RGB")


def _export_png(master_rgb: Image.Image, size: int, path: Path) -> None:
    w, h = master_rgb.size
    if w == h == size:
        im = master_rgb.copy()
    else:
        im = master_rgb.resize((size, size), Image.Resampling.LANCZOS)
    path.parent.mkdir(parents=True, exist_ok=True)
    im.save(path, format="PNG", optimize=True)


# (filename, pixel_size, idiom, scale_label, size_label for Contents.json)
ICON_SPECS: list[tuple[str, int, str, str, str]] = [
    # iPhone
    ("AppIcon-iPhone-20@2x.png", 40, "iphone", "2x", "20x20"),
    ("AppIcon-iPhone-20@3x.png", 60, "iphone", "3x", "20x20"),
    ("AppIcon-iPhone-29@2x.png", 58, "iphone", "2x", "29x29"),
    ("AppIcon-iPhone-29@3x.png", 87, "iphone", "3x", "29x29"),
    ("AppIcon-iPhone-40@2x.png", 80, "iphone", "2x", "40x40"),
    ("AppIcon-iPhone-40@3x.png", 120, "iphone", "3x", "40x40"),
    ("AppIcon-iPhone-60@2x.png", 120, "iphone", "2x", "60x60"),
    ("AppIcon-iPhone-60@3x.png", 180, "iphone", "3x", "60x60"),
    # iPad
    ("AppIcon-iPad-20@1x.png", 20, "ipad", "1x", "20x20"),
    ("AppIcon-iPad-20@2x.png", 40, "ipad", "2x", "20x20"),
    ("AppIcon-iPad-29@1x.png", 29, "ipad", "1x", "29x29"),
    ("AppIcon-iPad-29@2x.png", 58, "ipad", "2x", "29x29"),
    ("AppIcon-iPad-40@1x.png", 40, "ipad", "1x", "40x40"),
    ("AppIcon-iPad-40@2x.png", 80, "ipad", "2x", "40x40"),
    ("AppIcon-iPad-76@1x.png", 76, "ipad", "1x", "76x76"),
    ("AppIcon-iPad-76@2x.png", 152, "ipad", "2x", "76x76"),
    ("AppIcon-iPad-83.5@2x.png", 167, "ipad", "2x", "83.5x83.5"),
    # App Store
    ("AppIcon-AppStore-1024.png", 1024, "ios-marketing", "1x", "1024x1024"),
]


def build_contents_json() -> dict:
    images = []
    for fname, _px, idiom, scale, size in ICON_SPECS:
        images.append(
            {
                "filename": fname,
                "idiom": idiom,
                "scale": scale,
                "size": size,
            }
        )
    return {"images": images, "info": {"author": "xcode", "version": 1}}


def main() -> None:
    src = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else DEFAULT_SOURCE
    if not src.is_file():
        raise SystemExit(f"Source image not found: {src}")

    print(f"Source: {src}")
    logo = Image.open(src)
    master_rgb = _build_master_1024(logo)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for fname, px, *_rest in ICON_SPECS:
        out_path = OUT_DIR / fname
        _export_png(master_rgb, px, out_path)
        print(f"  wrote {px}x{px} -> {out_path.relative_to(ROOT)}")

    contents_path = OUT_DIR / "Contents.json"
    contents_path.write_text(json.dumps(build_contents_json(), indent=2), encoding="utf-8")
    print(f"  wrote {contents_path.relative_to(ROOT)}")

    # Optional: refresh web favicons from same master (optional consistency)
    web_96 = ROOT / "static" / "app-icon.png"
    web_180 = ROOT / "static" / "apple-touch-icon.png"
    _export_png(master_rgb, 96, web_96)
    _export_png(master_rgb, 180, web_180)
    print(f"  updated {web_96.relative_to(ROOT)} (96) and {web_180.relative_to(ROOT)} (180)")

    print("\nDone. Open ios_app_icons/AppIcon.appiconset in Xcode or copy the folder into Assets.xcassets.")


if __name__ == "__main__":
    main()
