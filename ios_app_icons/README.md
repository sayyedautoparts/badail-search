# iOS App Icon set (`AppIcon.appiconset`)

## Generate from your logo

1. Put your **high-resolution** logo as PNG (transparency OK) anywhere, or replace `static/app-icon.png`.
2. Install Pillow once:
   ```bash
   pip install -r tools/requirements-icons.txt
   ```
3. Run:
   ```bash
   python tools/generate_ios_icons.py
   ```
   Or with a custom file:
   ```bash
   python tools/generate_ios_icons.py "C:\path\to\your-logo.png"
   ```

## Output

- **`AppIcon.appiconset/`** — all PNG sizes + `Contents.json` ready for Xcode.
- **`AppIcon-AppStore-1024.png`** — App Store marketing (1024×1024, light background, no transparency).
- Web assets are refreshed: `static/app-icon.png` (96×96) and `static/apple-touch-icon.png` (180×180).

## Use in Xcode

1. Open your iOS project → **Assets.xcassets**.
2. Delete the default **AppIcon** set or create **New Image Set** named `AppIcon`.
3. In Finder, open `ios_app_icons/AppIcon.appiconset` and **drag all images** into the App Icon slots, **or** replace the folder:
   - Copy `AppIcon.appiconset` into `YourApp/Assets.xcassets/AppIcon.appiconset` (merge/replace `Contents.json` + PNGs).

## Design choices (script)

- Background: **#FCFCFE** (very light, not black) — reads well on light and dark wallpapers.
- Logo scaled to **~80%** of the 1024 canvas for iOS corner mask safe area.
- **LANCZOS** downscaling from 1024 master for sharp exports.
- Slight **contrast + sharpness** boost for legibility at small sizes.
- Subtle **soft shadow** under the mark.

## True vector (SVG)

The generator works from **PNG**. For a perfect vector master, recreate the logo in Illustrator / Figma, export **SVG + 1024 PNG**, then point the script at that PNG.
