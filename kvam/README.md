# Kvamskogen.no — UI kit

A high-fidelity recreation of a proposed front-door site for **Kvamskogen**: hero, live weather strip, season-aware activity grid, trail list, and an "add activity" submission flow.

This is a **proposed** UI — there is no existing kvamskogen.no design to copy from beyond the very plain WordPress site at the live URL. The visual language follows the design tokens in `/colors_and_type.css` and the editorial reference points (Engadin, Geilo) noted in the root README.

## Files
- `index.html` — entry, runs the demo. Click-thru: hero CTA → activities → "legg til aktivitet" modal → trail detail.
- `App.jsx` — root layout + routing (hash-based, fake).
- `Header.jsx`, `Footer.jsx` — page chrome.
- `Hero.jsx` — full-bleed hero with weather strip docked at bottom.
- `WeatherStrip.jsx` — sticky live conditions row.
- `ActivityGrid.jsx`, `ActivityCard.jsx` — season-aware activity grid.
- `TrailList.jsx` — løypeliste with grooming status.
- `AddActivityModal.jsx` — the "legg til aktivitet" submission form.
- `Brand.jsx` — wordmark + spruce mark.
- `Icons.jsx` — small inline-SVG Lucide subset (so we don't depend on a CDN at runtime).

## Components covered
sidebar/header chrome · hero with photo placeholder · weather strip · season filter chips · photo cards · numbered trail rows · grooming status chips · primary/secondary/accent buttons · form fields · modal · footer with dugnad/donation block.

## What we cut
This is a UI kit, not production code:
- No real weather API — values are static.
- No real trail data — three illustrative rows.
- The modal "submit" just closes the modal and shows a success state for one frame.
- Photos are CSS-gradient placeholders tinted to season; see `/assets/photos/README.md` for what the real assets should be.
