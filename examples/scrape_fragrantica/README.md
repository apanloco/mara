# scrape_fragrantica

The end-to-end proof that a real, JS-heavy, Cloudflare-fronted site can be
scraped **fully browser-free** through mara. It scrapes a Fragrantica perfume
page and emits a `{ blob, html, id, slug, perfume_url }` record.

This example is also a **parity target against decant**, the production scraper
mara is built to feed: it emits the same record decant does, for a direct diff. All site-specific logic lives here in the consumer, never in
the mara library — the `_pd` decrypt is ported to Rust (`decrypt.rs`), so no
Chrome sits in the data path.

## How it works

- **`fetch_http` returns the raw server HTML with no Chrome**, replaying a stored
  clearance, escalating to mara's headed solver only to *earn* the clearance the
  first time.
- **Encrypted blobs are CryptoJS "OpenSSL" passphrase mode**: `{ct, iv, s}` with
  `EvpKDF-MD5(passphrase, salt=hex(s))` → AES-256-CBC key+iv, Pkcs7, JSON (the
  stored `iv` is redundant). Passphrase rules, recovered by hooking the page's
  live CryptoJS (the recipe, not the source — `_pd` is a babel wrapper over
  webpack-internal modules):
  - **inline page globals** (`status`, `similar_perfumes`, `ai_opinions`) share a
    build-constant passphrase baked into the bundle. It's stable across pages; if
    the site rebuilds with a new seed the decrypt fails loudly and the passphrase
    is re-recovered.
  - **postscribe slots** key on the element's own `slotid` (unused — those slots
    are only ad/shop widgets).
- **The dynamic fields are Vue components** in the raw HTML, but their data is
  inlined as those encrypted globals, so the consumer reads them from the
  **structured blob** rather than re-rendering and DOM-scraping. `parse.rs`
  (ported verbatim from decant) handles the static SSR fields.

## Parity caveat

The blobs carry **exact** vote counts, whereas decant scrapes them from the DOM
where they're abbreviated ("1.5K"), so a diff differs *only* on vote magnitudes
(plus temporal drift) — text, ids and slugs match. The browser-free path is the
more precise source.
