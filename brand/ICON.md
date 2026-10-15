# Home-screen icon

`icon.png` is this app's tile on the Homeroom home screen, declared in
`dapp.json` as `"icon": { "image": "brand/icon.png" }`. The platform reads it at deploy time; the app never serves
it, which is why it lives outside `public/`.

It follows the Homeroom brand tile style (v2), so it sits with the other
apps' icons as one set:

- Glyph: Lucide `radio`, drawn as an outlined sticker: a 3.6-unit black
  stroke under a 1.7-unit yellow (`#FFEE6F`) stroke, round caps and joins,
  280 px wide, centred slightly low and tilted -6°.
- Colour: a flat black tile (`#000000`) from the brand palette. No
  gradient, no shadow, no sparkle.
- 512 × 512 PNG, full bleed, no text, no transparency. The platform rounds
  and crops the tile itself.

To change the icon, replace `icon.png` with a new render in the same style
and update this note. The change applies once the proposal is voted in and
deployed.
