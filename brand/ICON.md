# Home-screen icon

`icon.png` is this app's tile on the Homeroom home screen, declared in
`dapp.json` as `"icon": { "image": "brand/icon.png" }`. The platform reads it at deploy time; the app never serves
it, which is why it lives outside `public/`.

It follows the Homeroom brand's 3D icon style (v3), the look of the icon set
in the Homeroom brand guidelines, so it sits with the other apps' icons:

- Object: Lucide `radio` drawn as a solid object in charcoal (`#3A3A3A`, lit
  from the top left), with a #1B1B1B side face extruded toward the lower
  right and a thin ink (`#1D1B16`) outline.
- Tile: brand cream (`#FFFEEA`). No text.
- 512 × 512 PNG, full bleed, no transparency. The platform rounds and crops
  the tile itself.

To change the icon, replace `icon.png` with a new render in the same style
and update this note. The change applies once the proposal is voted in and
deployed.
