# Assets Directory

This directory should contain the following assets:

- `icon.ico` - Windows service and application icon
- Other images or resources needed by the application

## Creating an Icon File

You can create an ICO file from PNG images using software like:
- GIMP
- ImageMagick
- Online converters

For best results, include multiple sizes in the ICO file:
- 16x16
- 32x32
- 48x48
- 64x64
- 128x128
- 256x256

For example, using ImageMagick:

```bash
convert input.png -define icon:auto-resize=256,128,64,48,32,16 icon.ico
```