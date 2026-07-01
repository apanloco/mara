use anyhow::{Result, anyhow};
use image::ImageEncoder;
use std::path::Path;
use x11rb::connection::Connection;
use x11rb::protocol::xfixes::ConnectionExt as _;
use x11rb::protocol::xproto::{ConnectionExt as _, ImageFormat, ImageOrder};
use x11rb::rust_connection::RustConnection;

pub struct Image {
    pub w: usize,
    pub h: usize,
    pub px: Vec<u8>,
}

impl Image {
    #[inline]
    pub fn at(&self, x: usize, y: usize) -> (u8, u8, u8) {
        let o = (y * self.w + x) * 3;
        (self.px[o], self.px[o + 1], self.px[o + 2])
    }

    pub fn decode_png(bytes: &[u8]) -> Result<Image> {
        let img = image::load_from_memory(bytes)?.to_rgb8();
        let (w, h) = img.dimensions();
        Ok(Image {
            w: w as usize,
            h: h as usize,
            px: img.into_raw(),
        })
    }

    pub fn to_png(&self) -> Result<Vec<u8>> {
        encode_png(&self.px, self.w, self.h)
    }
}

pub fn grab(display: &str) -> Option<Vec<u8>> {
    (|| -> Result<Vec<u8>> {
        let (conn, mut img) = capture(display)?;
        composite_cursor(&conn, &mut img);
        img.to_png()
    })()
    .map_err(|e| tracing::debug!("frame grab failed: {e}"))
    .ok()
}

pub fn grab_image(display: &str) -> Result<Image> {
    Ok(capture(display)?.1)
}

pub fn screenshot(display: &str, path: &Path) -> Result<()> {
    std::fs::write(path, capture(display)?.1.to_png()?)?;
    Ok(())
}

fn capture(display: &str) -> Result<(RustConnection, Image)> {
    let (conn, screen_num) = x11rb::connect(Some(display))?;
    let screen = &conn.setup().roots[screen_num];
    let root = screen.root;
    let (w, h) = (
        screen.width_in_pixels as usize,
        screen.height_in_pixels as usize,
    );
    let img = conn
        .get_image(ImageFormat::Z_PIXMAP, root, 0, 0, w as u16, h as u16, !0u32)?
        .reply()?;
    let bpp = conn
        .setup()
        .pixmap_formats
        .iter()
        .find(|f| f.depth == img.depth)
        .map(|f| f.bits_per_pixel as usize)
        .ok_or_else(|| anyhow!("no pixmap format for depth {}", img.depth))?;
    if bpp != 32 {
        return Err(anyhow!("unexpected {bpp} bits/pixel (want 32)"));
    }
    let lsb = conn.setup().image_byte_order == ImageOrder::LSB_FIRST;
    let px = zpixmap_to_rgb(&img.data, w, h, lsb);
    Ok((conn, Image { w, h, px }))
}

fn composite_cursor(conn: &RustConnection, img: &mut Image) {
    let cursor = (|| {
        conn.xfixes_query_version(5, 0).ok()?.reply().ok()?;
        conn.xfixes_get_cursor_image().ok()?.reply().ok()
    })();
    let Some(cur) = cursor else { return };
    let (w, h) = (img.w, img.h);
    let (cw, ch) = (cur.width as i32, cur.height as i32);
    let (ox, oy) = (
        cur.x as i32 - cur.xhot as i32,
        cur.y as i32 - cur.yhot as i32,
    );
    for cy in 0..ch {
        for cx in 0..cw {
            let (px, py) = (ox + cx, oy + cy);
            if px < 0 || py < 0 || px >= w as i32 || py >= h as i32 {
                continue;
            }
            let argb = cur.cursor_image[(cy * cw + cx) as usize];
            let a = (argb >> 24) & 0xff;
            if a == 0 {
                continue;
            }
            let (cr, cg, cb) = ((argb >> 16) & 0xff, (argb >> 8) & 0xff, argb & 0xff);
            let inv = 255 - a;
            let p = (py as usize * w + px as usize) * 3;
            img.px[p] = (cr + img.px[p] as u32 * inv / 255) as u8;
            img.px[p + 1] = (cg + img.px[p + 1] as u32 * inv / 255) as u8;
            img.px[p + 2] = (cb + img.px[p + 2] as u32 * inv / 255) as u8;
        }
    }
}

fn encode_png(rgb: &[u8], w: usize, h: usize) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    image::codecs::png::PngEncoder::new(&mut out).write_image(
        rgb,
        w as u32,
        h as u32,
        image::ExtendedColorType::Rgb8,
    )?;
    Ok(out)
}

fn zpixmap_to_rgb(data: &[u8], w: usize, h: usize, lsb: bool) -> Vec<u8> {
    let stride = w * 4;
    let mut rgb = vec![0u8; w * h * 3];
    for y in 0..h {
        for x in 0..w {
            let o = y * stride + x * 4;
            let (r, g, b) = if lsb {
                (data[o + 2], data[o + 1], data[o])
            } else {
                (data[o + 1], data[o + 2], data[o + 3])
            };
            let p = (y * w + x) * 3;
            rgb[p] = r;
            rgb[p + 1] = g;
            rgb[p + 2] = b;
        }
    }
    rgb
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::display::VirtualDisplay;

    #[test]
    fn zpixmap_color_order() {
        let rgb = zpixmap_to_rgb(&[0, 0, 255, 0], 1, 1, true);
        assert_eq!(&rgb, &[255, 0, 0], "LSB red mis-ordered");
        let rgb = zpixmap_to_rgb(&[0, 255, 0, 0], 1, 1, false);
        assert_eq!(&rgb, &[255, 0, 0], "MSB red mis-ordered");
    }

    #[test]
    #[ignore = "needs Xvfb; run with --ignored"]
    fn composites_cursor() {
        use x11rb::protocol::xtest::ConnectionExt as _;
        let d = VirtualDisplay::start(800, 600).expect("start Xvfb");
        let name = d.name();
        std::thread::sleep(std::time::Duration::from_millis(300));
        let (conn, sn) = x11rb::connect(Some(&name)).expect("connect");
        let root = conn.setup().roots[sn].root;
        conn.xtest_fake_input(
            x11rb::protocol::xproto::MOTION_NOTIFY_EVENT,
            0,
            0,
            root,
            400,
            300,
            0,
        )
        .and_then(|_| conn.flush())
        .expect("warp cursor");
        std::thread::sleep(std::time::Duration::from_millis(200));

        let png = grab(&name).expect("grab a frame");
        let img = image::load_from_memory(&png).expect("valid PNG").to_rgb8();
        assert_eq!(img.dimensions(), (800, 600));

        let cursor_pixels = (380..420)
            .flat_map(|x| (290..330).map(move |y| (x, y)))
            .filter(|&(x, y)| img.get_pixel(x, y).0 != [0, 0, 0])
            .count();
        assert!(
            cursor_pixels > 10,
            "cursor not composited near (400,300): {cursor_pixels} px"
        );
    }
}
