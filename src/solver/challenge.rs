use chromiumoxide::Page;
use serde::Deserialize;

/// The challenge widget's bounding box, in viewport (CSS) coordinates. The locator JS also reports
/// a width, but only x/y/h are used, so it's dropped on deserialize.
#[derive(Debug, Clone, Copy, Deserialize)]
pub struct ChallengeWidget {
    pub x: f64,
    pub y: f64,
    pub h: f64,
}

const LOCATE_JS: &str = r#"(() => {
  const toBox = (r) => ({ x: r.x, y: r.y, w: r.width, h: r.height });
  const findIframe = (root) => {
    for (const el of root.querySelectorAll('*')) {
      if (el.tagName === 'IFRAME') {
        const r = el.getBoundingClientRect();
        if (r.width > 0 && r.height > 0) return r;
      }
      if (el.shadowRoot) {
        const r = findIframe(el.shadowRoot);
        if (r) return r;
      }
    }
    return null;
  };
  const r = findIframe(document);
  if (r) return toBox(r);
  const inp = document.querySelector(
    'input[name="cf-turnstile-response"], input[id^="cf-chl-widget"][id$="_response"]'
  );
  if (inp) {
    let el = inp.parentElement;
    while (el) {
      const a = el.getBoundingClientRect();
      if (a.height >= 40 && a.height <= 120 && a.y > 0) {
        return toBox({ x: a.x, y: a.y, width: 300, height: a.height });
      }
      el = el.parentElement;
    }
  }
  return null;
})()"#;

pub const PROBE_JS: &str = r#"(() => {
  const out = { hasInput: false, inputAncestors: [], shadowHosts: 0, iframes: [], webgl: null };
  try {
    const gl = document.createElement('canvas').getContext('webgl');
    if (!gl) { out.webgl = 'NO_CONTEXT'; }
    else {
      const dbg = gl.getExtension('WEBGL_debug_renderer_info');
      out.webgl = dbg ? gl.getParameter(dbg.UNMASKED_RENDERER_WEBGL) : 'no_debug_ext';
    }
  } catch (e) { out.webgl = 'ERR:' + e; }
  out.geom = {
    screenX: window.screenX, screenY: window.screenY,
    innerW: window.innerWidth, innerH: window.innerHeight,
    outerW: window.outerWidth, outerH: window.outerHeight,
    dpr: window.devicePixelRatio,
    toolbarByOuterInner: window.outerHeight - window.innerHeight,
  };
  let shadowHosts = 0;
  const walk = (root, inShadow) => {
    for (const el of root.querySelectorAll('*')) {
      if (el.tagName === 'IFRAME') {
        const r = el.getBoundingClientRect();
        out.iframes.push({ src: (el.src||'').slice(0, 60), x: r.x, y: r.y, w: r.width, h: r.height, inShadow });
      }
      if (el.shadowRoot) { shadowHosts++; walk(el.shadowRoot, true); }
    }
  };
  walk(document, false);
  out.shadowHosts = shadowHosts;
  const inp = document.querySelector('input[name="cf-turnstile-response"], input[id^="cf-chl-widget"][id$="_response"]');
  if (inp) {
    out.hasInput = true;
    let el = inp;
    for (let i = 0; i < 6 && el; i++) {
      const r = el.getBoundingClientRect();
      out.inputAncestors.push({ tag: el.tagName, id: el.id || '', x: r.x, y: r.y, w: r.width, h: r.height });
      el = el.parentElement;
    }
  }
  return JSON.stringify(out);
})()"#;

pub async fn locate_widget(page: &Page) -> Option<ChallengeWidget> {
    match page.evaluate(LOCATE_JS).await {
        Ok(res) => res.into_value::<Option<ChallengeWidget>>().ok().flatten(),
        Err(e) => {
            tracing::debug!("locate_widget eval failed (likely mid-navigation): {e}");
            None
        }
    }
}
