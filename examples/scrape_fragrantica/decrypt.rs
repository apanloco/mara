use aes::cipher::{BlockDecryptMut, KeyIvInit, block_padding::Pkcs7};
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use md5::{Digest, Md5};
use serde::Deserialize;

const INLINE_PASSPHRASE: &[u8] = b"998ed1c8a43d1d2a9c4fd64963db30e4";

type Aes256CbcDec = cbc::Decryptor<aes::Aes256>;

#[derive(Deserialize)]
struct Blob {
    ct: String,
    s: String,
}

fn evp_bytes_to_key(pass: &[u8], salt: &[u8], key_len: usize, iv_len: usize) -> (Vec<u8>, Vec<u8>) {
    let mut out = Vec::new();
    let mut block = Vec::new();
    while out.len() < key_len + iv_len {
        let mut h = Md5::new();
        h.update(&block);
        h.update(pass);
        h.update(salt);
        block = h.finalize().to_vec();
        out.extend_from_slice(&block);
    }
    (
        out[..key_len].to_vec(),
        out[key_len..key_len + iv_len].to_vec(),
    )
}

fn decrypt(blob: &Blob, passphrase: &[u8]) -> Result<Vec<u8>> {
    let salt = hex::decode(&blob.s).context("blob salt is not hex")?;
    let (key, iv) = evp_bytes_to_key(passphrase, &salt, 32, 16);
    let ct = base64::engine::general_purpose::STANDARD
        .decode(blob.ct.as_bytes())
        .context("blob ciphertext is not base64")?;
    Aes256CbcDec::new(key.as_slice().into(), iv.as_slice().into())
        .decrypt_padded_vec_mut::<Pkcs7>(&ct)
        .map_err(|e| anyhow!("AES-256-CBC decrypt/unpad failed: {e} (passphrase rotated?)"))
}

pub fn decrypt_inline(html: &str, name: &str) -> Result<serde_json::Value> {
    let raw = extract_inline(html, name)
        .with_context(|| format!("inline `{name}` blob not found in page"))?;
    let blob: Blob = serde_json::from_str(raw)
        .with_context(|| format!("inline `{name}` is not a CryptoJS {{ct,iv,s}} blob"))?;
    let plaintext =
        decrypt(&blob, INLINE_PASSPHRASE).with_context(|| format!("decrypting inline `{name}`"))?;
    serde_json::from_slice(&plaintext).with_context(|| format!("decrypted `{name}` is not JSON"))
}

fn extract_inline<'a>(html: &'a str, name: &str) -> Option<&'a str> {
    let mut from = 0;
    while let Some(rel) = html[from..].find(name) {
        let at = from + rel;
        from = at + name.len();
        let before_ok = html[..at]
            .chars()
            .next_back()
            .is_none_or(|c| !c.is_alphanumeric() && c != '_');
        let after = html[at + name.len()..].trim_start();
        if !before_ok || !after.starts_with('=') {
            continue;
        }
        let rest = after[1..].trim_start();
        if !rest.starts_with('{') {
            continue;
        }
        let start = rest.as_ptr() as usize - html.as_ptr() as usize;
        if let Some(end) = brace_end(&html[start..]) {
            return Some(&html[start..start + end]);
        }
    }
    None
}

fn brace_end(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth = 0i32;
    let mut in_str = false;
    let mut escaped = false;
    for (i, &b) in bytes.iter().enumerate() {
        if in_str {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_str = false;
            }
            continue;
        }
        match b {
            b'"' => in_str = true,
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i + 1);
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const STICKY: &str = r#"<script>let stickyNote = {"ct":"UdBXZmk57fAkc3vhr+KO6w==","iv":"57a698a1d793e4d6818eb3c0828007f9","s":"bcbaba9d98fd49ef"};</script>"#;

    #[test]
    fn decrypts_real_inline_blob() {
        assert_eq!(
            decrypt_inline(STICKY, "stickyNote").unwrap(),
            serde_json::Value::Bool(false)
        );
    }

    #[test]
    fn name_match_respects_word_boundary() {
        let html = r#"my_stickyNote = {"ct":"x","s":"00"};"#;
        assert!(decrypt_inline(html, "stickyNote").is_err());
    }

    #[test]
    fn evp_kdf_matches_openssl_vector() {
        let (key, _iv) = evp_bytes_to_key(
            INLINE_PASSPHRASE,
            &hex::decode("e59dc19708bb7522").unwrap(),
            32,
            16,
        );
        assert_eq!(
            hex::encode(key),
            "6a26275f0eb485d9d23c78ac8559bcf9ae030dd32f2d03b910eb77a839039780"
        );
    }
}
