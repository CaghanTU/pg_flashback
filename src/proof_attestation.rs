use hmac::{Hmac, Mac};
use pgrx::prelude::*;
use sha2::Sha256;
use std::fs;
use std::os::unix::fs::PermissionsExt;

type HmacSha256 = Hmac<Sha256>;

fn decode_key(raw: &str) -> Result<[u8; 32], String> {
    let encoded = raw.trim();
    if encoded.len() != 64 || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("proof HMAC key must contain exactly 64 hexadecimal characters".to_owned());
    }
    let mut key = [0_u8; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&encoded[index * 2..index * 2 + 2], 16)
            .map_err(|_| "proof HMAC key contains invalid hexadecimal".to_owned())?;
    }
    Ok(key)
}

fn load_key() -> Result<[u8; 32], String> {
    let path = crate::storage::worker::proof_hmac_key_file()
        .ok_or_else(|| "pg_flashback.proof_hmac_key_file is not configured".to_owned())?;
    let metadata = fs::symlink_metadata(&path)
        .map_err(|error| format!("cannot inspect proof HMAC key file: {error}"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err("proof HMAC key path must be a regular non-symlink file".to_owned());
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err("proof HMAC key file must not grant group or other permissions".to_owned());
    }
    if metadata.len() > 4096 {
        return Err("proof HMAC key file is unexpectedly large".to_owned());
    }
    let raw = fs::read_to_string(&path)
        .map_err(|error| format!("cannot read proof HMAC key file: {error}"))?;
    decode_key(&raw)
}

fn decode_signature(signature: &str) -> Option<[u8; 32]> {
    let encoded = signature.trim();
    if encoded.len() != 64 || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let mut result = [0_u8; 32];
    for (index, byte) in result.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&encoded[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(result)
}

#[pg_extern(strict)]
fn flashback_verify_proof_hmac(payload: &str, signature: &str) -> bool {
    let key = load_key().unwrap_or_else(|error| pgrx::error!("{error}"));
    let Some(signature) = decode_signature(signature) else {
        return false;
    };
    let mut mac =
        HmacSha256::new_from_slice(&key).unwrap_or_else(|_| pgrx::error!("invalid proof HMAC key"));
    mac.update(payload.as_bytes());
    mac.verify_slice(&signature).is_ok()
}

#[cfg(test)]
mod tests {
    use super::decode_key;

    #[test]
    fn key_parser_is_strict() {
        assert!(decode_key(&"ab".repeat(32)).is_ok());
        assert!(decode_key("abcd").is_err());
        assert!(decode_key(&"zz".repeat(32)).is_err());
    }
}
