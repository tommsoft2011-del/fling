#!/usr/bin/env python3
"""把 config-crypt.js 生成的 bundle.json 转成 Rust embedded_config.rs（开发者用）"""
import base64
import json
import sys

def rust_bytes(b: bytes) -> str:
    return "[" + ", ".join(str(x) for x in b) + "]"

bundle_path = sys.argv[1] if len(sys.argv) > 1 else "bundle.json"
out_path = sys.argv[2] if len(sys.argv) > 2 else "apps/desktop/src-tauri/src/embedded_config.rs"

b = json.load(open(bundle_path))
key = base64.b64decode(b["key"])
nonce = base64.b64decode(b["nonce"])
data = base64.b64decode(b["data"])
assert len(key) == 32 and len(nonce) == 12

rs = f"""// 自动生成：scripts/gen-embedded-config.py（AES-256-GCM 密文，勿手改）

/// AES-256-GCM 密钥（32 字节）
pub const CONFIG_KEY: [u8; 32] = {rust_bytes(key)};
/// 加密时的 nonce（12 字节）
pub const CONFIG_NONCE: [u8; 12] = {rust_bytes(nonce)};
/// AES-GCM 密文（JSON 配置的加密结果，ciphertext||tag）
pub const CONFIG_CIPHERTEXT: &[u8] = &{rust_bytes(data)};
/// 是否有有效的嵌入配置
pub const HAS_EMBEDDED: bool = true;
"""
open(out_path, "w").write(rs)
print(f"OK → {out_path}（{len(data)} 字节密文）")
