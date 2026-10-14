// 嵌入式加密配置：敏感的服务器凭据（LiveKit/TURN）经 AES-256-GCM 加密后编译进二进制，
// 运行时解密使用。防 strings 扫描、配置文件明文、服务器脱库明文；
// 不防逆向提取密钥（密钥也在二进制里），真·密钥安全需 token server。
//
// 此文件由 scripts/encrypt_config.py 生成（用户本地运行，敏感值不经他人之手）。
// 若未生成（开源构建），则使用下面的空占位，app 回退到用户手动配置。

/// AES-256-GCM 密钥（32 字节，由构建时口令 PBKDF2 派生）
pub const CONFIG_KEY: [u8; 32] = [0u8; 32];
/// 加密时的 nonce（12 字节）
pub const CONFIG_NONCE: [u8; 12] = [0u8; 12];
/// AES-GCM 密文（JSON 配置的加密结果）
pub const CONFIG_CIPHERTEXT: &[u8] = &[];
/// 是否有有效的嵌入配置（空密文 = 未生成）
pub const HAS_EMBEDDED: bool = false;
