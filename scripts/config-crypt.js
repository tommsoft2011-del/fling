#!/usr/bin/env node
/**
 * Fling 配置加密小工具（本地运行，明文不出本机）
 *
 * 用法：
 *   # 1. 准备明文配置 plain-config.json（见下方格式），然后加密：
 *   node scripts/config-crypt.js encrypt --in plain-config.json --out bundle.json
 *
 *   # 2. 验证解密（可选）：
 *   node scripts/config-crypt.js decrypt --in bundle.json
 *
 * 输出 bundle.json 包含：
 *   { v: 1, key: "<base64>", nonce: "<base64>", data: "<base64>" }
 *   - key/nonce/data 发给开发者 → 编译进 app（embedded_config.rs）
 *   - 把 { v, nonce, data }（去掉 key）另存为 server-config.json 放到服务器上，
 *     app 联网时拉取并用内置 key 解密更新。
 *
 * plain-config.json 格式：
 *   {
 *     "server_url": "wss://rtc.example.com",
 *     "api_key": "...",
 *     "api_secret": "...",
 *     "turn_url": "turn:1.2.3.4:3478",
 *     "turn_username": "...",
 *     "turn_password": "...",
 *     "remote_config_url": "https://example.com/fling/server-config.json"
 *   }
 */
const crypto = require('crypto');
const fs = require('fs');

function b64(b) { return Buffer.from(b).toString('base64'); }
function unb64(s) { return Buffer.from(s, 'base64'); }

function encrypt(plainJson) {
  const key = crypto.randomBytes(32);
  const nonce = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, nonce);
  const ct = Buffer.concat([cipher.update(plainJson, 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  // data = ciphertext || tag（Rust 端 aes-gcm 期望这种拼接）
  return { key, nonce, data: Buffer.concat([ct, tag]) };
}

function decrypt(key, nonce, data) {
  const ct = data.subarray(0, data.length - 16);
  const tag = data.subarray(data.length - 16);
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, nonce);
  decipher.setAuthTag(tag);
  return Buffer.concat([decipher.update(ct), decipher.final()]).toString('utf8');
}

const [cmd, ...rest] = process.argv.slice(2);
const args = {};
for (let i = 0; i < rest.length; i += 2) args[rest[i].replace(/^--/, '')] = rest[i + 1];

if (cmd === 'encrypt') {
  if (!args.in || !args.out) {
    console.error('用法: node scripts/config-crypt.js encrypt --in plain-config.json --out bundle.json');
    process.exit(1);
  }
  const plain = fs.readFileSync(args.in, 'utf8');
  JSON.parse(plain); // 校验 JSON 合法
  const { key, nonce, data } = encrypt(plain);
  const bundle = { v: 1, key: b64(key), nonce: b64(nonce), data: b64(data) };
  fs.writeFileSync(args.out, JSON.stringify(bundle, null, 2));
  // 自检：解密验证
  const back = decrypt(key, nonce, data);
  if (back !== plain) { console.error('自检失败！'); process.exit(1); }
  console.log(`OK → ${args.out}（自检通过）`);
  console.log('把这个文件发给开发者用于编译；另把 {v,nonce,data} 存为 server-config.json 放服务器。');
} else if (cmd === 'decrypt') {
  if (!args.in) {
    console.error('用法: node scripts/config-crypt.js decrypt --in bundle.json');
    process.exit(1);
  }
  const b = JSON.parse(fs.readFileSync(args.in, 'utf8'));
  console.log(decrypt(unb64(b.key), unb64(b.nonce), unb64(b.data)));
} else if (cmd === 'server-json') {
  // 从 bundle 提取去 key 的服务器版本
  if (!args.in || !args.out) {
    console.error('用法: node scripts/config-crypt.js server-json --in bundle.json --out server-config.json');
    process.exit(1);
  }
  const b = JSON.parse(fs.readFileSync(args.in, 'utf8'));
  fs.writeFileSync(args.out, JSON.stringify({ v: 1, nonce: b.nonce, data: b.data }, null, 2));
  console.log(`OK → ${args.out}（不含 key，可放心放服务器）`);
} else {
  console.error('命令: encrypt | decrypt | server-json');
  process.exit(1);
}
