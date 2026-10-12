import './style.css';
import { invoke } from '@tauri-apps/api/core';
import { writeText as writeClipboard } from '@tauri-apps/plugin-clipboard-manager';
import {
  FlingRoom,
  normalizeCode,
  roomNameForCode,
  wrapWebFile,
  type FileManifest,
} from '@fling/core';
import { BrowserTransport } from './transport-browser.js';

// ---------- tiny DOM helpers ----------
const $ = <T extends HTMLElement = HTMLElement>(sel: string) =>
  document.querySelector(sel) as T;
const el = (tag: string, cls = '', text = '') => {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text) e.textContent = text;
  return e;
};
const fmtSize = (n: number) =>
  n < 1024 ? `${n} B` : n < 1048576 ? `${(n / 1024).toFixed(1)} KB` : `${(n / 1048576).toFixed(1)} MB`;
const fmtTime = () => new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
const esc = (s: string) => s.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]!));

// ---------- app state ----------
let fling: FlingRoom | null = null;
let transport: BrowserTransport | null = null;
let myIdentity = '';
let myRoomNumber = '';
const peers = new Set<string>();
const pendingFiles = new Map<string, { bytes: Uint8Array; manifest: FileManifest }>();

function setStatus(t: string) {
  $('#status').textContent = t;
}
function renderPeers() {
  $('#peers').textContent = peers.size === 0 ? '只有你在房间里' : `${peers.size + 1} 人在线`;
  $('#sendBtn').toggleAttribute('data-warn', peers.size === 0);
}

// ---------- screens ----------
function show(id: string) {
  document.querySelectorAll('.screen').forEach((s) => s.classList.remove('active'));
  $(`#${id}`).classList.add('active');
}

// ---------- setup screen ----------
async function initSetup() {
  $('#saveCfg').addEventListener('click', async () => {
    const cfg = {
      server_url: ($('#cfgUrl') as HTMLInputElement).value.trim(),
      api_key: ($('#cfgKey') as HTMLInputElement).value.trim(),
      api_secret: ($('#cfgSecret') as HTMLInputElement).value.trim(),
    };
    try {
      await invoke('save_config', { cfg });
      await initJoin();
    } catch (e) {
      alert('保存失败：' + e);
    }
  });
}

// ---------- join screen ----------
async function initJoin() {
  show('joinScreen');
  setStatus('');
  const tryJoin = async () => {
    const code = normalizeCode(($('#roomInput') as HTMLInputElement).value);
    if (!code) {
      alert('请输入房间号');
      return;
    }
    await joinRoom(code);
  };
  $('#joinBtn').onclick = tryJoin;
  ($('#roomInput') as HTMLInputElement).onkeydown = (e) => {
    if (e.key === 'Enter') void tryJoin();
  };
  $('#editCfg').onclick = async () => {
    const cfg: any = await invoke('get_config').catch(() => null);
    if (cfg) {
      ($('#cfgUrl') as HTMLInputElement).value = cfg.server_url ?? '';
      ($('#cfgKey') as HTMLInputElement).value = cfg.api_key ?? '';
    }
    show('setupScreen');
  };
}

async function joinRoom(code: string) {
  setStatus('正在加入…');
  try {
    const roomName = await roomNameForCode(code);
    myIdentity = 'fling-' + Math.random().toString(16).slice(2, 8);
    myRoomNumber = code;
    const token = (await invoke('mint_token', { room: roomName, identity: myIdentity })) as string;
    const serverUrl = (await invoke('server_url')) as string;

    transport = new BrowserTransport();
    fling = new FlingRoom(transport, myIdentity);
    peers.clear();
    await fling.connect(serverUrl, token, code, {
      onText: (text, from) => addTextMsg(text, from, false),
      onPeerJoined: (id) => {
        if (id === myIdentity) return;
        if (!peers.has(id)) {
          peers.add(id);
          addSysMsg(`${shortId(id)} 加入了房间`);
          renderPeers();
        }
      },
      onPeerLeft: (id) => {
        peers.delete(id);
        addSysMsg(`${shortId(id)} 离开了`);
        renderPeers();
      },
      onFileManifest: (m, from) => addIncomingFileCard(m, from),
      onFileProgress: (id, r, t) => updateFileProgress(id, r / t),
      onFileComplete: (id, m, bytes) => completeIncomingFile(id, m, bytes),
      onFileError: (id, err) => fileCardError(id, err),
    });
    // peers already in the room
    for (const id of transport.remoteIdentities) {
      if (id !== myIdentity) peers.add(id);
    }
    $('#roomTitle').textContent = '房间 ' + code;
    $('#msgList').innerHTML = '';
    renderPeers();
    show('roomScreen');
    setStatus('');
    addSysMsg(`你加入了房间 ${code}，把房间号告诉对方即可开始`);
  } catch (e) {
    setStatus('');
    alert('加入失败：' + e);
  }
}

const shortId = (id: string) => (id.length > 14 ? id.slice(0, 14) + '…' : id);

// ---------- room screen: messages ----------
function addSysMsg(text: string) {
  const list = $('#msgList');
  list.appendChild(el('div', 'sysmsg', text));
  list.scrollTop = list.scrollHeight;
}

function addTextMsg(text: string, from: string, mine: boolean) {
  const list = $('#msgList');
  const wrap = el('div', 'msg ' + (mine ? 'mine' : 'theirs'));
  const meta = el('div', 'meta', `${mine ? '我' : shortId(from)} · ${fmtTime()}`);
  const body = el('div', 'bubble');
  body.innerHTML = esc(text).replace(/\n/g, '<br>');
  wrap.append(meta, body);
  if (!mine) {
    const copy = el('button', 'copybtn', '复制');
    copy.onclick = async () => {
      try {
        await writeClipboard(text);
        copy.textContent = '已复制';
        setTimeout(() => (copy.textContent = '复制'), 1200);
      } catch {
        copy.textContent = '复制失败';
      }
    };
    wrap.appendChild(copy);
    // auto-copy like the CLI
    writeClipboard(text).catch(() => {});
  }
  list.append(wrap);
  list.scrollTop = list.scrollHeight;
}

// ---------- room screen: files ----------
function fileCard(id: string, name: string, size: number, dir: 'in' | 'out', from?: string) {
  const list = $('#msgList');
  const wrap = el('div', 'msg ' + (dir === 'out' ? 'mine' : 'theirs'));
  wrap.id = 'file-' + id;
  wrap.appendChild(el('div', 'meta', `${dir === 'out' ? '我' : shortId(from ?? '')} · ${fmtTime()}`));
  const card = el('div', 'filecard');
  card.innerHTML = `<div class="fname">${esc(name)}</div><div class="fsize">${fmtSize(size)}</div>`;
  const bar = el('div', 'pbar');
  bar.appendChild(el('div', 'pfill'));
  const plabel = el('div', 'plabel', '准备中…');
  card.append(bar, plabel);
  wrap.appendChild(card);
  list.append(wrap);
  list.scrollTop = list.scrollHeight;
  return { bar: bar.firstChild as HTMLElement, plabel };
}

const fileBars = new Map<string, { bar: HTMLElement; plabel: HTMLElement }>();

function addIncomingFileCard(m: FileManifest, from: string) {
  fileBars.set(m.id, fileCard(m.id, m.name, m.size, 'in', from));
}

function updateFileProgress(id: string, ratio: number) {
  const f = fileBars.get(id);
  if (!f) return;
  const pct = Math.floor(ratio * 100);
  f.bar.style.width = pct + '%';
  f.plabel.textContent = pct + '%';
}

function fileCardError(id: string, err: string) {
  const f = fileBars.get(id);
  if (f) f.plabel.textContent = '失败：' + err;
}

async function completeIncomingFile(id: string, m: FileManifest, bytes: Uint8Array) {
  updateFileProgress(id, 1);
  pendingFiles.set(id, { bytes, manifest: m });
  const f = fileBars.get(id);
  if (f) {
    f.plabel.textContent = '接收完成，校验通过';
    const btn = el('button', 'savebtn', '保存到…');
    btn.onclick = () => saveReceivedFile(id);
    f.plabel.appendChild(document.createTextNode(' '));
    f.plabel.appendChild(btn);
  }
  // keep a copy ready; user picks location explicitly
}

async function saveReceivedFile(id: string) {
  const p = pendingFiles.get(id);
  if (!p) return;
  try {
    const path = (await invoke('pick_save_path', { name: p.manifest.name })) as string | null;
    if (!path) return;
    // base64 through IPC (fine for v1 sizes)
    let bin = '';
    const CH = 0x8000;
    for (let i = 0; i < p.bytes.length; i += CH) {
      bin += String.fromCharCode(...p.bytes.subarray(i, i + CH));
    }
    await invoke('write_binary_file', { path, data_b64: btoa(bin) });
    const f = fileBars.get(id);
    if (f) f.plabel.textContent = '已保存';
  } catch (e) {
    alert('保存失败：' + e);
  }
}

// ---------- room screen: sending ----------
async function initRoom() {
  const sendText = async () => {
    const input = $('#textInput') as HTMLInputElement;
    const text = input.value;
    if (!text.trim() || !fling) return;
    if (peers.size === 0 && !(await confirmAlone())) return;
    try {
      await fling.sendText(text);
      addTextMsg(text, myIdentity, true);
      input.value = '';
    } catch (e) {
      alert('发送失败：' + e);
    }
  };
  $('#sendBtn').onclick = sendText;
  ($('#textInput') as HTMLInputElement).onkeydown = (e) => {
    if (e.key === 'Enter') void sendText();
  };

  ($('#fileInput') as HTMLInputElement).addEventListener('change', async (e) => {
    const files = (e.target as HTMLInputElement).files;
    if (!files || files.length === 0 || !fling) return;
    if (peers.size === 0 && !(await confirmAlone())) {
      (e.target as HTMLInputElement).value = '';
      return;
    }
    for (const f of files) {
      const fb = fileCard('out-' + Date.now() + '-' + f.name, f.name, f.size, 'out');
      const key = 'out-' + f.name + f.size;
      fileBars.set(key, fb);
      try {
        await fling.sendFile(wrapWebFile(f), (sent, total) => {
          const pct = Math.floor((sent / total) * 100);
          fb.bar.style.width = pct + '%';
          fb.plabel.textContent = `发送中 ${pct}%`;
        });
        fb.plabel.textContent = '发送完成';
      } catch (err) {
        fb.plabel.textContent = '发送失败：' + (err as Error).message;
      } finally {
        fileBars.delete(key);
      }
    }
    (e.target as HTMLInputElement).value = '';
  });

  $('#leaveBtn').onclick = async () => {
    try {
      await fling?.close();
    } catch {
      /* ignore */
    }
    fling = null;
    peers.clear();
    pendingFiles.clear();
    fileBars.clear();
    await initJoin();
  };
}

async function confirmAlone(): Promise<boolean> {
  return confirm('对方还没加入房间，现在发送对方收不到。仍要发送吗？');
}

// ---------- boot ----------
async function boot() {
  await initSetup();
  await initRoom();
  try {
    const cfg = await invoke('get_config');
    if (cfg) await initJoin();
    else show('setupScreen');
  } catch {
    show('setupScreen');
  }
}

boot();
