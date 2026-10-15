#!/usr/bin/env python3
"""发版脚本：升级版本号 → 提交 → 打 tag（一键完成）

用法：
  python3 scripts/release.py 0.5.31 "提交信息" [额外文件...]
  python3 scripts/release.py patch "提交信息" [额外文件...]     # 自动 +0.0.1
  python3 scripts/release.py minor "提交信息" [额外文件...]     # 自动 +0.1.0

额外文件：除版本号文件外一起提交的文件（相对仓库根目录路径），
  用 = 指定目标路径（默认与源相同），例如：
  python3 scripts/release.py patch "修xx" apps/desktop/src/taskcard.ts

只改版本号和提交、打 tag，不触发构建（tag 推送后 CI 自动构建发版）。
"""
import json
import re
import subprocess
import sys
import urllib.request

REPO = "tommsoft2011-del/fling"
VERSION_FILES = [
    "apps/desktop/package.json",
    "apps/desktop/src-tauri/tauri.conf.json",
    "apps/desktop/src-tauri/Cargo.toml",
]

sys.path.insert(0, "/opt/muse/skills/custom-connector-setup/bin")
from dynamic_credentials import add_surrogate_to_request, read_json_response


def gh_api(method, url, payload=None):
    data = json.dumps(payload).encode() if payload else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "muse-agent", "Content-Type": "application/json"})
    add_surrogate_to_request(req, "custom.github", allowed_hosts=("api.github.com",))
    with urllib.request.urlopen(req, timeout=30) as r:
        return read_json_response(r)


def current_version():
    s = open("apps/desktop/package.json").read()
    m = re.search(r'"version":\s*"([^"]+)"', s)
    return m.group(1)


def bump(v, part):
    major, minor, patch = (int(x) for x in v.split("."))
    if part == "patch":
        patch += 1
    elif part == "minor":
        minor += 1
        patch = 0
    elif part == "major":
        major += 1
        minor = patch = 0
    else:
        raise ValueError(f"unknown part: {part}")
    return f"{major}.{minor}.{patch}"


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    ver_arg, message = sys.argv[1], sys.argv[2]
    extra_files = sys.argv[3:]  # 额外一起提交的文件
    cur = current_version()
    new = bump(cur, ver_arg) if ver_arg in ("patch", "minor", "major") else ver_arg
    print(f"{cur} → {new}")

    # 1. 改三个文件的版本号
    for p in VERSION_FILES:
        s = open(p).read()
        if p.endswith(".json"):
            s2, n = re.subn(r'"version":\s*"[^"]+"', f'"version": "{new}"', s, count=1)
        else:
            s2, n = re.subn(r'version\s*=\s*"[^"]+"', f'version = "{new}"', s, count=1)
        assert n == 1, f"version not found in {p}"
        open(p, "w").write(s2)

    # 2. 提交（用 gh_commit.py，走 GitHub API）
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "gh_commit", "/home/hatch/workspace/skills/github/bin/gh_commit.py")
    # gh_commit.py 是 CLI，直接调 subprocess 更稳
    files = [f"{p}={p}" for p in VERSION_FILES]
    for ef in extra_files:
        files.append(ef if "=" in ef else f"{ef}={ef}")
    cmd = ["python3", "/home/hatch/workspace/skills/github/bin/gh_commit.py",
           REPO, "--branch", "main", "--message", f"v{new}：{message}"] + files
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        print("commit failed:", out.stderr[-500:])
        sys.exit(1)
    sha = json.loads(out.stdout)["commit_sha"]
    print("committed:", sha[:8])

    # 3. 打 tag
    tag = gh_api("POST", f"https://api.github.com/repos/{REPO}/git/refs",
                 {"ref": f"refs/tags/v{new}", "sha": sha})
    print("tag ok:", tag["ref"])
    print(f"CI 构建中，完成后 release 见 https://github.com/{REPO}/releases/tag/v{new}")


if __name__ == "__main__":
    main()
