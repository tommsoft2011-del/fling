use std::process::Command;

fn valid_commit(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn main() {
    println!("cargo:rerun-if-env-changed=GITHUB_SHA");
    let commit = std::env::var("GITHUB_SHA")
        .ok()
        .filter(|value| valid_commit(value))
        .or_else(|| {
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .output()
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| String::from_utf8(output.stdout).ok())
                .map(|value| value.trim().to_string())
                .filter(|value| valid_commit(value))
        })
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=FLING_BUILD_ID={commit}");
    println!("cargo:rustc-env=FLING_BUILD_IDENTITY=v{}+{commit}", std::env::var("CARGO_PKG_VERSION").expect("package version"));
    tauri_build::build();
}
