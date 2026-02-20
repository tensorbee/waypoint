fn main() {
    // Git commit hash
    let git_hash = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                String::from_utf8(o.stdout).ok()
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=GIT_HASH={}", git_hash.trim());

    // Build timestamp
    let now = chrono::Utc::now();
    println!(
        "cargo:rustc-env=BUILD_TIME={}",
        now.format("%Y-%m-%d")
    );
}
