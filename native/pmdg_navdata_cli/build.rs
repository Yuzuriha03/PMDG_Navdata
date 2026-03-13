fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let manifest_path = std::path::Path::new(&manifest_dir);
    let icon_path = manifest_path.join("icon.ico");

    #[cfg(windows)]
    {
        let mut res = winres::WindowsResource::new();
        res.set_icon(icon_path.to_str().expect("icon path is not valid UTF-8"));
        res.compile().expect("failed to compile Windows resources");
    }

    println!(
        "cargo:rerun-if-changed={}",
        std::path::Path::new(&manifest_dir)
            .join("src")
            .join("core")
            .join("magcof")
            .join("WMM.COF")
            .display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        std::path::Path::new(&manifest_dir)
            .join("src")
            .join("core")
            .join("magcof")
            .join("WMMHR.COF")
            .display()
    );
    println!("cargo:rerun-if-changed={}", icon_path.display());
}
