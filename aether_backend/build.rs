//! Embeds the web console (the static export in `../console/out`, or
//! `$AETHER_CONSOLE_DIR`) into the binary. Without a console build the server
//! serves a page explaining how to make one, so Rust work never needs Node.
//! Release builds set `AETHER_CONSOLE_DIR`, which must hold a build, so a
//! missing one fails the build instead of shipping that page.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::{env, fs};

fn main() {
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-env-changed=AETHER_CONSOLE_DIR");
    let manifest = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("set by cargo"));
    let required = env::var_os("AETHER_CONSOLE_DIR").filter(|d| !d.is_empty()).map(PathBuf::from);
    let dir = required.clone().unwrap_or_else(|| manifest.join("../console/out"));

    let mut files = Vec::new();
    if dir.join("index.html").is_file() {
        println!("cargo::rerun-if-changed={}", dir.display());
        collect(&dir, &dir, &mut files);
        files.sort();
    } else if required.is_some() {
        panic!(
            "AETHER_CONSOLE_DIR={} holds no console build (index.html); run `npm run build` in console/",
            dir.display()
        );
    } else if let Some(parent) = dir.parent().filter(|p| p.is_dir()) {
        // A missing path would rerun this script on every build; watching the
        // console's directory instead picks up its first build.
        println!("cargo::rerun-if-changed={}", parent.display());
    }

    let mut code = String::from("/// (path relative to the export root, contents), sorted by path.\n");
    code.push_str("pub static ASSETS: &[(&str, &[u8])] = &[\n");
    for (relative, absolute) in &files {
        writeln!(code, "    ({relative:?}, include_bytes!({absolute:?})),").expect("writing to a String");
    }
    code.push_str("];\n");
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("set by cargo")).join("console_assets.rs");
    fs::write(out, code).expect("cannot write console_assets.rs");
}

fn collect(root: &Path, dir: &Path, files: &mut Vec<(String, String)>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()));
    for entry in entries {
        let path = entry.expect("directory entry").path();
        if path.is_dir() {
            collect(root, &path, files);
        } else {
            let relative = path.strip_prefix(root).expect("inside root").to_string_lossy().replace('\\', "/");
            let absolute = path.canonicalize().expect("file exists").to_string_lossy().into_owned();
            files.push((relative, absolute));
        }
    }
}
