use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = match std::env::var_os("PROTOC") {
        Some(path) => PathBuf::from(path),
        None => protoc_bin_vendored::protoc_bin_path()?,
    };
    println!("cargo:rerun-if-changed=proto");
    println!("cargo:rerun-if-env-changed=PROTOC");
    prost_build::Config::new()
        .protoc_executable(protoc)
        .compile_protos(&["proto/lumen.proto"], &["proto/"])?;
    Ok(())
}
