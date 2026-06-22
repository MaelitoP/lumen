use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = match std::env::var_os("PROTOC") {
        Some(path) => PathBuf::from(path),
        None => protoc_bin_vendored::protoc_bin_path()?,
    };
    println!("cargo:rerun-if-changed=proto");
    println!("cargo:rerun-if-env-changed=PROTOC");

    let mut messages = tonic_prost_build::Config::new();
    messages.protoc_executable(&protoc);
    tonic_prost_build::configure()
        .build_client(false)
        .build_server(false)
        .compile_with_config(messages, &["proto/lumen.proto"], &["proto/"])?;

    if std::env::var_os("CARGO_FEATURE_GRPC").is_some() {
        let mut raft = tonic_prost_build::Config::new();
        raft.protoc_executable(&protoc);
        tonic_prost_build::configure()
            .build_client(true)
            .build_server(true)
            .btree_map(".")
            .extern_path(".lumen.v1", "crate::v1")
            .compile_with_config(raft, &["proto/raft.proto"], &["proto/"])?;
    }

    Ok(())
}
