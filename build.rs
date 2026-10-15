fn main() {
    println!("cargo:rerun-if-changed=proto/gtfs-realtime.proto");
    prost_build::Config::new()
        .compile_protos(&["proto/gtfs-realtime.proto"], &["proto/"])
        .expect("compile gtfs-realtime.proto");
}
