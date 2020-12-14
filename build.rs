fn main() {
    tonic_build::configure()
        .build_server(false)
        .compile(
            &[
                "proto/tg_private/kv.proto",
            ],
            &["proto/tg_private"],
        )
        .unwrap();
}
