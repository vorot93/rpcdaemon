# tarpcdaemon
RPC daemon that reads [turbo-geth](https://github.com/ledgerwatch/turbo-geth)'s database and provides access through [common Ethereum tarpc API](https://github.com/rust-ethereum/tarpc-api).


# Running
`env RUST_LOG=tarpcdaemon=info cargo run --release -- --listen-addr="<tarpc listen address>" --kv-addr="grpc://<turbo-geth private api address>"`.

# Options
Run `cargo run --release -- --help` to see the full list of options.
