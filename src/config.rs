use clap::Clap;

#[derive(Clap, Debug)]
#[clap(name = "tarpcdaemon", about = "RPC daemon for turbo-geth.")]
pub struct Opts {
    #[clap(long, env)]
    pub listen_addr: String,
    #[clap(long, env)]
    pub kv_addr: String,
}
