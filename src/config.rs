use clap::Clap;

#[derive(Clap, Debug)]
pub struct Opts {
    pub listen_addr: String,
    pub kv_addr: String,
}
