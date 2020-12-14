use crate::{config::*, grpc::kv_client::*, server::*};
use clap::Clap;
use ethereum_tarpc_api::*;
use tarpc::server::{Channel as _, Handler};
use tokio::stream::StreamExt;
use tokio_serde::formats::Bincode;

mod api;
mod config;
#[allow(dead_code)]
mod dbutils;
mod grpc;
mod models;
mod server;

async fn real_main() -> anyhow::Result<()> {
    let opts = Opts::parse();

    let kv_client = KvClient::connect(opts.kv_addr).await?;
    let s = EthApiImpl { kv_client };

    let mut listener =
        tarpc::serde_transport::tcp::listen(&opts.listen_addr, Bincode::default).await?;
    listener.config_mut().max_frame_length(4294967296);
    let mut l = listener
        // Ignore accept errors.
        .filter_map(|r| r.ok())
        .map(tarpc::server::BaseChannel::with_defaults)
        // Limit channels to 1 per IP.
        .max_channels_per_key(1, |t| t.as_ref().peer_addr().unwrap().ip());
    while let Some(channel) = l.next().await {
        channel.respond_with(s.clone().serve()).execute().await;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tokio_compat_02::FutureExt::compat(real_main()).await
}
