use crate::{config::*, grpc::kv_client::*, server::*};
use anyhow::Context;
use clap::Clap;
use ethereum_tarpc_api::*;
use tarpc::server::{Channel as _, Handler};
use tokio_serde::formats::Bincode;
use tokio_stream::StreamExt;
use tracing_subscriber::EnvFilter;

mod api;
mod config;
#[allow(dead_code)]
mod dbutils;
mod grpc;
mod models;
mod server;

async fn real_main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let opts = Opts::parse();

    let kv_client = KvClient::connect(opts.kv_addr)
        .await
        .context("failed to connect to remote database")?;
    let s = EthApiImpl { kv_client };

    let mut listener = tarpc::serde_transport::tcp::listen(&opts.listen_addr, Bincode::default)
        .await
        .context("failed to start tarpc server")?;
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
