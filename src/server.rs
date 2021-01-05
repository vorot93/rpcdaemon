use ethdb::{RemoteKvClient, RemoteTransaction};
use tonic::transport::Channel;
use tracing::*;

#[derive(Clone, Debug)]
pub struct EthApiImpl {
    pub kv_client: RemoteKvClient<Channel>,
}

impl EthApiImpl {
    pub async fn transaction(&self) -> anyhow::Result<RemoteTransaction> {
        trace!("Opening transaction");
        RemoteTransaction::open(self.kv_client.clone()).await
    }
}
