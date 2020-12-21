use crate::server::EthApiImpl;
use anyhow::Context;
use ethereum_tarpc_api::*;

impl EthApiImpl {
    pub async fn get_forks(self) -> anyhow::Result<ForkData> {
        let tx = self.transaction().await?;
        let genesis = tx.read_canonical_hash(0).await?.context("no genesis")?;

        let forks = tx
            .read_chain_config(genesis)
            .await?
            .context("no chain config")?
            .gather_forks();

        Ok(ForkData { genesis, forks })
    }
}
