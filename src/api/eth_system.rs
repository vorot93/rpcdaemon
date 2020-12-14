use crate::server::EthApiImpl;
use anyhow::Context;
use ethereum_tarpc_api::*;
use futures::future::BoxFuture;
use tarpc::context;

impl EthApi for EthApiImpl {
    type ForksFut = BoxFuture<'static, Result<ForkData, String>>;

    fn forks(self, _: context::Context) -> Self::ForksFut {
        Box::pin(async move {
            let res: anyhow::Result<ForkData> = async move {
                let genesis = self.read_canonical_hash(0).await?.context("no genesis")?;

                let forks = self
                    .read_chain_config(genesis)
                    .await?
                    .context("no chain config")?
                    .gather_forks();

                Ok(ForkData { genesis, forks })
            }
            .await;

            res.map_err(|e: anyhow::Error| e.to_string())
        })
    }
}
