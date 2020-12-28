use crate::server::EthApiImpl;
use ethereum::Header;
use ethereum_tarpc_api::*;
use ethereum_types::{H256, U256};
use futures::future::BoxFuture;
use std::future::Future;
use tarpc::context;

mod eth_block;
mod eth_system;

trait FutureConverter {
    type Out;

    fn c(self) -> Self::Out;
}

impl<Fut, T> FutureConverter for Fut
where
    Fut: Future<Output = anyhow::Result<T>> + Send + 'static,
    T: Send + 'static,
{
    type Out = BoxFuture<'static, Result<T, String>>;

    fn c(self) -> Self::Out {
        Box::pin(async move {
            self.await.map_err(|e| {
                e.chain()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(". ")
            })
        })
    }
}

impl EthApi for EthApiImpl {
    type ForksFut = BoxFuture<'static, Result<ForkData, String>>;
    type BestBlockFut = BoxFuture<'static, Result<(H256, u64), String>>;
    type CanonicalHashFut = BoxFuture<'static, Result<Option<H256>, String>>;
    type HeaderFut = BoxFuture<'static, Result<Option<Header>, String>>;
    type BodyFut = BoxFuture<'static, Result<Option<BlockBody>, String>>;
    type TotalDifficultyFut = BoxFuture<'static, Result<Option<U256>, String>>;

    fn forks(self, _: context::Context) -> Self::ForksFut {
        self.get_forks().c()
    }

    fn best_block(self, _: context::Context) -> Self::BestBlockFut {
        self.get_best_block().c()
    }

    fn canonical_hash(self, _: context::Context, number: u64) -> Self::CanonicalHashFut {
        self.get_canonical_hash(number).c()
    }

    fn header(self, _: context::Context, hash: H256) -> Self::HeaderFut {
        self.get_header(hash).c()
    }

    fn body(self, _: context::Context, hash: H256) -> Self::BodyFut {
        self.get_body(hash).c()
    }

    fn total_difficulty(self, _: context::Context, hash: H256) -> Self::TotalDifficultyFut {
        self.get_total_difficulty(hash).c()
    }
}
