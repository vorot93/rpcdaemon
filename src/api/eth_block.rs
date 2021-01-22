use crate::server::EthApiImpl;
use anyhow::Context;
use ethdb::{SyncStage, TransactionExt};
use ethereum::Header;
use ethereum_tarpc_api::*;
use ethereum_types::{H256, U256};

impl EthApiImpl {
    pub async fn get_canonical_hash(self, block_num: u64) -> anyhow::Result<Option<H256>> {
        Ok(self
            .transaction()
            .await?
            .read_canonical_hash(block_num)
            .await?)
    }

    pub async fn get_best_block(self) -> anyhow::Result<(H256, u64)> {
        let tx = self.transaction().await?;
        let block_num = tx
            .get_stage_progress(SyncStage::Finish)
            .await?
            .context("no block at finish stage")?;

        let hash = tx
            .read_canonical_hash(block_num)
            .await?
            .context("failed to resolve hash for finished block")?;

        Ok((hash, block_num))
    }

    pub async fn get_header(self, hash: H256) -> anyhow::Result<Option<Header>> {
        let tx = self.transaction().await?;
        if let Some(number) = tx.get_block_number(hash).await? {
            return tx.read_header(hash, number).await;
        }

        Ok(None)
    }

    pub async fn get_body(self, hash: H256) -> anyhow::Result<Option<BlockBody>> {
        let tx = self.transaction().await?;
        if let Some(number) = tx
            .get_block_number(hash)
            .await
            .context("failed to resolve hash to block number")?
        {
            if let Some(body) = tx
                .get_storage_body(hash, number)
                .await
                .context("failed to get storage body")?
            {
                let transactions = tx
                    .read_transactions(body.base_tx_id, body.tx_amount)
                    .await
                    .context("failed to read transactions")?;

                return Ok(Some(BlockBody {
                    transactions,
                    ommers: body.uncles,
                }));
            }
        }

        Ok(None)
    }

    pub async fn get_total_difficulty(self, hash: H256) -> anyhow::Result<Option<U256>> {
        let tx = self.transaction().await?;

        if let Some(number) = tx
            .get_block_number(hash)
            .await
            .context("failed to resolve hash to block number")?
        {
            return tx.get_total_difficulty(hash, number).await;
        }

        Ok(None)
    }
}
