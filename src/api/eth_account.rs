use crate::server::EthApiImpl;
use ethdb::TransactionExt;
use ethereum_types::{Address, H256, U256};

impl EthApiImpl {
    pub async fn get_balance(self, block: H256, address: Address) -> anyhow::Result<Option<U256>> {
        let tx = self.transaction().await?;
        Ok(tx
            .read_account_state(block, address)
            .await?
            .map(|state| state.balance))
    }

    pub async fn get_nonce(self, block: H256, address: Address) -> anyhow::Result<Option<u64>> {
        let tx = self.transaction().await?;
        Ok(tx
            .read_account_state(block, address)
            .await?
            .map(|state| state.nonce))
    }
}
