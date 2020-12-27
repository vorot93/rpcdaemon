use crate::{
    dbutils::*,
    grpc::{kv_client::*, *},
    models::*,
};
use anyhow::{bail, Context};
use arrayref::array_ref;
use async_stream::{stream, try_stream};
use bytes::Bytes;
use ethereum::Header;
use ethereum_types::{H256, U256};
use mem::size_of;
use std::{fmt::Display, mem, sync::Arc};
use tokio::sync::{
    mpsc::{channel, Sender},
    oneshot::{channel as oneshot, Sender as OneshotSender},
    Mutex as AsyncMutex,
};
use tokio_stream::{Stream, StreamExt};
use tonic::{transport::Channel, Streaming};
use tracing::*;

#[derive(Clone, Debug)]
pub struct EthApiImpl {
    pub kv_client: KvClient<Channel>,
}

pub struct Transaction {
    // Invariant: cannot send new message until we process response to it.
    io: Arc<AsyncMutex<(Sender<Cursor>, Streaming<Pair>)>>,
}

pub struct RemoteCursor<'tx> {
    transaction: &'tx Transaction,
    id: u32,

    #[allow(unused)]
    drop_handle: OneshotSender<()>,
}

impl Transaction {
    // - send op open
    // - get cursor id
    pub async fn cursor<B: Display>(&self, bucket_name: B) -> anyhow::Result<RemoteCursor<'_>> {
        let mut s = self.io.lock().await;

        let bucket_name = bucket_name.to_string();

        trace!("Sending request to open cursor");

        s.0.send(Cursor {
            op: Op::Open as i32,
            bucket_name: bucket_name.clone(),
            cursor: Default::default(),
            k: Default::default(),
            v: Default::default(),
        })
        .await?;

        let id = s.1.message().await?.context("no response")?.cursor_id;

        trace!("Opened cursor {}", id);

        drop(s);

        let (drop_handle, drop_rx) = oneshot();

        tokio::spawn({
            let io = self.io.clone();
            async move {
                let _ = drop_rx.await;
                let io = io.lock().await;

                trace!("Closing cursor {}", id);
                let _ =
                    io.0.send(Cursor {
                        op: Op::Close as i32,
                        cursor: id,
                        bucket_name: Default::default(),
                        k: Default::default(),
                        v: Default::default(),
                    })
                    .await;
            }
        });

        Ok(RemoteCursor {
            transaction: self,
            drop_handle,
            id,
        })
    }

    pub async fn get_one<B: Display, K: AsRef<[u8]>>(
        &self,
        bucket_name: B,
        key: &K,
    ) -> anyhow::Result<Bytes> {
        let mut cursor = self.cursor(bucket_name).await?;

        Ok(cursor.seek_exact(key).await?.1)
    }
}

impl<'tx> RemoteCursor<'tx> {
    async fn op(&mut self, cursor: Cursor) -> anyhow::Result<(Bytes, Bytes)> {
        let mut io = self.transaction.io.lock().await;

        io.0.send(cursor).await?;

        let rsp = io.1.message().await?.context("no response")?;

        Ok((rsp.k.into(), rsp.v.into()))
    }

    pub async fn seek_exact<K: AsRef<[u8]>>(&mut self, key: &K) -> anyhow::Result<(Bytes, Bytes)> {
        self.op(Cursor {
            op: Op::SeekExact as i32,
            cursor: self.id,
            k: key.as_ref().to_vec(),

            bucket_name: Default::default(),
            v: Default::default(),
        })
        .await
    }

    pub async fn seek<K: AsRef<[u8]>>(&mut self, key: &K) -> anyhow::Result<(Bytes, Bytes)> {
        self.op(Cursor {
            op: Op::Seek as i32,
            cursor: self.id,
            k: key.as_ref().to_vec(),

            bucket_name: Default::default(),
            v: Default::default(),
        })
        .await
    }

    pub async fn next(&mut self) -> anyhow::Result<(Bytes, Bytes)> {
        self.op(Cursor {
            op: Op::Next as i32,
            cursor: self.id,

            k: Default::default(),
            bucket_name: Default::default(),
            v: Default::default(),
        })
        .await
    }

    fn walk_continue<K: AsRef<[u8]>>(
        k: &[u8],
        fixed_bytes: u64,
        fixed_bits: u64,
        start_key: &K,
        mask: u8,
    ) -> bool {
        !k.is_empty()
            && k.len() as u64 >= fixed_bytes
            && (fixed_bits == 0
                || (k[..fixed_bytes as usize - 1]
                    == start_key.as_ref()[..fixed_bytes as usize - 1])
                    && (k[fixed_bytes as usize - 1] & mask)
                        == (start_key.as_ref()[fixed_bytes as usize - 1] & mask))
    }

    pub fn walk<'a: 'tx, K: AsRef<[u8]> + 'a>(
        &'a mut self,
        start_key: &'a K,
        fixed_bits: u64,
    ) -> impl Stream<Item = anyhow::Result<(Bytes, Bytes)>> + 'a {
        try_stream! {
            let (fixed_bytes, mask) = bytes_mask(fixed_bits);

            let (mut k, mut v) = self.seek(start_key).await?;

            while Self::walk_continue(&k, fixed_bytes, fixed_bits, &start_key, mask) {
                yield (k, v);

                let next = self.next().await?;
                k = next.0;
                v = next.1;
            }
        }
    }
}

impl EthApiImpl {
    pub async fn transaction(&self) -> anyhow::Result<Transaction> {
        trace!("Opening transaction");
        let (sender, mut rx) = channel(1);
        let receiver = self
            .kv_client
            .clone()
            .tx(stream! {
                while let Some(v) = rx.recv().await {
                    yield v;
                }
            })
            .await?
            .into_inner();

        trace!("Acquired transaction receiver");

        Ok(Transaction {
            io: Arc::new(AsyncMutex::new((sender, receiver))),
        })
    }
}

impl Transaction {
    pub async fn read_canonical_hash(&self, block_num: u64) -> anyhow::Result<Option<H256>> {
        let b = self
            .get_one(HEADER_PREFIX, &header_hash_key(block_num))
            .await?;

        const L: usize = H256::len_bytes();

        match b.len() {
            0 => Ok(None),
            L => Ok(Some(H256::from_slice(&*b))),
            other => bail!("invalid length: {}", other),
        }
    }

    pub async fn read_header(&self, hash: H256, number: u64) -> anyhow::Result<Option<Header>> {
        let b = self
            .get_one(HEADER_PREFIX, &number_hash_composite_key(number, hash))
            .await?;

        if b.is_empty() {
            return Ok(None);
        }

        Ok(Some(rlp::decode(&b)?))
    }

    pub async fn get_block_number(&self, hash: H256) -> anyhow::Result<Option<u64>> {
        let b = self
            .get_one(HEADER_NUMBER_PREFIX, &hash.to_fixed_bytes())
            .await?;

        const L: usize = size_of::<u64>();

        match b.len() {
            0 => Ok(None),
            L => Ok(Some(u64::from_be_bytes(*array_ref![b, 0, 8]))),
            other => bail!("invalid length: {}", other),
        }
    }

    pub async fn read_chain_config(&self, genesis: H256) -> anyhow::Result<Option<ChainConfig>> {
        let b = self.get_one(CONFIG_PREFIX, &genesis.as_bytes()).await?;

        if b.is_empty() {
            return Ok(None);
        }

        Ok(Some(serde_json::from_slice(&*b).context("invalid JSON")?))
    }

    pub async fn get_stage_progress(&self, stage: SyncStage) -> anyhow::Result<Option<u64>> {
        let b = self.get_one(SYNC_STAGE_PROGRESS, &stage).await?;

        if b.is_empty() {
            return Ok(None);
        }

        let block_num_byte_len = mem::size_of::<u64>();

        Ok(Some(u64::from_be_bytes(*array_ref![
            b.get(0..block_num_byte_len)
                .context("failed to read block number from bytes")?,
            0,
            mem::size_of::<u64>()
        ])))
    }

    pub async fn get_storage_body(
        &self,
        hash: H256,
        number: u64,
    ) -> anyhow::Result<Option<BodyForStorage>> {
        let b = self
            .get_one(BLOCK_BODY_PREFIX, &number_hash_composite_key(number, hash))
            .await?;

        if b.is_empty() {
            return Ok(None);
        }

        Ok(rlp::decode(&b)?)
    }

    pub async fn read_transactions(
        &self,
        base_tx_id: u64,
        amount: u32,
    ) -> anyhow::Result<Vec<ethereum::Transaction>> {
        Ok(if amount > 0 {
            let mut out = Vec::with_capacity(amount as usize);

            let mut cursor = self.cursor(ETH_TX).await?;

            let start_key = base_tx_id.to_be_bytes();
            let mut walker = Box::pin(cursor.walk(&start_key, 0));

            while let Some((_, tx_rlp)) = walker.try_next().await? {
                out.push(rlp::decode(&tx_rlp).context("broken tx rlp")?);

                if out.len() >= amount as usize {
                    break;
                }
            }

            out
        } else {
            vec![]
        })
    }

    pub async fn get_total_difficulty(
        &self,
        hash: H256,
        number: u64,
    ) -> anyhow::Result<Option<U256>> {
        let b = self
            .get_one(HEADER_PREFIX, &header_td_key(number, hash))
            .await?;

        if b.is_empty() {
            return Ok(None);
        }

        Ok(rlp::decode(&b)?)
    }
}
