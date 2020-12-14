use crate::{
    dbutils::*,
    grpc::{kv_client::*, *},
    models::*,
};
use anyhow::{anyhow, Context};
use bytes::Bytes;
use ethereum_types::H256;
use std::{fmt::Display, sync::Arc};
use tokio::sync::{
    mpsc::{channel, Sender},
    oneshot::{channel as oneshot, Sender as OneshotSender},
    Mutex as AsyncMutex,
};
use tonic::{transport::Channel, Streaming};

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
    bucket_name: String,

    #[allow(unused)]
    drop_handle: OneshotSender<()>,
}

impl Transaction {
    // - send op open
    // - get cursor id
    pub async fn cursor<B: Display>(&self, bucket_name: B) -> anyhow::Result<RemoteCursor<'_>> {
        let mut s = self.io.lock().await;

        let bucket_name = bucket_name.to_string();

        s.0.send(Cursor {
            op: Op::Open as i32,
            bucket_name: bucket_name.clone(),
            cursor: Default::default(),
            k: Default::default(),
            v: Default::default(),
        })
        .await?;

        let id = s.1.message().await?.context("no response")?.cursor_id;

        drop(s);

        let (drop_handle, drop_rx) = oneshot();

        tokio::spawn({
            let io = self.io.clone();
            async move {
                let _ = drop_rx.await;
                let io = io.lock().await;
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
            bucket_name,
        })
    }

    pub async fn get_one<B: Display>(&self, bucket_name: B, key: &[u8]) -> anyhow::Result<Bytes> {
        let mut cursor = self.cursor(bucket_name).await?;

        Ok(cursor.seek_exact(key).await?.1)
    }
}

impl<'tx> RemoteCursor<'tx> {
    pub async fn seek_exact(&mut self, key: &[u8]) -> anyhow::Result<(Bytes, Bytes)> {
        let mut io = self.transaction.io.lock().await;

        io.0.send(Cursor {
            op: Op::SeekExact as i32,
            cursor: self.id,
            bucket_name: self.bucket_name.clone(),
            k: key.to_vec(),
            v: Default::default(),
        })
        .await?;

        let rsp = io.1.message().await?.context("no response")?;

        Ok((rsp.k.into(), rsp.v.into()))
    }
}

impl EthApiImpl {
    async fn transaction(&self) -> anyhow::Result<Transaction> {
        let (sender, rx) = channel(1);
        let receiver = self.kv_client.clone().tx(rx).await?.into_inner();

        Ok(Transaction {
            io: Arc::new(AsyncMutex::new((sender, receiver))),
        })
    }
}

impl EthApiImpl {
    pub async fn read_canonical_hash(&self, block_num: u64) -> anyhow::Result<Option<H256>> {
        let b = self
            .transaction()
            .await?
            .get_one(HEADER_PREFIX, &header_hash_key(block_num))
            .await?;

        const L: usize = H256::len_bytes();

        match b.len() {
            0 => Ok(None),
            L => Ok(Some(H256::from_slice(&*b))),
            other => Err(anyhow!("invalid length: {}", other)),
        }
    }

    pub async fn read_chain_config(&self, genesis: H256) -> anyhow::Result<Option<ChainConfig>> {
        let b = self
            .transaction()
            .await?
            .get_one(CONFIG_PREFIX, genesis.as_bytes())
            .await?;

        if b.is_empty() {
            return Ok(None);
        }

        Ok(Some(serde_json::from_slice(&*b).context("invalid JSON")?))
    }
}
