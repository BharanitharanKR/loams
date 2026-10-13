//! The Kafka sink (plan Task 9, CN1 Task 6).
//!
//! The default is CN1's **idempotent producer** (`enable.idempotence=true`,
//! `acks=all`): a retry inside the producer never duplicates a record, and a
//! batch re-read from the Fabric after a crash is written again with the same
//! `ce_id`, so a consumer that deduplicates by `ce_id` sees each event once
//! (`effectively_once`, OR-7). `delivery: exactly_once` selects the
//! transactional sink of [`crate::txn`].
//!
//! Each event becomes one binary-mode record (D270, [`crate::record`]) on the
//! instance's one topic. `write` returns once every record of the batch is
//! acknowledged by the cluster, so `flush` has nothing left to wait for. A
//! record over `max_record_bytes`, or an Arrow batch (the manifest declares
//! `bulk.arrow: false`), is `Poison`, checked before anything is sent.

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::join_all;
use loams_connector_sdk::{
    ConnectorError, ErrorClass, EventBatch, Sink, SinkAck, SinkFactory, SinkHandle,
};
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use crate::config::{CALL_TIMEOUT, Connection, SinkDelivery};
use crate::error::kafka_error;
use crate::record::{OutRecord, record_from_event};
use crate::txn::TransactionalKafkaSink;

/// Makes the instance's sink: plain (idempotent) or transactional.
#[derive(Debug)]
pub struct KafkaSinkFactory {
    conn: Arc<Connection>,
}

impl KafkaSinkFactory {
    pub fn new(conn: Arc<Connection>) -> Result<Self, ConnectorError> {
        conn.cfg.sink_topic()?;
        if !conn.cfg.enable_idempotence {
            return Err(ConnectorError::fatal(
                "kafka config: enable_idempotence may not be turned off (the manifest declares sink.idempotent)",
            ));
        }
        if !matches!(conn.cfg.acks.as_str(), "all" | "-1") {
            return Err(ConnectorError::fatal(
                "kafka config: the idempotent producer needs acks=all",
            ));
        }
        Ok(Self { conn })
    }
}

impl SinkFactory for KafkaSinkFactory {
    fn create(&self) -> Result<SinkHandle, ConnectorError> {
        Ok(match self.conn.cfg.delivery {
            SinkDelivery::EffectivelyOnce => {
                SinkHandle::Plain(Box::new(KafkaSink::new(self.conn.clone())))
            }
            SinkDelivery::ExactlyOnce => {
                SinkHandle::Transactional(Box::new(TransactionalKafkaSink::new(self.conn.clone())))
            }
        })
    }
}

/// The producer settings both sinks share.
pub(crate) async fn producer_config(conn: &Connection) -> Result<ClientConfig, ConnectorError> {
    let mut cfg = conn.base().await?;
    cfg.set("enable.idempotence", "true")
        .set("acks", "all")
        .set("message.max.bytes", conn.cfg.max_record_bytes.to_string())
        .set("linger.ms", "5");
    Ok(cfg)
}

/// The records of a batch, refused as a whole (and before anything is sent)
/// when one of them can never be written.
pub(crate) fn records(
    conn: &Connection,
    batch: &EventBatch,
) -> Result<Vec<OutRecord>, ConnectorError> {
    let EventBatch::Events(events) = batch else {
        return Err(ConnectorError::poison(
            "the Kafka sink writes events, not Arrow batches (bulk.arrow: false)",
        ));
    };
    let max = conn.cfg.max_record_bytes;
    events
        .iter()
        .map(|ev| {
            let r = record_from_event(ev);
            if r.size() > max {
                Err(ConnectorError::poison(format!(
                    "event {} is {} bytes, over max_record_bytes ({max})",
                    ev.id,
                    r.size()
                )))
            } else {
                Ok(r)
            }
        })
        .collect()
}

/// Send `records` to `topic` and wait for every acknowledgement. The first
/// error is returned (the others are of the same batch, which is retried or
/// isolated as a whole).
pub(crate) async fn send_all(
    producer: &FutureProducer,
    topic: &str,
    records: &[OutRecord],
    partition: Option<i32>,
) -> Result<SinkAck, ConnectorError> {
    let sends = records.iter().map(|r| {
        let mut fr: FutureRecord<'_, [u8], [u8]> = FutureRecord::to(topic)
            .payload(r.payload.as_ref())
            .headers(r.owned_headers());
        if let Some(k) = &r.key {
            fr = fr.key(k.as_slice());
        }
        if let Some(p) = partition {
            fr = fr.partition(p);
        }
        producer.send(fr, Timeout::After(CALL_TIMEOUT))
    });
    let mut ack = SinkAck::default();
    let mut first_err: Option<ConnectorError> = None;
    for (res, r) in join_all(sends).await.into_iter().zip(records) {
        match res {
            Ok(_) => {
                ack.rows += 1;
                ack.bytes += r.size() as u64;
            }
            Err((e, _)) => {
                let err = kafka_error(&format!("producing to {topic}"), &e);
                // Keep the most telling error: anything over a retryable one.
                let replace = match &first_err {
                    None => true,
                    Some(prev) => prev.class.is_transient() && !err.class.is_transient(),
                };
                if replace {
                    first_err = Some(err);
                }
            }
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(ack),
    }
}

/// The idempotent sink (`effectively_once`).
pub struct KafkaSink {
    conn: Arc<Connection>,
    producer: Option<Arc<FutureProducer>>,
}

impl std::fmt::Debug for KafkaSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSink")
            .field("topics", &self.conn.cfg.topics)
            .finish_non_exhaustive()
    }
}

impl KafkaSink {
    pub fn new(conn: Arc<Connection>) -> Self {
        Self {
            conn,
            producer: None,
        }
    }

    async fn producer(&mut self) -> Result<Arc<FutureProducer>, ConnectorError> {
        if let Some(p) = &self.producer {
            return Ok(p.clone());
        }
        let p: FutureProducer = producer_config(&self.conn)
            .await?
            .create()
            .map_err(|e| kafka_error("creating the producer", &e))?;
        let p = Arc::new(p);
        self.producer = Some(p.clone());
        Ok(p)
    }
}

#[async_trait]
impl Sink for KafkaSink {
    async fn write(&mut self, batch: EventBatch) -> Result<SinkAck, ConnectorError> {
        let records = records(&self.conn, &batch)?;
        let producer = self.producer().await?;
        let topic = self.conn.cfg.sink_topic()?.to_owned();
        let res = send_all(&producer, &topic, &records, None).await;
        if let Err(e) = &res
            && e.class == ErrorClass::Fatal
        {
            // A fatal idempotent-producer error (a fenced or broken producer
            // id) poisons the client: start over with a new one.
            self.producer = None;
        }
        res
    }

    async fn flush(&mut self) -> Result<SinkAck, ConnectorError> {
        // `write` already waited for every acknowledgement.
        Ok(SinkAck::default())
    }
}
