//! Linearizability of `BucketRefLog` histories (§36 §4.2, §5.2; plan GT1
//! Task 10): `commit` as compare-and-swap on 3 refs and `snapshot(Latest)`
//! from 4 clients, checked with `loams_meta_conformance::linearizability`.
//!
//! Seeds 0..32 run by default; `LOAMS_GIT_LIN_SEEDS=1024` is the nightly
//! setting.

mod support;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use loams_git::testing::mem_reflog;
use loams_git::{
    BucketRefLog, Expect, IdemKey, ObjectId, Principal, ReadAt, RefError, RefLog, RefName, RefTxn,
    RefUpdate,
};
use loams_meta_conformance::linearizability::{Model, Op, Outcome, check};
use loams_store::{FaultRates, FaultyStore, Store};
use object_store::memory::InMemory;

const REFS: usize = 3;
const CLIENTS: u32 = 4;
const OPS_PER_CLIENT: u32 = 300;

/// The values of 3 refs, with compare-and-swap.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct Refs([Option<u32>; REFS]);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Input {
    Cas {
        slot: usize,
        expect: Option<u32>,
        new: u32,
    },
    Read,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Output {
    Done,
    Refused,
    Read([Option<u32>; REFS]),
}

impl Model for Refs {
    type Input = Input;
    type Output = Output;

    fn step(&self, input: &Input) -> (Self, Output) {
        match input {
            Input::Read => (self.clone(), Output::Read(self.0)),
            Input::Cas { slot, expect, new } => {
                if self.0[*slot] == *expect {
                    let mut next = self.clone();
                    next.0[*slot] = Some(*new);
                    (next, Output::Done)
                } else {
                    (self.clone(), Output::Refused)
                }
            }
        }
    }
}

fn oid(value: u32) -> ObjectId {
    let mut bytes = [0x22u8; 20];
    bytes[16..].copy_from_slice(&value.to_be_bytes());
    ObjectId::try_from(&bytes[..]).expect("oid")
}

fn value(oid: &ObjectId) -> u32 {
    let bytes = oid.as_bytes();
    u32::from_be_bytes([bytes[16], bytes[17], bytes[18], bytes[19]])
}

fn slot_name(slot: usize) -> RefName {
    RefName::parse(&format!("refs/heads/r{slot}")).expect("ref")
}

/// A small deterministic generator (xorshift64*).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

type History = Arc<Mutex<Vec<Op<Input, Output>>>>;

/// One client's operations against `log`.
async fn client(id: u32, seed: u64, log: BucketRefLog, clock: Arc<AtomicU64>, history: History) {
    let mut rng = Rng::new(seed ^ (u64::from(id) << 32));
    // What this client last saw in each ref, used as its next expectation.
    let mut seen: [Option<u32>; REFS] = [None; REFS];
    for i in 0..OPS_PER_CLIENT {
        let invoke = clock.fetch_add(1, Ordering::SeqCst);
        let (input, outcome) = if rng.below(2) == 0 {
            let result = log.snapshot(ReadAt::Latest).await;
            let outcome = match result {
                Ok(snap) => {
                    let mut values = [None; REFS];
                    for (slot, value_slot) in values.iter_mut().enumerate() {
                        *value_slot = snap.refs.get(&slot_name(slot)).map(value);
                    }
                    seen = values;
                    Outcome::Ok(Output::Read(values))
                }
                Err(_) => Outcome::Indeterminate,
            };
            (Input::Read, outcome)
        } else {
            let slot = rng.below(REFS as u64) as usize;
            let expect = seen[slot];
            let new = id * 100_000 + i + 1;
            let txn = RefTxn {
                key: IdemKey::parse(&format!("c{id}-{i}")).expect("key"),
                request_digest: [id as u8; 32],
                updates: vec![RefUpdate {
                    name: slot_name(slot),
                    expect: expect.map_or(Expect::Absent, |v| Expect::Oid(oid(v))),
                    new: Some(oid(new)),
                }],
                packs: Vec::new(),
                principal: Principal::parse("lin").expect("principal"),
                scope: None,
                trace: None,
                push_options: Vec::new(),
            };
            let outcome = match log.commit(txn).await {
                Ok(_) => {
                    seen[slot] = Some(new);
                    Outcome::Ok(Output::Done)
                }
                Err(RefError::Rejected(_)) => Outcome::Ok(Output::Refused),
                Err(_) => Outcome::Indeterminate,
            };
            (Input::Cas { slot, expect, new }, outcome)
        };
        let complete = match outcome {
            Outcome::Ok(_) => clock.fetch_add(1, Ordering::SeqCst),
            Outcome::Indeterminate => u64::MAX,
        };
        history.lock().expect("history").push(Op {
            client: id,
            invoke,
            complete,
            input,
            outcome,
        });
    }
}

/// Runs the 4 clients over `sequencers` ref logs on one store (client `c`
/// uses log `c % sequencers`) and checks the history.
async fn run(seed: u64, sequencers: usize, faults: Option<FaultRates>) {
    let faulty = Arc::new(FaultyStore::random(
        Arc::new(InMemory::new()),
        seed,
        FaultRates::none(),
    ));
    let store = Store::new(faulty.clone()).with_sorted_listing(true);
    let mut logs = Vec::new();
    for _ in 0..sequencers {
        logs.push(mem_reflog(store.clone()).await);
    }
    if let Some(rates) = faults {
        faulty.set_rates(rates);
    }
    let clock = Arc::new(AtomicU64::new(0));
    let history: History = Arc::new(Mutex::new(Vec::new()));
    let clients: Vec<_> = (0..CLIENTS)
        .map(|id| {
            let log = logs[id as usize % sequencers].clone();
            tokio::spawn(client(id, seed, log, clock.clone(), history.clone()))
        })
        .collect();
    for client in clients {
        client.await.expect("client");
    }
    faulty.set_rates(FaultRates::none());
    for log in logs {
        log.shutdown().await;
    }
    let history = history.lock().expect("history").clone();
    let done = history
        .iter()
        .filter(|op| matches!(op.outcome, Outcome::Ok(Output::Done)))
        .count();
    assert!(done > 0, "seed {seed}: some commits succeed");
    if let Err(violation) = check(Refs::default(), &history) {
        // `LIN_DEBUG=1` prints the start of the history, by invocation.
        if std::env::var("LIN_DEBUG").is_ok() {
            let mut sorted = history.clone();
            sorted.sort_by_key(|op| op.invoke);
            for op in sorted.iter().take(60) {
                eprintln!(
                    "c{} {}..{} {:?} -> {:?}",
                    op.client, op.invoke, op.complete, op.input, op.outcome
                );
            }
        }
        panic!("seed {seed}, {sequencers} sequencers: not linearizable: {violation}");
    }
}

fn seeds() -> u64 {
    std::env::var("LOAMS_GIT_LIN_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32)
}

#[tokio::test(start_paused = true)]
async fn single_sequencer_linearizable() {
    for seed in 0..seeds() {
        run(seed, 1, None).await;
    }
}

#[tokio::test(start_paused = true)]
async fn two_sequencers_one_store_linearizable() {
    for seed in 0..seeds() {
        run(seed, 2, None).await;
    }
}

#[tokio::test(start_paused = true)]
async fn four_sequencers_under_faults_linearizable() {
    let rates = FaultRates {
        error: 0.05,
        error_after_apply: 0.05,
        precondition: 0.0,
        delay: 0.2,
        max_delay: Duration::from_millis(20),
    };
    for seed in 0..seeds() {
        run(seed, 4, Some(rates)).await;
    }
}
