use {
    super::*,
    crate::config::ConfigStorage,
    anyhow::Context as _,
    prost::Message as _,
    richat_proto::geyser::{
        SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateSlot,
        subscribe_update::UpdateOneof,
    },
    serde_json::json,
    solana_account::ReadableAccount,
    std::{
        fs,
        sync::atomic::AtomicUsize,
        time::{Instant as TestInstant, SystemTime, UNIX_EPOCH},
    },
};

struct DurableFixture {
    config: ConfigStorage,
    shutdown: CancellationToken,
    storage: Option<Storage>,
    messages: Option<Messages>,
    sender: Option<Sender>,
    threads: SpawnedThreads,
}

#[test]
fn ownership_acquired_after_unbanked_recovery_is_persisted_and_reopens() -> anyhow::Result<()> {
    let mut fixture = DurableFixture::new()?;
    fixture.persist(None)?;
    fixture.recover()?;
    let sender = fixture.sender.as_mut().unwrap();
    sender.push(DedupMode::SingleSource, "one", account(7, 789))?;
    assert!(sender.slots[&100].source_persisted);
    sender.push(DedupMode::SingleSource, "one", account(7, 790))?;
    assert!(sender.slots[&100].source_persisted);
    fixture.wait_for_source(5)?;
    let (checkpoint, index) = fixture.recover()?;
    assert_eq!((checkpoint, index), (Some(100), 5));
    let sender = fixture.sender.as_mut().unwrap();
    assert_eq!(sender.dedup[&100].source.as_deref(), Some("one"));
    sender.push(DedupMode::SingleSource, "one", account(7, 789))?;
    sender.push(DedupMode::SingleSource, "one", account(7, 790))?;
    assert_eq!(sender.index, 5, "replayed accounts were persisted twice");
    Ok(())
}

impl DurableFixture {
    fn new() -> anyhow::Result<Self> {
        static NEXT_DIRECTORY: AtomicUsize = AtomicUsize::new(0);
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "richat-durable-owner-{}-{nonce}-{sequence}",
            std::process::id()
        ));
        let config: ConfigStorage = serde_json::from_str(
            &json!({
                "path": directory,
                "chunk_target_size": "1B",
                "max_slots": 128,
            })
            .to_string(),
        )?;
        fs::create_dir(&config.path)?;
        let mut fixture = Self {
            config,
            shutdown: CancellationToken::new(),
            storage: None,
            messages: None,
            sender: None,
            threads: Vec::new(),
        };
        let (storage, threads) = Storage::open(
            fixture.config.clone(),
            MessageParserEncoding::Prost,
            fixture.shutdown.clone(),
        )?;
        fixture.storage = Some(storage);
        fixture.threads = threads;
        Ok(fixture)
    }

    fn persist(&self, bank_id: Option<u64>) -> anyhow::Result<()> {
        let storage = self.storage.as_ref().unwrap();
        for (init, slot, head, index, update) in [
            (
                true,
                99,
                0,
                0,
                UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot: 99,
                    status: SlotStatus::SlotFinalized as i32,
                    ..Default::default()
                }),
            ),
            (
                true,
                100,
                1,
                1,
                UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot: 100,
                    bank_id,
                    status: if bank_id.is_some() {
                        SlotStatus::SlotCreatedBank
                    } else {
                        SlotStatus::SlotFirstShredReceived
                    } as i32,
                    ..Default::default()
                }),
            ),
            (
                false,
                100,
                1,
                2,
                UpdateOneof::Account(SubscribeUpdateAccount {
                    slot: 100,
                    bank_id,
                    account: Some(SubscribeUpdateAccountInfo {
                        pubkey: vec![1; 32],
                        owner: vec![2; 32],
                        lamports: 123,
                        write_version: 456,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            ),
        ] {
            let encoded = SubscribeUpdate {
                update_oneof: Some(update),
                created_at: Some(Default::default()),
                ..Default::default()
            }
            .encode_to_vec();
            let message = Message::parse(encoded.into(), MessageParserEncoding::Prost)?.into();
            storage.push_message(init, slot, head, index, message);
        }

        let deadline = TestInstant::now() + Duration::from_secs(5);
        loop {
            let slots = storage.read_slots();
            let mut records = Vec::new();
            for chunk in storage.read_messages_from_index(0, MessageParserEncoding::Prost) {
                for record in chunk? {
                    records.push(record?);
                }
            }
            if slots.get(&99).is_some_and(|slot| slot.finalized)
                && slots.get(&100).is_some_and(|slot| slot.head == 1)
                && records.len() == 3
            {
                anyhow::ensure!(records[0].0 == 0 && records[0].1.slot() == 99);
                anyhow::ensure!(records[1].0 == 1 && records[1].1.bank_id() == bank_id);
                anyhow::ensure!(records[2].0 == 2 && records[2].1.bank_id() == bank_id);
                anyhow::ensure!(matches!(&records[2].1, ParsedMessage::Account(account)
                    if account.slot() == 100 && account.lamports() == 123));
                return Ok(());
            }
            anyhow::ensure!(
                TestInstant::now() < deadline,
                "journal records did not commit"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    fn recover(&mut self) -> anyhow::Result<(Option<Slot>, u64)> {
        self.close()?;
        self.shutdown = CancellationToken::new();
        let (messages, threads) = Messages::new(
            MessageParserEncoding::Prost,
            ConfigChannelInner {
                max_messages: 128,
                max_bytes: 1_048_576,
                storage: Some(self.config.clone()),
            },
            true,
            true,
            true,
            self.shutdown.clone(),
        )?;
        self.messages = Some(messages);
        self.threads = threads;
        let (sender, checkpoint) = self.messages.as_mut().unwrap().to_sender(2)?;
        let result = (checkpoint.load(), sender.index);
        self.sender = Some(sender);
        Ok(result)
    }

    fn persist_with_source(&self) -> anyhow::Result<()> {
        self.persist(None)?;
        self.storage.as_ref().unwrap().push_message_with_source(
            false,
            100,
            999,
            3,
            account(7, 789).into(),
            Some(Arc::from("one")),
        )?;
        self.wait_for_source(4)
    }

    fn wait_for_source(&self, record_count: usize) -> anyhow::Result<()> {
        let storage = self
            .storage
            .as_ref()
            .or_else(|| self.messages.as_ref()?.storage.as_ref())
            .unwrap();
        let deadline = TestInstant::now() + Duration::from_secs(5);
        loop {
            let slots = storage.read_slots();
            let mut records = Vec::new();
            for chunk in storage.read_messages_from_index(0, MessageParserEncoding::Prost) {
                for record in chunk? {
                    records.push(record?);
                }
            }
            if records.len() == record_count
                && slots
                    .get(&100)
                    .is_some_and(|slot| slot.source.as_deref() == Some("one"))
            {
                anyhow::ensure!(
                    slots[&100].head == 1,
                    "source update reset the original replay index"
                );
                anyhow::ensure!(slots[&99].finalized);
                anyhow::ensure!(records[1].1.bank_id().is_none());
                anyhow::ensure!(records[3].1.bank_id() == Some(7));
                return Ok(());
            }
            anyhow::ensure!(
                TestInstant::now() < deadline,
                "slot source ownership did not commit"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    fn close(&mut self) -> anyhow::Result<()> {
        self.shutdown.cancel();
        self.sender.take();
        self.messages.take();
        self.storage.take();
        let outcomes = std::mem::take(&mut self.threads)
            .into_iter()
            .filter_map(|(name, thread)| thread.map(|thread| (name, thread)))
            .map(|(name, thread)| {
                thread
                    .join()
                    .map_err(|_| anyhow::anyhow!("storage thread {name} panicked"))?
                    .with_context(|| format!("storage thread {name} failed"))
            })
            .collect::<Vec<_>>();
        outcomes.into_iter().collect()
    }

    fn finish(mut self) -> anyhow::Result<()> {
        self.close()?;
        fs::remove_dir_all(&self.config.path)?;
        Ok(())
    }
}

impl Drop for DurableFixture {
    fn drop(&mut self) {
        let _ = self.close();
        let _ = fs::remove_dir_all(&self.config.path);
    }
}

#[test]
fn bank_aware_unfinished_journal_recovery_requires_persisted_source_ownership() -> anyhow::Result<()>
{
    let mut fixture = DurableFixture::new()?;
    fixture.persist(Some(7))?;
    let recovery = fixture.recover();
    fixture.finish()?;
    let error = recovery.expect_err("bank-aware unfinished journal recovery must fail explicitly");
    assert!(
        error
            .to_string()
            .contains("bank-aware durable recovery requires persisted source ownership"),
        "unexpected recovery error: {error:#}"
    );
    Ok(())
}

#[test]
fn legacy_unbanked_unfinished_journal_recovery_remains_supported() -> anyhow::Result<()> {
    let mut fixture = DurableFixture::new()?;
    fixture.persist(None)?;
    let recovery = fixture.recover();
    fixture.finish()?;
    assert_eq!(recovery?, (Some(100), 3));
    Ok(())
}

fn account(bank_id: u64, write_version: u64) -> Message {
    account_at(100, bank_id, write_version)
}

fn account_at(slot: Slot, bank_id: u64, write_version: u64) -> Message {
    Message::parse(
        SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Account(SubscribeUpdateAccount {
                slot,
                bank_id: Some(bank_id),
                account: Some(SubscribeUpdateAccountInfo {
                    pubkey: vec![1; 32],
                    owner: vec![2; 32],
                    lamports: write_version,
                    write_version,
                    ..Default::default()
                }),
                ..Default::default()
            })),
            created_at: Some(Default::default()),
            ..Default::default()
        }
        .encode_to_vec()
        .into(),
        MessageParserEncoding::Prost,
    )
    .unwrap()
}

#[test]
fn known_source_survives_restart_and_rejects_other_source_claims() -> anyhow::Result<()> {
    let mut fixture = DurableFixture::new()?;
    fixture.persist_with_source()?;
    assert_eq!(fixture.recover()?, (Some(100), 4));
    let sender = fixture.sender.as_mut().unwrap();
    let error = sender
        .push(DedupMode::SingleSource, "two", account(7, 999))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("recovered slot 100 belongs to source one"),
        "{error:#}"
    );
    sender.push(DedupMode::SingleSource, "one", account(7, 1000))?;
    fixture.wait_for_source(5)?;
    assert_eq!(fixture.recover()?, (Some(100), 5));
    let error = fixture
        .sender
        .as_mut()
        .unwrap()
        .push(DedupMode::SingleSource, "two", account(7, 1001))
        .unwrap_err();
    fixture.finish()?;
    assert!(
        error
            .to_string()
            .contains("recovered slot 100 belongs to source one"),
        "{error:#}"
    );
    Ok(())
}

fn status(slot: Slot, bank_id: Option<u64>, status: SlotStatus) -> Message {
    Message::parse(
        SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot,
                bank_id,
                status: status as i32,
                ..Default::default()
            })),
            created_at: Some(Default::default()),
            ..Default::default()
        }
        .encode_to_vec()
        .into(),
        MessageParserEncoding::Prost,
    )
    .unwrap()
}

#[test]
fn ambiguous_finalization_cannot_skip_unresolved_banks_after_restart() -> anyhow::Result<()> {
    let mut fixture = DurableFixture::new()?;
    assert_eq!(fixture.recover()?, (None, 0));
    let sender = fixture.sender.as_mut().unwrap();
    sender.push(
        DedupMode::SingleSource,
        "one",
        status(99, None, SlotStatus::SlotFinalized),
    )?;
    for bank_id in [11, 22] {
        sender.push(
            DedupMode::SingleSource,
            "one",
            status(100, Some(bank_id), SlotStatus::SlotCreatedBank),
        )?;
        sender.push(DedupMode::SingleSource, "one", account(bank_id, bank_id))?;
    }
    sender.push(
        DedupMode::SingleSource,
        "one",
        status(100, None, SlotStatus::SlotFinalized),
    )?;
    sender.push(
        DedupMode::SingleSource,
        "one",
        status(101, Some(9), SlotStatus::SlotCreatedBank),
    )?;
    sender.push(DedupMode::SingleSource, "one", account_at(101, 9, 9))?;

    let (checkpoint, _) = fixture.recover()?;
    let sender = fixture.sender.as_mut().unwrap();
    let rejection = sender.push(
        DedupMode::SingleSource,
        "one",
        status(101, Some(9), SlotStatus::SlotFinalized),
    );
    assert_eq!(
        (rejection.is_err(), sender.global_replay_from_slot.load()),
        (true, Some(100)),
        "same-owner finalization crossed a recovered unfinished slot"
    );
    let storage = fixture.messages.as_ref().unwrap().storage.as_ref().unwrap();
    let slots = storage.read_slots();
    let mut account_banks = Vec::new();
    for chunk in storage.read_messages_from_index(slots[&100].head, MessageParserEncoding::Prost) {
        for record in chunk? {
            let (_, message) = record?;
            if message.slot() == 100 && matches!(message, ParsedMessage::Account(_)) {
                account_banks.push(message.bank_id());
            }
        }
    }
    assert!(slots[&99].finalized);
    assert_eq!(
        (slots[&100].finalized, checkpoint),
        (false, Some(100)),
        "ambiguous finalization changed the durable root and skipped unresolved banks"
    );
    assert_eq!(slots[&100].source.as_deref(), Some("one"));
    assert_eq!(slots[&101].source.as_deref(), Some("one"));
    assert_eq!(account_banks, vec![Some(11), Some(22)]);

    let sender = fixture.sender.as_mut().unwrap();
    sender.push(
        DedupMode::SingleSource,
        "one",
        status(100, Some(11), SlotStatus::SlotFinalized),
    )?;
    assert_eq!(sender.global_replay_from_slot.load(), Some(101));
    sender.push(
        DedupMode::SingleSource,
        "one",
        status(101, Some(9), SlotStatus::SlotFinalized),
    )?;
    assert_eq!(sender.global_replay_from_slot.load(), Some(102));
    fixture.finish()?;
    Ok(())
}
