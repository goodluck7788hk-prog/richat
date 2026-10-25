use {
    super::{IndexLocation, Messages, SubscribeClient, SubscribeClientState},
    richat_shared::{jsonrpc::helpers::X_SUBSCRIPTION_ID, mutex_lock},
    std::{
        collections::BTreeMap,
        fmt::Write,
        sync::{
            Arc, Mutex, Weak,
            atomic::{AtomicUsize, Ordering},
        },
    },
    tonic::Request,
};

#[derive(Debug, Clone)]
pub struct BacklogMetrics {
    entries: Arc<Mutex<BTreeMap<u64, Arc<Entry>>>>,
    messages: Messages,
}

#[derive(Debug)]
struct Entry {
    labels: String,
    state: Weak<Mutex<SubscribeClientState>>,
    pending_bytes: Weak<AtomicUsize>,
}

struct Snapshot {
    entry: Arc<Entry>,
    pending_bytes: usize,
    lag: f64,
    replay_active: i32,
    commitment: &'static str,
    state: &'static str,
}

#[derive(Debug)]
pub(super) struct Registration {
    id: u64,
    entries: Arc<Mutex<BTreeMap<u64, Arc<Entry>>>>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        mutex_lock(&self.entries).remove(&self.id);
    }
}

impl BacklogMetrics {
    pub fn new(messages: Messages) -> Self {
        Self {
            entries: Arc::new(Mutex::new(BTreeMap::new())),
            messages,
        }
    }

    pub(super) fn register<T>(
        &self,
        id: u64,
        request: &Request<T>,
        client: &SubscribeClient,
    ) -> Registration {
        let source_ip = request
            .remote_addr()
            .map(|peer| peer.ip().to_canonical().to_string())
            .unwrap_or_else(|| "unknown".to_owned());
        let name = request
            .metadata()
            .get(X_SUBSCRIPTION_ID)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        let mut client_label = String::new();
        for character in name.chars().take(128) {
            match character {
                '\\' => client_label.push_str("\\\\"),
                '"' => client_label.push_str("\\\""),
                '\n' => client_label.push_str("\\n"),
                character => client_label.push(character),
            }
        }
        let entry = Entry {
            labels: format!(
                "source_ip=\"{source_ip}\",subscription_id=\"{id}\",x_subscription_id=\"{client_label}\""
            ),
            state: Arc::downgrade(&client.state),
            pending_bytes: Arc::downgrade(&client.messages_len),
        };
        mutex_lock(&self.entries).insert(id, Arc::new(entry));
        Registration {
            id,
            entries: Arc::clone(&self.entries),
        }
    }

    pub fn append_to(&self, output: &mut String) {
        let snapshots = self.snapshots();
        for (metric, help) in [
            (
                "grpc_subscribe_pending_bytes",
                "Encoded payload bytes waiting in the subscription application queue; excludes HTTP/2, TCP and client processing",
            ),
            (
                "grpc_subscribe_lag_messages",
                "Unfiltered commitment-channel records not yet scanned by this memory reader; -1 when unavailable or replaying disk",
            ),
            (
                "grpc_subscribe_backlog_available",
                "1 when this scrape obtained a valid memory reader lag; 0 when unset, busy, finished or replaying disk",
            ),
            (
                "grpc_subscribe_replay_active",
                "1 for disk replay, 0 for a memory reader, -1 when reader state is unavailable or unset",
            ),
            (
                "grpc_subscribe_backlog_state",
                "Current observation state; one series per active subscription, including unavailable reasons",
            ),
        ] {
            writeln!(output, "# HELP {metric} {help}\n# TYPE {metric} gauge")
                .expect("writing to a String cannot fail");
            for snapshot in &snapshots {
                let labels = &snapshot.entry.labels;
                let commitment = snapshot.commitment;
                match metric {
                    "grpc_subscribe_pending_bytes" => {
                        writeln!(output, "{metric}{{{labels}}} {}", snapshot.pending_bytes,)
                    }
                    "grpc_subscribe_backlog_state" => writeln!(
                        output,
                        "{metric}{{{labels},commitment=\"{commitment}\",state=\"{}\"}} 1",
                        snapshot.state,
                    ),
                    _ => {
                        let value = match metric {
                            "grpc_subscribe_lag_messages" => snapshot.lag,
                            "grpc_subscribe_backlog_available" => {
                                if snapshot.lag >= 0.0 {
                                    1.0
                                } else {
                                    0.0
                                }
                            }
                            "grpc_subscribe_replay_active" => f64::from(snapshot.replay_active),
                            _ => unreachable!("known backlog metric"),
                        };
                        writeln!(
                            output,
                            "{metric}{{{labels},commitment=\"{commitment}\"}} {value}"
                        )
                    }
                }
                .expect("writing to a String cannot fail");
            }
        }
    }

    fn snapshots(&self) -> Vec<Snapshot> {
        let entries: Vec<_> = mutex_lock(&self.entries).values().cloned().collect();
        let mut snapshots = Vec::with_capacity(entries.len());
        for entry in entries {
            let (Some(state), Some(pending_bytes)) =
                (entry.state.upgrade(), entry.pending_bytes.upgrade())
            else {
                continue;
            };
            let cursor = match state.try_lock() {
                Ok(state) => {
                    if state.finished {
                        continue;
                    }
                    Some((state.head, state.commitment))
                }
                Err(_) => None,
            };
            let commitment = match cursor.map(|(_, commitment)| commitment) {
                Some(solana_commitment_config::CommitmentLevel::Processed) => "processed",
                Some(solana_commitment_config::CommitmentLevel::Confirmed) => "confirmed",
                Some(solana_commitment_config::CommitmentLevel::Finalized) => "finalized",
                None => "unknown",
            };
            let (lag, replay_active, state) = match cursor {
                Some((IndexLocation::Memory(head), commitment)) => {
                    let lag = self
                        .messages
                        .get_next_index(commitment)
                        .checked_sub(head)
                        .map_or(-1.0, |lag| lag as f64);
                    (lag, 0, if lag >= 0.0 { "memory" } else { "cursor_ahead" })
                }
                Some((IndexLocation::Storage(_), _)) => (-1.0, 1, "disk"),
                Some((IndexLocation::Unknown, _)) => (-1.0, -1, "unset"),
                None => (-1.0, -1, "busy"),
            };
            snapshots.push(Snapshot {
                entry,
                pending_bytes: pending_bytes.load(Ordering::Relaxed),
                lag,
                replay_active,
                commitment,
                state,
            });
        }
        snapshots
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        mutex_lock(&self.entries).is_empty()
    }
}
