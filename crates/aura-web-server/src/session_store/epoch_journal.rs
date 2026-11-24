//! SQLite-backed session journals over Epoch's event streams: the
//! `aura-journal` category in one database file holds one append-only
//! stream per session, keyed by the session id, so a server's journals
//! survive a restart and concurrent writers are fenced by optimistic
//! position checks even across processes sharing the file. This is the
//! server's journal persistence only — approvals, skill logs, tasks, run
//! facts and the bus stay in memory on this backend, and the standalone
//! CLI's store is unchanged.
//!
//! The seam onto Epoch's public surface:
//!
//! | [`JournalStore`] call | Epoch call |
//! | --- | --- |
//! | `append` | `EventStreams::append` with one `JournalRecord` per event |
//! | `read` | `EventStreams::load_stream_page` — inclusive, oldest first, limit counts rows |
//! | `latest` | the same page call with a zero limit: a head-only probe |
//!
//! See `DESIGN.md` beside this module for the type rules, the visibility
//! and seam table, and the exact fill map.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use async_trait::async_trait;
use aura::config::SessionId;
use aura::session_store::{JournalEntry, JournalStore, SessionStoreError};
use aura_events::run::{SessionEvent, SessionEventPayload};
use aura_events::{RunId, SequenceNumber};
use epoch::decider::Event;
use epoch::streams::AppendError;
use epoch::streams::sqlite::{SqliteEventStreams, SqliteStreamsError};
use serde::{Deserialize, Serialize};

/// The stream category every session's journal lives under.
#[expect(dead_code, reason = "read by F1's connect body (P73)")]
const CATEGORY: &str = "aura-journal";

/// The `event_type` every stored journal event carries, under the storage
/// layer's 255-character bound on event types.
const SESSION_EVENT_TYPE: &str = "aura.session_event";

/// The highest position storage represents: stored sequences are signed
/// 64-bit, unlike the journal's `u64` numbering.
#[expect(dead_code, reason = "read by F1's append ceiling branch (P73)")]
const MAX_STORED_POSITION: u64 = i64::MAX as u64;

/// A journal event as stored: the serialized [`SessionEvent`] as raw JSON,
/// self-describing, with no identity of its own — the stream key is the
/// session id, supplied at append. The record accepts any JSON value.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JournalRecord(pub serde_json::Value);

impl Event for JournalRecord {
    /// A journal event has no entity identity of its own; its stream is
    /// named by the session id it is appended under.
    type EntityId = ();

    fn event_type(&self) -> String {
        SESSION_EVENT_TYPE.to_owned()
    }

    fn get_id(&self) -> Self::EntityId {}
}

/// A [`JournalStore`] over one SQLite database file: one stream per
/// session, keyed by the session id.
pub struct EpochSqliteJournalStore {
    #[expect(dead_code, reason = "read by F1/F2's bodies (P73)")]
    streams: SqliteEventStreams<String, JournalRecord>,
}

impl EpochSqliteJournalStore {
    /// Open the database file and apply pending schema migrations, so an
    /// unreadable or misconfigured path fails here, not on the first
    /// append.
    #[expect(unused_variables, reason = "todo!() body; filled by F1 (P73)")]
    pub async fn connect(path: &str) -> Result<Self, SessionStoreError> {
        todo!()
    }
}

/// Map an append failure onto the store error: a version conflict carries
/// both positions — no stream as `None`, an exact position as its number —
/// and every other failure, a lock timeout included, is a backend request
/// failure, never a conflict.
#[expect(dead_code, reason = "called by F1's append body (P73)")]
#[expect(unused_variables, reason = "todo!() body; filled by F1 (P73)")]
fn map_append_error(error: AppendError<SqliteStreamsError>) -> SessionStoreError {
    todo!()
}

#[async_trait]
impl JournalStore for EpochSqliteJournalStore {
    #[expect(unused_variables, reason = "todo!() body; filled by F1 (P73)")]
    async fn append(
        &self,
        session: &SessionId,
        expected: Option<SequenceNumber>,
        run_id: Option<RunId>,
        at: aura_events::Timestamp,
        payload: SessionEventPayload,
    ) -> Result<Arc<SessionEvent>, SessionStoreError> {
        todo!()
    }

    /// A stored record this binary cannot decode into a [`SessionEvent`] is
    /// preserved as [`JournalEntry::Undecodable`]; the read continues past
    /// it rather than failing.
    #[expect(unused_variables, reason = "todo!() body; filled by F2 (P73)")]
    async fn read(
        &self,
        session: &SessionId,
        from: SequenceNumber,
        to: SequenceNumber,
        limit: usize,
    ) -> Result<Vec<JournalEntry>, SessionStoreError> {
        todo!()
    }

    #[expect(unused_variables, reason = "todo!() body; filled by F2 (P73)")]
    async fn latest(
        &self,
        session: &SessionId,
    ) -> Result<Option<SequenceNumber>, SessionStoreError> {
        todo!()
    }
}
