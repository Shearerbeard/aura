//! The session-store factory: one backend handing out the capability handles
//! for cross-instance session state ([`ApprovalStore`], [`SkillInvocationStore`],
//! [`JournalStore`], [`RunStore`] and [`EventBus`] from `aura::session_store`,
//! plus the upstream `a2a_server::TaskStore`).
//!
//! See `docs/design/session-storage.md` and
//! `docs/adr/2026-07-08-session-storage.md`.

#[cfg(feature = "session-store-redis")]
mod redis;

#[cfg(feature = "session-store-sqlite")]
mod epoch_journal;

use std::sync::Arc;

use a2a_server::{InMemoryTaskStore, TaskStore};
use async_trait::async_trait;
use aura::session_store::{
    ApprovalStore, EventBus, FileApprovalStore, FileSkillInvocationStore, InMemoryApprovalStore,
    InMemoryEventBus, InMemoryJournalStore, InMemoryRunStore, InMemorySkillInvocationStore,
    JournalStore, RunStore, SessionStoreError, SkillInvocationStore,
};
use aura_config::{FileSessionStoreConfig, SessionStoreBackend, SessionStoreConfig};

#[cfg(feature = "session-store-sqlite")]
use aura_config::session_store::SqliteSessionStoreConfig;

#[cfg(feature = "session-store-redis")]
pub use redis::RedisSessionStore;

#[cfg(feature = "session-store-sqlite")]
pub use epoch_journal::{EpochSqliteJournalStore, JournalRecord};

/// A pluggable backend for cross-instance session state, handing out one handle
/// per capability.
#[async_trait]
pub trait SessionStore: Send + Sync {
    /// Which configured backend this is.
    fn backend(&self) -> SessionStoreBackend;

    /// Durable parked HITL approvals.
    fn approvals(&self) -> Arc<dyn ApprovalStore>;

    /// Durable A2A tasks (the upstream `a2a_server::TaskStore` trait).
    fn tasks(&self) -> Arc<dyn TaskStore>;

    /// Durable per-session skill invocations.
    fn skills(&self) -> Arc<dyn SkillInvocationStore>;

    /// Per-session event journals.
    fn journals(&self) -> Arc<dyn JournalStore>;

    /// Each run's facts.
    fn runs(&self) -> Arc<dyn RunStore>;

    /// Cross-instance pub/sub.
    fn bus(&self) -> Arc<dyn EventBus>;

    /// Backend health check. Implementations may touch the backend (a
    /// round trip, a probe write) but must stay cheap enough to run on
    /// every health probe.
    async fn ping(&self) -> Result<(), SessionStoreError>;
}

/// Construct configured backend, failing fast.
pub async fn build_session_store(
    config: &SessionStoreConfig,
) -> Result<Arc<dyn SessionStore>, SessionStoreError> {
    match config {
        SessionStoreConfig::Memory => Ok(Arc::new(InMemorySessionStore::new())),
        SessionStoreConfig::File(file_config) => {
            // File-backed approvals and skill logs; tasks, journals, runs and
            // the bus stay in memory.
            Ok(Arc::new(FileSessionStore::new(file_config)?))
        }
        #[cfg(feature = "session-store-redis")]
        SessionStoreConfig::Redis(redis_config) => {
            Ok(Arc::new(RedisSessionStore::connect(redis_config).await?))
        }
        #[cfg(not(feature = "session-store-redis"))]
        SessionStoreConfig::Redis(_) => Err(SessionStoreError::BackendUnavailable {
            backend: SessionStoreBackend::Redis.to_string(),
            feature: "session-store-redis".to_string(),
        }),
        #[cfg(feature = "session-store-sqlite")]
        SessionStoreConfig::Sqlite(sqlite_config) => {
            Ok(Arc::new(SqliteSessionStore::connect(sqlite_config).await?))
        }
        #[cfg(not(feature = "session-store-sqlite"))]
        SessionStoreConfig::Sqlite(_) => Err(SessionStoreError::BackendUnavailable {
            backend: SessionStoreBackend::Sqlite.to_string(),
            feature: "session-store-sqlite".to_string(),
        }),
    }
}

/// The default backend.
pub struct InMemorySessionStore {
    approvals: Arc<InMemoryApprovalStore>,
    tasks: Arc<InMemoryTaskStore>,
    skills: Arc<InMemorySkillInvocationStore>,
    journals: Arc<InMemoryJournalStore>,
    runs: Arc<InMemoryRunStore>,
    bus: Arc<InMemoryEventBus>,
}

impl InMemorySessionStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            approvals: Arc::new(InMemoryApprovalStore::new()),
            tasks: Arc::new(InMemoryTaskStore::new()),
            skills: Arc::new(InMemorySkillInvocationStore::new()),
            journals: Arc::new(InMemoryJournalStore::new()),
            runs: Arc::new(InMemoryRunStore::new()),
            bus: Arc::new(InMemoryEventBus::new()),
        }
    }
}

impl Default for InMemorySessionStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SessionStore for InMemorySessionStore {
    fn backend(&self) -> SessionStoreBackend {
        SessionStoreBackend::Memory
    }

    fn approvals(&self) -> Arc<dyn ApprovalStore> {
        self.approvals.clone()
    }

    fn tasks(&self) -> Arc<dyn TaskStore> {
        self.tasks.clone()
    }

    fn skills(&self) -> Arc<dyn SkillInvocationStore> {
        self.skills.clone()
    }

    fn journals(&self) -> Arc<dyn JournalStore> {
        self.journals.clone()
    }

    fn runs(&self) -> Arc<dyn RunStore> {
        self.runs.clone()
    }

    fn bus(&self) -> Arc<dyn EventBus> {
        self.bus.clone()
    }

    async fn ping(&self) -> Result<(), SessionStoreError> {
        Ok(())
    }
}

/// The file-backed backend.
pub struct FileSessionStore {
    approvals: Arc<FileApprovalStore>,
    tasks: Arc<InMemoryTaskStore>,
    skills: Arc<FileSkillInvocationStore>,
    journals: Arc<InMemoryJournalStore>,
    runs: Arc<InMemoryRunStore>,
    bus: Arc<InMemoryEventBus>,
}

impl FileSessionStore {
    /// Open the approval and skill stores under the configured path, failing
    /// fast when either cannot. Tasks, journals, runs and the bus stay in
    /// memory.
    pub fn new(config: &FileSessionStoreConfig) -> Result<Self, SessionStoreError> {
        Ok(Self {
            approvals: Arc::new(FileApprovalStore::open(&config.path)?),
            tasks: Arc::new(InMemoryTaskStore::new()),
            skills: Arc::new(FileSkillInvocationStore::open(
                &config.path,
                config.skills_ttl_secs,
            )?),
            journals: Arc::new(InMemoryJournalStore::new()),
            runs: Arc::new(InMemoryRunStore::new()),
            bus: Arc::new(InMemoryEventBus::new()),
        })
    }
}
#[async_trait]
impl SessionStore for FileSessionStore {
    fn backend(&self) -> SessionStoreBackend {
        SessionStoreBackend::File
    }

    fn approvals(&self) -> Arc<dyn ApprovalStore> {
        self.approvals.clone()
    }

    fn tasks(&self) -> Arc<dyn TaskStore> {
        self.tasks.clone()
    }

    fn skills(&self) -> Arc<dyn SkillInvocationStore> {
        self.skills.clone()
    }

    fn journals(&self) -> Arc<dyn JournalStore> {
        self.journals.clone()
    }

    fn runs(&self) -> Arc<dyn RunStore> {
        self.runs.clone()
    }

    fn bus(&self) -> Arc<dyn EventBus> {
        self.bus.clone()
    }

    async fn ping(&self) -> Result<(), SessionStoreError> {
        self.approvals.probe_writable().await?;
        self.skills.probe_writable().await
    }
}

/// The SQLite-backed backend: durable session journals in one database
/// file; approvals, skill logs, tasks, run facts and the bus stay in
/// memory.
#[cfg(feature = "session-store-sqlite")]
pub struct SqliteSessionStore {
    approvals: Arc<InMemoryApprovalStore>,
    tasks: Arc<InMemoryTaskStore>,
    skills: Arc<InMemorySkillInvocationStore>,
    journals: Arc<EpochSqliteJournalStore>,
    runs: Arc<InMemoryRunStore>,
    bus: Arc<InMemoryEventBus>,
}

#[cfg(feature = "session-store-sqlite")]
impl SqliteSessionStore {
    /// Open the journal database under the configured path, so a
    /// misconfigured path fails at startup rather than on the first
    /// append.
    #[expect(unused_variables, reason = "todo!() body; filled by F3 (P73)")]
    pub async fn connect(config: &SqliteSessionStoreConfig) -> Result<Self, SessionStoreError> {
        todo!()
    }
}

#[cfg(feature = "session-store-sqlite")]
#[async_trait]
impl SessionStore for SqliteSessionStore {
    fn backend(&self) -> SessionStoreBackend {
        SessionStoreBackend::Sqlite
    }

    fn approvals(&self) -> Arc<dyn ApprovalStore> {
        self.approvals.clone()
    }

    fn tasks(&self) -> Arc<dyn TaskStore> {
        self.tasks.clone()
    }

    fn skills(&self) -> Arc<dyn SkillInvocationStore> {
        self.skills.clone()
    }

    fn journals(&self) -> Arc<dyn JournalStore> {
        self.journals.clone()
    }

    fn runs(&self) -> Arc<dyn RunStore> {
        self.runs.clone()
    }

    fn bus(&self) -> Arc<dyn EventBus> {
        self.bus.clone()
    }

    /// A read-only head probe over the journal database; migrations take
    /// the write lock and stay out of health checks.
    async fn ping(&self) -> Result<(), SessionStoreError> {
        todo!()
    }
}
