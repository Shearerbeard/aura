# DESIGN — SQLite journal session-store backend (P71 type skeleton)

Specification: P70 revision 2, `.review/aura-a1/p70-spec-proposal.md`
(SHA256 `56a0bb59dfa9a543428fe15076e372a835e4a1d3d1562bb121cf01d99a2d188a`,
coordinator-approved). Input commit `f76caf1f193189b98a33058d2cb5585405ce93f4`.
Dependency: `epoch-journal = "=0.2.0-beta.1"` (lib `epoch`),
`default-features = false`, feature `sqlite` only — the staged cache
copy at `.review/aura-a1/epoch-journal-0.2.0-beta.1/` is the version
this skeleton's types and paths were checked against.

Scope: server-only journal persistence. The standalone CLI keeps its
hardcoded in-memory store; `RunStore::finish`, run-facts persistence,
HA, park/reify and crash/power-loss certification are all excluded by
the card. Every behavior body below is a named hole (F1–F4); nothing
here implements parsing, assembly, IO, mapping, or decoding. Tests are
P72's — this record names the surfaces they need and nothing more.

## Type-to-rule map

| Type | One business rule | Invalid state it forbids |
| --- | --- | --- |
| `JournalRecord` | A stored journal event is the serialized `SessionEvent` as raw JSON, self-describing, with no identity of its own. | An event that names its own stream (`EntityId = ()`); a second discriminator or identity field. |
| `EpochSqliteJournalStore` | One session's journal is one append-only stream under the `aura-journal` category, keyed by the session id, densely numbered from 1. | An interleaved double-writer append (conflict); a position handed out twice; a gap in a session's numbering. |
| `SqliteSessionStore` | Journals are the only durable capability; approvals, tasks, skills, runs and the bus stay process-local. | A sqlite selection that claims durable approvals, tasks, skill logs, run facts, or bus state. |
| `SqliteSessionStoreConfig` | The sqlite backend is located by exactly one required path. | A sqlite selection with no path — a parse error, never a runtime fallback or invented default. |
| `SessionStoreBackend::Sqlite` / `SessionStoreConfig::Sqlite` | The backend tag `sqlite` (`AURA_SESSION_STORE=sqlite`). | — (identity variants; constant mappings, not holes). |

`JournalRecord` is transparent over any JSON value on purpose. The
storage layer's `json_valid` CHECK bounds what can be stored; the
record adds no second gate. Whether a row still decodes into a
`SessionEvent` is the read path's question (F2). The real-world case
is a future binary's new payload variant: it must surface as
`JournalEntry::Undecodable`, not a failed page.

## Visibility and seam table

| Item | Visibility | Reaches into | Notes |
| --- | --- | --- | --- |
| `epoch_journal` module | `pub(crate)` items re-exported from `session_store.rs` | `aura::session_store` (JournalStore, JournalEntry, SessionStoreError), `aura::config::SessionId`, `aura_events`, `epoch::streams::sqlite` | Feature-gated `session-store-sqlite`; not compiled otherwise. |
| `JournalRecord` + field | `pub` (re-exported) | `serde_json::Value` | P72's backend-local fixtures build records directly over a second `SqliteEventStreams` on the same file (C3 loose record, C4 ceiling rows). |
| `EpochSqliteJournalStore::connect(path)` | `pub` (re-exported) | `SqliteEventStreams::<String, JournalRecord>::open` + `migrate()` | Production entry is F3's `SqliteSessionStore::connect`; also the direct-adapter test seam. |
| `map_append_error` | private | `AppendError<SqliteStreamsError>` → `SessionStoreError` | Same-file `#[cfg(test)] mod tests` (P72) reaches it for the C1 pure mapping oracle. |
| `CATEGORY`, `MAX_STORED_POSITION` | private consts | — | Values are contract; P72 pins them behaviorally with literals, independently of the constants. |
| `SqliteSessionStore` + `connect` | `pub`, cfg-gated | in-memory capabilities + `EpochSqliteJournalStore` | Mirrors `FileSessionStore`'s shape with the journal handle swapped. |
| `build_session_store` arms | pub fn, both cfg states | `SqliteSessionStore::connect` / `BackendUnavailable` | Feature-off arm is real and final: P74 asserts it rejects a sqlite selection. |
| `SqliteSessionStoreConfig` | `pub` in `aura_config::session_store` | `ConfigError` | Not re-exported at the crate root by assignment; consumers use the module path. |
| Epoch surface used | — | `EventStreams::{append, load_stream_page}`, `SqliteEventStreams::{open, migrate}`, `Event` with `EntityId = ()` | `load_stream`, `load_stream_from`, `load_category` are not called by the adapter, by design. |

## Exact hole map (F1–F4)

| Hole | Fill unit | Marker | The fill owns |
| --- | --- | --- | --- |
| `EpochSqliteJournalStore::connect` | F1 | `#[expect(unused_variables)]` | `open(path, "aura-journal")` then `migrate()`; open/migrate failures map into the startup `Connect` family, mirroring the redis backend. |
| `JournalStore::append` | F1 | `#[expect(unused_variables)]` | Mint the next seq from `expected` (`None`→FIRST, `Some(n)`→`n.next()`); for expected positions at/above `MAX_STORED_POSITION` read the actual head first — mismatch→`Conflict { expected, actual }`, matching maximum→`Request` (exhausted storage), no append attempted; stamp the seq into the full `SessionEvent`, encode to `JournalRecord`, `EventBatch`, map `expected` (`None`→`NoStream`, `Some(n)`→`Exact`), append, and hold that the returned position equals the stamped seq. Ordinary writes never use the read as OCC — Epoch owns the atomic check/append. |
| `map_append_error` | F1 | `#[expect(dead_code)]` + `#[expect(unused_variables)]` | `Conflict`→`Conflict { expected, actual }` with `NoStream`↔`None` and `Exact`↔`Some`; `LockTimeout`→`Request`, never `Conflict`; `Backend`→`Request`. |
| `JournalStore::read` | F2 | `#[expect(unused_variables)]` | `load_stream_page(from..=to, limit)` (`from`/`to` are nonzero, so `StreamSequence::new` is infallible); entries oldest-first with seq derived positionally (`from + index`); each record's `Value`→`SessionEvent` decode — failure yields `JournalEntry::Undecodable { seq, raw }` where `raw` is the canonical re-serialized stored JSON, and the read continues past it; read errors: `Serialization`→`Decode`, everything else→`Request`. |
| `JournalStore::latest` | F2 | `#[expect(unused_variables)]` | `load_stream_page(1..=1, 0)` — a zero limit selects nothing while observing the head — then `NoStream`→`None`, `Exact(h)`→`Some(h)`. |
| `SqliteSessionStore::connect` | F3 | `#[expect(unused_variables)]` | Assemble the in-memory capabilities and open the journal store under `config.path`. |
| `SessionStore::ping` | F3 | none: zero-arg hole, inventory-tracked | A read-only head-probe page read through the journals handle; never `migrate()`, which takes the write lock. |
| `SqliteSessionStoreConfig::from_env` | F4 | none: zero-arg hole, inventory-tracked | Read `AURA_SESSION_STORE_SQLITE_PATH`; required — unset/blank is `ConfigError::Validation` naming the variable, like the file backend's required path. |

Real in this skeleton, no hole: `JournalRecord` and its `Event` impl
(constant `event_type`, unit `EntityId`), the store structs and
capability getters, `backend()`/`Display`/`FromStr` arms, the
`SessionStoreConfig::from_env` dispatch arm, both
`build_session_store` arms, the manifest edits.

Marker-sweep duties: each fill removes its own `#[expect]`s by hand —
a filled body that still ignores an argument keeps its marker quiet,
so sweep rather than waiting for the unfulfilled-expectation warning.
The `dead_code` expectations on `CATEGORY`, `MAX_STORED_POSITION`,
`map_append_error`, and the `streams` field stop firing the moment
F1/F2's bodies land; sweep them in the same unit. Final scoped checks
deny `clippy::todo` only once no authorized hole remains under the
command's scope (P73 completion).

## Residual risks (named)

- **R1 — 255-char stream key.** The storage CHECK bounds `stream_key`
  at 255 characters; a client-supplied session id past it fails the
  append as `Request`. No truncation or hashing to fit (C2): two
  distinct long ids sharing a 255-char prefix must stay distinct
  streams.
- **R2 — signed-64-bit position ceiling.** Stored sequences cap at
  `MAX_STORED_POSITION` (`i64::MAX`), not a quota. The last
  representable append (head max−1 → max) succeeds; the next fails
  `Request` without changing rows or head. No truncating casts; the
  read side's u64 recheck (owned by Epoch's page call) keeps
  above-ceiling cursors from aliasing the maximum stored row.
- **R3 — `Undecodable.raw` is the canonical re-serialized JSON** of the
  stored value, not the stored byte string; Epoch's public API never
  hands back stored bytes (approved decision D3). Byte identity is
  not claimed anywhere.
- **R4 — lock timeout is never a conflict** (C1). A busy timeout
  mapped to `Conflict` would permanently fence a healthy journal.
- **R5 — the in-memory "forgotten session takes `expected` as where it
  stands" path never arises**: SQLite evicts nothing and always
  remembers. A documented difference from `InMemoryJournalStore`, not
  code.
- **R6 — single-file writers, no HA.** Cross-process fencing holds for
  writers sharing one database file; this is not a multi-node claim.
  Power-loss certification stays disclaimed, as Epoch's own posture
  does — the adapter inherits it, nothing stronger.
- **R7 — `ping` is a read-only probe.** Migrations take the write lock
  and stay out of health checks.
- **R8 — undecodable fixtures must fail at the top level.** Unknown
  nested lifecycle/agent tags decode as their `Unknown` variants and
  do not count; C3's fixture needs a top-level payload kind this
  binary's `SessionEventPayload` cannot decode at all.
- **R9 — the OCC check is Epoch's**, on the append path. The adapter's
  ceiling read is not a second OCC for ordinary writes.

## P72 test-visible surfaces

- Public: `build_session_store` (configured factory, including the
  reopen/replay oracle through `types::build_runtime`),
  `EpochSqliteJournalStore::connect`, `JournalRecord`'s public field,
  `SqliteSessionStoreConfig`.
- Private, through the `#[cfg(test)] epoch_journal::tests` module (same
  file, so private items are reachable): `map_append_error` for C1's
  pure mapping oracle, plus record fixtures.
- Backend-local boundary fixtures P72 must construct itself, because
  normal ingress cannot produce the stored values: C3's loose record
  over a second `SqliteEventStreams` on the same file, and C4's
  stored-ceiling rows. Their scope is exactly this store's own tables;
  they are representation tests, not production-state claims.
- Commands: `cargo test -p aura-web-server --features
  session-store-sqlite --test epoch_journal`, and the lib-test filter
  `cargo test -p aura-web-server --features session-store-sqlite
  epoch_journal::tests::`.
- No unsupported getters exist: the adapter exposes no `head()`, no
  `streams()`, no validation surface — tests reach the same public
  Epoch calls the adapter does. P72's F4 tests also extend the existing
  `clear_env` helper in `aura-config`'s test module with
  `AURA_SESSION_STORE_SQLITE_PATH`; that test-module edit belongs to
  the test card, not this skeleton.
