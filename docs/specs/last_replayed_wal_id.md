# Spec: Last Replayed WAL ID in DbStatus

## Objective

Expose the latest WAL ID that is readable through a `Db` or `DbReader` handle.
This lets users coordinate `WalReader` with the regular read path: after a WAL
file is visible to `WalReader`, they can wait until the corresponding handle's
`DbStatus` reports that the WAL has been processed into the handle's read view.

The new signal is `DbStatus::last_replayed_wal_id`. It is one additional reason
for `DbStatus` to change; it does not replace the existing status signals.
Changes to `durable_seq`, `current_manifest`, and `close_reason` must continue
to publish `DbStatus` updates exactly as they do today.

For `Db`, `last_replayed_wal_id` advances after a WAL flush succeeds. It should
not advance merely when a WAL ID is allocated, because the intended contract is
read availability through the `Db` handle.

For `DbReader`, `last_replayed_wal_id` advances after the reader observes and
replays WAL files into its read state. Because `DbReader` discovers changes by
polling, notifications are bounded by `manifest_poll_interval` and object store
visibility.

When WAL is disabled, `last_replayed_wal_id` remains `0`.

## Tech Stack

- Rust workspace.
- Public API surface: `slatedb::DbStatus`.
- Status producers: `Db`, `DbReader`, and their shared `DbStatusManager`.
- Status consumers: `Db::status`, `Db::subscribe`, `DbReader::status`, and
  `DbReader::subscribe`.

## Commands

```bash
cargo test -p slatedb should_notify_last_replayed_wal_id_watcher_on_wal_flush
cargo test -p slatedb should_subscribe_to_last_replayed_wal_id_updates
cargo test -p slatedb
cargo fmt --check
```

Run targeted test filters separately.

## Project Structure

- `slatedb/src/db_status.rs`: add `DbStatus::last_replayed_wal_id` and a
  monotonic reporting method on `DbStatusManager` without changing existing
  reporting behavior for `durable_seq`, `current_manifest`, or `close_reason`.
- `slatedb/src/wal_buffer.rs`: report the writer-side boundary after each
  successful WAL flush.
- `slatedb/src/db_reader.rs`: report the reader-side boundary after initial WAL
  replay and after polling replay advances.
- `slatedb/src/ops.rs`: document the new status field and the difference between
  writer and reader update timing.
- `slatedb/src/db.rs`: add a writer-side regression test.
- `slatedb/src/db_reader.rs`: add a reader-side regression test.

## Code Style

Add the field as an additive public API extension:

```rust
pub struct DbStatus {
    pub durable_seq: u64,
    pub current_manifest: VersionedManifest,
    pub last_replayed_wal_id: u64,
    pub close_reason: Option<CloseReason>,
}
```

Keep updates monotonic and avoid notifying subscribers when the value does not
advance:

```rust
pub(crate) fn report_last_replayed_wal_id(&self, wal_id: u64) {
    self.tx.send_if_modified(|status| {
        if wal_id > status.last_replayed_wal_id {
            status.last_replayed_wal_id = wal_id;
            true
        } else {
            false
        }
    });
}
```

Existing reporting methods remain independent. `report_durable_seq`,
`report_manifest`, and close reporting still publish status updates when their
fields change, even if `last_replayed_wal_id` is unchanged.

## Testing Strategy

Writer-side regression:

1. Open `Db` with WAL enabled.
2. Subscribe to status.
3. Write a key.
4. Flush with `FlushType::Wal`.
5. Wait for `last_replayed_wal_id` to advance.
6. Assert the value advanced by the expected WAL ID boundary.

Reader-side regression:

1. Open a writer `Db`.
2. Open a `DbReader` with a short `manifest_poll_interval`.
3. Subscribe to reader status and record the initial `last_replayed_wal_id`.
4. Write a key through `Db`.
5. Flush with `FlushType::Wal`.
6. Wait for reader status to report a larger `last_replayed_wal_id`.
7. Assert `reader.get(...)` returns the written value.

WAL-disabled behavior:

- Existing WAL-disabled tests should continue to pass.
- No WAL-disabled path should advance `last_replayed_wal_id`; it remains `0`.

## Boundaries

- Always: Treat `last_replayed_wal_id` as a per-handle read-availability
  boundary, not as a persisted manifest field.
- Always: Keep `last_replayed_wal_id` monotonic for a handle.
- Always: Preserve existing `DbStatus` update triggers. `last_replayed_wal_id`
  adds a new trigger; it must not gate or suppress durable sequence, manifest,
  or close status updates.
- Always: Preserve the existing low-latency write path by avoiding manifest
  updates on every WAL write.
- Ask first: Changing manifest schema or persisting this value.
- Ask first: Renaming existing manifest fields such as `replay_after_wal_id` or
  `next_wal_sst_id`.
- Never: Use `replay_after_wal_id` as the user-facing signal for WAL read
  availability; it remains tied to L0 flush/replay truncation semantics.

## Success Criteria

- `DbStatus` exposes `last_replayed_wal_id`.
- Existing `DbStatus` updates for `durable_seq`, `current_manifest`, and
  `close_reason` continue to notify subscribers independently of
  `last_replayed_wal_id`.
- `Db::status().last_replayed_wal_id` reflects the latest successfully flushed
  WAL ID readable by that `Db`.
- `Db::subscribe()` notifies when a WAL-only flush advances the value.
- `DbReader::status().last_replayed_wal_id` reflects the latest WAL ID replayed
  into that reader's read state.
- `DbReader::subscribe()` notifies after polling/replay advances the value.
- WAL-disabled configurations report `0` permanently.
- Targeted writer and reader tests pass.
- `cargo test -p slatedb` and `cargo fmt --check` pass before completion.

## Open Questions

None. The current decisions are:

- `Db` advances after WAL flush success, not WAL ID allocation.
- WAL-disabled configurations report `0` permanently.
