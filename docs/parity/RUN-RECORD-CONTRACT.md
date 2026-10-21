# Persisted run-record compatibility

This Rust-side acceptance slice supports LED-01 and QUA-04. It uses fixed,
synthetic JSONL input rather than serializing the current structs to generate
the reader's own test data. It is not a cross-implementation capture, a schema
migration, or evidence that whole-system parity is complete. The existing
cross-implementation manifest and capability status counts are unchanged.

Run the real file-backed tests with:

```sh
cargo test --locked -p vyane-ledger --test run_record_contract
```

`crates/vyane-ledger/tests/fixtures/run-record-contract.jsonl` pins a legacy
record without optional fields/owner, a complete two-attempt failover record,
and a terminal timeout record. The test reads through `JsonlLedger::query`,
compares every JSON field, checks the legacy local-owner default, then appends
through the real writer and compares every persisted field again. It preserves
four-layer targets, attempt order/outcomes, optional usage counters, a session
link, labels, and unknown cost as absence rather than an invented zero. The
fixture contains no prompt preview, credential, endpoint or machine path.

JSONL corruption is handled one original line at a time. A line containing
invalid UTF-8 must be skipped and counted, even when replacing its bad bytes
would make a valid JSON string or an ignored key. The reader must not silently
repair the record. Adjacent valid lines remain readable, a truncated tail is
skipped, multiple bad byte sequences in one line count once, and blank lines
do not count. Valid Unicode, including an actual U+FFFD replacement character,
is valid data; a complete final JSON record needs no trailing newline.

All read tests compare the input bytes afterward. Reading does not rewrite or
repair the ledger. The skipped count belongs to the current scan; limits can
stop a scan before older corruption is reached. These tests do not change
query limits, filters, owner scope or append behavior. They do not establish
fsync/power-loss durability, a total file-size budget, or cross-language schema
equivalence. Those remain separate requirements.
