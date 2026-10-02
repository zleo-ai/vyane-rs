# WP-05 feedback

WP-05 was implemented **as specified, within the frozen `vyane-core` interface** —
no core change was required. This file records the one design gap worth surfacing
for a future revision, plus the documented conventions chosen where the spec left
latitude. Nothing here is a blocker.

## Observation: the corrupt-line count is not reachable through `dyn Ledger`

WP-05 requires corrupt ledger lines to be "skipped with a counted warning … a
counter you can assert in tests". The frozen trait is:

```rust
async fn query(&self, query: RunQuery) -> Result<Vec<RunRecord>>;
```

The return type carries no side-channel for the skipped-line count. I surfaced it
as a method on the **concrete** type only:

```rust
impl JsonlLedger {
    pub fn skipped_lines(&self) -> u64; // reflects the most recent `query`
}
```

That satisfies the acceptance test (which holds a `JsonlLedger` directly) but has
a real limitation: per `ARCHITECTURE.md`, the kernel holds the ledger as
`Arc<dyn Ledger>`, behind which `skipped_lines()` is invisible. So corruption
monitoring works for tests and for any code that downcasts to the concrete type,
but not through the trait object.

**No change was made to the frozen trait.** If a future WP wants corruption
visibility through the trait, two shapes would work without breaking object
safety:

- a separate `fn query_stats(&self) -> QueryStats` on `Ledger`, or
- a `query_with_stats` variant returning `Result<(Vec<RunRecord>, QueryStats)>`.

Either is additive; I did not pursue it here to stay within the frozen interface.

## Documented conventions (within spec latitude)

These are implementation choices the spec left open; recorded so they are not
surprising:

- **`since` filters on `started_at`** (`>=`), inclusive. A run whose `started_at`
  is at or after `since` matches. `finished_at` is not used.
- **`owner` matches exactly.** Owner is a scope, not a search term, so `alice`
  and `ALICE` are distinct owners.
- **`limit` with `None` returns every match.** The spec calls `None` an
  "implementation default"; returning all matches is that default.
- **推理／缓存计费**（`cost`）：推理子计数必须包含在 `output_tokens` 中，
  缓存子计数必须包含在 `input_tokens` 中。`ModelPricing` 设置
  `reasoning_per_1m`／`cache_read_per_1m` 时，先从对应总量中扣除子计数，
  再用专用费率替换该部分的普通费率。超出父计数的子计数钳制到父计数。
  上游分别报告计数时，调用方须先归一化。内置价格表没有专用费率，
  因此仍只按输入总量与输出总量计费。
- **Builtin price table is illustrative.** Public list prices (USD per 1M tokens)
  for a few well-known models, best-effort and clearly marked as will-go-stale;
  `PriceTable::with_overrides` lets config win, as required.
- **`query` takes no lock.** The advisory lock guards appends only (per the spec
  notes). A read that races an in-flight append simply treats the incomplete
  trailing line as corrupt and skips it — the graceful, counted path.
