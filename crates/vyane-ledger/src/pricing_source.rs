//! Queryable unit-price provenance.
//!
//! [`crate::PriceTable`] is a separate, older flat list of model ids
//! (`gpt-4o`, `claude-3-5-sonnet-20241022`, `gemini-2.0-flash`, and the
//! other builtin ids). This module keeps its own read-only copy of a public
//! provider/model rate list — 11 providers and 45 model keys — plus the
//! source URL, check date, and status for each row. The two number sets
//! coexist.
//!
//! `PriceSource` unit prices are not the billing source for
//! [`crate::PriceTable`]. [`crate::cost::PriceTable::builtins`] and
//! [`crate::cost::PriceTable::estimate`] do not read this module. Nothing
//! here writes into that table or into
//! [`vyane_core::estimate_configured_cost`]. Cost display and the command
//! line do not call this lookup.
//!
//! The copied rates are the public provider/model list at `zleo-ai/vyane`
//! commit `6465bb0562944f81f28add181fa89e6b002da79e`, together with the
//! source metadata from that same revision. Unknown unit prices are `None`,
//! never `0.0`. A missing quote must not be treated as zero spend. A present
//! `0.0` is a copied zero rate (a subscription row, a local row, or a promo
//! row). `counts_as_real_spend` is true only when `spend_class` is `metered`,
//! `status` is `verified`, and both the input and output unit prices are
//! present. [`real_metered_spend_usd`] returns USD only for those rows.
//!
//! [`CHECKED_TODAY`] is the frozen date `2026-09-12`. It is not the current
//! day, and it does not move when this crate is built. This module does not
//! refresh quotes over the network.

use std::fmt;

use serde::Serialize;

/// Frozen check date for rows marked on that day.
///
/// This is not the date this module was written, and it is not the process's
/// current date.
pub const CHECKED_TODAY: &str = "2026-09-12";

/// Date recorded for an OpenRouter catalogue sync.
///
/// Rows in this module that name a check date use [`CHECKED_TODAY`]. This
/// constant stays on the public surface and is not applied as `checked_at`.
pub const OPENROUTER_SYNC_CHECKED: &str = "2026-09-02";

const URL_ANTHROPIC: &str = "https://docs.anthropic.com/en/docs/about-claude/pricing";
const URL_OPENAI: &str = "https://platform.openai.com/docs/pricing";
const URL_GEMINI: &str = "https://ai.google.dev/gemini-api/docs/pricing";
const URL_DEEPSEEK: &str = "https://api-docs.deepseek.com/quick_start/pricing";
const URL_OPENROUTER_MODELS: &str = "https://openrouter.ai/api/v1/models";
const URL_ALIYUN_PLAN: &str = "https://help.aliyun.com/zh/model-studio/coding-plan";
const URL_MIMO_PLAN: &str = "https://mimo.mi.com/docs/en-US/price/token-plan";

// Provider sets copied with the rate list. `agy` is a priced provider and is
// in neither set, so a known agy row is metered and unverified.
const SUBSCRIPTION_PROVIDERS: &[&str] = &["aliyun", "mimo", "kimi-code", "grok-build"];
const LOCAL_PROVIDERS: &[&str] = &["ollama"];

/// `verified`, `estimated`, or `unverified`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PriceStatus {
    /// The unit prices were matched to the recorded source page.
    Verified,
    /// The unit prices are a kept estimate.
    Estimated,
    /// No source confirmation was recorded.
    Unverified,
}

impl PriceStatus {
    /// The status literal: `verified`, `estimated`, or `unverified`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::Estimated => "estimated",
            Self::Unverified => "unverified",
        }
    }
}

impl fmt::Display for PriceStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// `metered`, `subscription`, `local`, or `unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SpendClass {
    /// Token prices are a metered API rate.
    Metered,
    /// Token prices are covered by a subscription plan.
    Subscription,
    /// The runtime is local.
    Local,
    /// The provider or model is not in the copied rate list.
    Unknown,
}

impl SpendClass {
    /// The spend-class literal.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Metered => "metered",
            Self::Subscription => "subscription",
            Self::Local => "local",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for SpendClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// One queryable provenance row.
///
/// Unit prices are USD per 1,000,000 tokens. `None` means the copied list has
/// no quote for that component. It is not a zero price.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PriceSource {
    /// Provider id after a `provider/model` split.
    pub provider: String,
    /// Requested model, or the resolved key when the request omitted one.
    pub model: String,
    /// Key in the copied rate list. Empty when the row is unknown.
    pub model_key: String,
    /// USD per 1,000,000 input tokens, or `None` when unquoted.
    pub input_per_million: Option<f64>,
    /// USD per 1,000,000 output tokens, or `None` when unquoted.
    pub output_per_million: Option<f64>,
    /// USD per 1,000,000 cache-read tokens, or `None` when unquoted.
    pub cache_read_per_million: Option<f64>,
    /// USD per 1,000,000 cache-write tokens, or `None` when unquoted.
    pub cache_write_per_million: Option<f64>,
    /// Recorded source page. Empty when none was recorded.
    pub source_url: String,
    /// Recorded check date (`YYYY-MM-DD`). Empty when none was recorded.
    pub checked_at: String,
    /// `verified`, `estimated`, or `unverified`.
    pub status: PriceStatus,
    /// `metered`, `subscription`, `local`, or `unknown`.
    pub spend_class: SpendClass,
    /// True only for a verified metered row with both input and output prices.
    pub counts_as_real_spend: bool,
    /// Provenance note. Empty when the copied metadata has none.
    pub note: String,
}

impl PriceSource {
    /// JSON object with the same fields as this row.
    ///
    /// Missing unit prices are JSON `null`, not `0.0`.
    #[must_use]
    pub fn to_dict(&self) -> serde_json::Value {
        serde_json::json!({
            "provider": self.provider,
            "model": self.model,
            "model_key": self.model_key,
            "input_per_million": self.input_per_million,
            "output_per_million": self.output_per_million,
            "cache_read_per_million": self.cache_read_per_million,
            "cache_write_per_million": self.cache_write_per_million,
            "source_url": self.source_url,
            "checked_at": self.checked_at,
            "status": self.status.as_str(),
            "spend_class": self.spend_class.as_str(),
            "counts_as_real_spend": self.counts_as_real_spend,
            "note": self.note,
        })
    }
}

#[derive(Clone, Copy)]
struct UnitPrice {
    input: f64,
    output: f64,
    cache_read: Option<f64>,
    cache_write: Option<f64>,
}

#[derive(Clone, Copy)]
struct ModelRow {
    key: &'static str,
    price: UnitPrice,
}

struct ProviderRows {
    provider: &'static str,
    models: &'static [ModelRow],
}

#[derive(Clone, Copy)]
struct SourceMeta {
    source_url: &'static str,
    checked_at: &'static str,
    status: PriceStatus,
    note: &'static str,
}

struct ProviderMetaRow {
    provider: &'static str,
    meta: SourceMeta,
}

struct RowMeta {
    provider: &'static str,
    model: &'static str,
    meta: SourceMeta,
}

const fn rates(input: f64, output: f64) -> UnitPrice {
    UnitPrice {
        input,
        output,
        cache_read: None,
        cache_write: None,
    }
}

const fn rates_with_read(input: f64, output: f64, cache_read: f64) -> UnitPrice {
    UnitPrice {
        input,
        output,
        cache_read: Some(cache_read),
        cache_write: None,
    }
}

const fn rates_with_cache(input: f64, output: f64, cache_read: f64, cache_write: f64) -> UnitPrice {
    UnitPrice {
        input,
        output,
        cache_read: Some(cache_read),
        cache_write: Some(cache_write),
    }
}

const fn row(key: &'static str, price: UnitPrice) -> ModelRow {
    ModelRow { key, price }
}

// price-catalog-start
static CATALOG: &[ProviderRows] = &[
    ProviderRows {
        provider: "codex",
        models: &[
            row("default", rates(2.0, 8.0)),
            row("gpt-6.1-sol", rates_with_cache(2.0, 10.0, 0.10, 2.50)),
            row("gpt-6-astra", rates_with_cache(10.0, 50.0, 1.00, 12.50)),
            row("gpt-4.1", rates_with_read(2.0, 8.0, 0.5)),
            row("gpt-4.1-mini", rates_with_read(0.4, 1.6, 0.1)),
            row("gpt-4.1-nano", rates_with_read(0.1, 0.4, 0.025)),
            row("gpt-5.5", rates_with_read(5.0, 30.0, 0.5)),
            row("gpt-5.6-terra", rates_with_cache(2.0, 12.0, 0.2, 2.5)),
            row("gpt-5.6-sol", rates_with_cache(2.0, 10.0, 0.2, 2.5)),
            row("gpt-5.6-luna", rates_with_cache(0.2, 1.2, 0.02, 0.25)),
            row("o3", rates_with_read(2.0, 8.0, 0.5)),
            row("o4-mini", rates_with_read(1.1, 4.4, 0.275)),
        ],
    },
    ProviderRows {
        provider: "gemini",
        models: &[
            row("default", rates(1.25, 10.0)),
            row("gemini-2.5-pro", rates_with_cache(1.25, 10.0, 0.125, 0.375)),
            row(
                "gemini-2.5-flash",
                rates_with_cache(0.3, 2.5, 0.03, 0.0833333333),
            ),
            row(
                "gemini-3.1-pro-preview",
                rates_with_cache(2.0, 12.0, 0.2, 0.375),
            ),
        ],
    },
    ProviderRows {
        provider: "claude",
        models: &[
            row("default", rates(3.0, 15.0)),
            row("claude-sonnet-4-7", rates(3.0, 15.0)),
            row("claude-opus-4-8", rates_with_cache(5.0, 25.0, 0.5, 6.25)),
            row("claude-haiku-4-5", rates_with_cache(1.0, 5.0, 0.1, 1.25)),
            row("claude-opus-5", rates_with_cache(5.0, 25.0, 0.5, 6.25)),
            row("claude-fable-5", rates_with_cache(10.0, 50.0, 1.0, 12.5)),
            row("claude-sonnet-5", rates_with_cache(2.0, 10.0, 0.2, 2.5)),
        ],
    },
    ProviderRows {
        provider: "ollama",
        models: &[row("default", rates(0.0, 0.0))],
    },
    ProviderRows {
        provider: "openrouter",
        models: &[
            row("stealth/ox-alpha", rates(0.0, 0.0)),
            row("openai/gpt-4o-mini", rates_with_read(0.15, 0.6, 0.075)),
        ],
    },
    ProviderRows {
        provider: "aliyun",
        models: &[
            row("default", rates(0.0, 0.0)),
            row("qwen3-coder-plus", rates(0.0, 0.0)),
            row("qwen3-coder-next", rates(0.0, 0.0)),
            row("qwen3.5-plus", rates(0.0, 0.0)),
            row("qwen3-max-2026-01-23", rates(0.0, 0.0)),
            row("qwen3.7-max", rates(0.0, 0.0)),
            row("kimi-k2.5", rates(0.0, 0.0)),
            row("glm-5.2", rates(0.0, 0.0)),
            row("MiniMax-M2.5", rates(0.0, 0.0)),
        ],
    },
    ProviderRows {
        provider: "kimi-code",
        models: &[row("default", rates(0.0, 0.0))],
    },
    ProviderRows {
        provider: "agy",
        models: &[row("default", rates(0.0, 0.0))],
    },
    ProviderRows {
        provider: "grok-build",
        models: &[row("default", rates(0.0, 0.0))],
    },
    ProviderRows {
        provider: "deepseek",
        models: &[
            row("default", rates(0.435, 0.87)),
            row("deepseek-v4-flash", rates(0.14, 0.28)),
            row("DeepSeek-V4-Flash", rates(0.14, 0.28)),
            row("deepseek-v4-pro", rates(0.435, 0.87)),
            row("DeepSeek-V4-Pro", rates(0.435, 0.87)),
        ],
    },
    ProviderRows {
        provider: "mimo",
        models: &[
            row("default", rates(0.0, 0.0)),
            row("mimo-v2.5-pro", rates(0.0, 0.0)),
        ],
    },
];
// price-catalog-end

static PROVIDER_META: &[ProviderMetaRow] = &[
    ProviderMetaRow {
        provider: "codex",
        meta: SourceMeta {
            source_url: URL_OPENAI,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Estimated,
            note: "2026-09-12 jina extract of OpenAI API pricing; most Codex aliases were not in the extracted tables. Do not treat as vendor-billed.",
        },
    },
    ProviderMetaRow {
        provider: "gemini",
        meta: SourceMeta {
            source_url: URL_GEMINI,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Estimated,
            note: "2026-09-12 official Gemini pricing page fetched; 2.5-pro / 2.5-flash table numbers were not isolated in the extract.",
        },
    },
    ProviderMetaRow {
        provider: "claude",
        meta: SourceMeta {
            source_url: URL_ANTHROPIC,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Estimated,
            note: "2026-09-12 official Anthropic pricing page fetched.",
        },
    },
    ProviderMetaRow {
        provider: "openrouter",
        meta: SourceMeta {
            source_url: URL_OPENROUTER_MODELS,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Estimated,
            note: "Aggregator list price, not a vendor direct-API invoice.",
        },
    },
    ProviderMetaRow {
        provider: "deepseek",
        meta: SourceMeta {
            source_url: URL_DEEPSEEK,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Estimated,
            note: "2026-09-12 official DeepSeek page uses peak/off-peak cache-hit and cache-miss rates; table numbers (2026-08-12 comments) do not match those published cells. Values left unchanged.",
        },
    },
    ProviderMetaRow {
        provider: "aliyun",
        meta: SourceMeta {
            source_url: URL_ALIYUN_PLAN,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Estimated,
            note: "Official Coding Plan page. Token $0 is plan-covered, not metered.",
        },
    },
    ProviderMetaRow {
        provider: "mimo",
        meta: SourceMeta {
            source_url: URL_MIMO_PLAN,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Estimated,
            note: "Official MiMo Token Plan page. Token $0 is plan-covered.",
        },
    },
    ProviderMetaRow {
        provider: "kimi-code",
        meta: SourceMeta {
            source_url: "",
            checked_at: "",
            status: PriceStatus::Unverified,
            note: "Coding Plan login; no official plan URL confirmed 2026-09-12.",
        },
    },
    ProviderMetaRow {
        provider: "grok-build",
        meta: SourceMeta {
            source_url: "",
            checked_at: "",
            status: PriceStatus::Unverified,
            note: "grok.com subscription shell; no official plan URL confirmed.",
        },
    },
    ProviderMetaRow {
        provider: "ollama",
        meta: SourceMeta {
            source_url: "",
            checked_at: "",
            status: PriceStatus::Unverified,
            note: "Local runtime. Table $0 is not a verified metered rate.",
        },
    },
];

static ROW_META: &[RowMeta] = &[
    RowMeta {
        provider: "claude",
        model: "claude-opus-4-8",
        meta: SourceMeta {
            source_url: URL_ANTHROPIC,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Verified,
            note: "Official table: Claude Opus 4.8 $5 / $25 per MTok.",
        },
    },
    RowMeta {
        provider: "claude",
        model: "claude-haiku-4-5",
        meta: SourceMeta {
            source_url: URL_ANTHROPIC,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Verified,
            note: "Official table: Claude Haiku 4.5 $1 / $5 per MTok.",
        },
    },
    RowMeta {
        provider: "claude",
        model: "claude-opus-5",
        meta: SourceMeta {
            source_url: URL_ANTHROPIC,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Verified,
            note: "Official table: Claude Opus 5 $5 / $25 per MTok.",
        },
    },
    RowMeta {
        provider: "claude",
        model: "claude-fable-5",
        meta: SourceMeta {
            source_url: URL_ANTHROPIC,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Verified,
            note: "Official table: Claude Fable 5 $10 / $50 per MTok.",
        },
    },
    RowMeta {
        provider: "claude",
        model: "claude-sonnet-5",
        meta: SourceMeta {
            source_url: URL_ANTHROPIC,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Verified,
            note: "Official table: Claude Sonnet 5 $2 / $10 per MTok.",
        },
    },
    RowMeta {
        provider: "claude",
        model: "claude-sonnet-4-7",
        meta: SourceMeta {
            source_url: URL_ANTHROPIC,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Estimated,
            note: "Anthropic page lists Sonnet 4.6 at $3 / $15, not Sonnet 4.7. Historical estimate kept.",
        },
    },
    RowMeta {
        provider: "claude",
        model: "default",
        meta: SourceMeta {
            source_url: URL_ANTHROPIC,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Estimated,
            note: "Fallback row, not a published model id.",
        },
    },
    RowMeta {
        provider: "codex",
        model: "gpt-5.6-terra",
        meta: SourceMeta {
            source_url: URL_OPENAI,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Verified,
            note: "Official standard short-context: input $2 / output $12 per 1M.",
        },
    },
    RowMeta {
        provider: "codex",
        model: "gpt-5.6-luna",
        meta: SourceMeta {
            source_url: URL_OPENAI,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Verified,
            note: "Official standard short-context: input $0.20 / output $1.20 per 1M.",
        },
    },
    RowMeta {
        provider: "codex",
        model: "gpt-5.6-sol",
        meta: SourceMeta {
            source_url: URL_OPENAI,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Estimated,
            note: "Official standard short-context is $4 / $20; table $2 / $10 matches Flex/Batch, not Standard. Values left unchanged.",
        },
    },
    RowMeta {
        provider: "gemini",
        model: "gemini-3.1-pro-preview",
        meta: SourceMeta {
            source_url: URL_GEMINI,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Estimated,
            note: "2026-09-12 official page fetched; dedicated 3.1 Pro table was not isolated. An image-model block claims text I/O matches 3.1 Pro at $2 / $12. Not marked verified.",
        },
    },
    RowMeta {
        provider: "openrouter",
        model: "openai/gpt-4o-mini",
        meta: SourceMeta {
            source_url: URL_OPENROUTER_MODELS,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Verified,
            note: "2026-09-12 public models API: prompt $0.15 / completion $0.60 per 1M. This is the aggregator invoice rate for provider=openrouter.",
        },
    },
    RowMeta {
        provider: "openrouter",
        model: "stealth/ox-alpha",
        meta: SourceMeta {
            source_url: URL_OPENROUTER_MODELS,
            checked_at: CHECKED_TODAY,
            status: PriceStatus::Estimated,
            note: "2026-09-12 public models API no longer lists this id. Table $0 is a kept promo row, not current verified spend.",
        },
    },
];

const EMPTY_META: SourceMeta = SourceMeta {
    source_url: "",
    checked_at: "",
    status: PriceStatus::Unverified,
    note: "",
};

fn provider_rows(provider: &str) -> Option<&'static ProviderRows> {
    CATALOG.iter().find(|entry| entry.provider == provider)
}

fn model_row<'a>(models: &'a [ModelRow], key: &str) -> Option<&'a ModelRow> {
    models.iter().find(|entry| entry.key == key)
}

fn meta_for(provider: &str, model_key: &str) -> &'static SourceMeta {
    if let Some(row) = ROW_META
        .iter()
        .find(|entry| entry.provider == provider && entry.model == model_key)
    {
        return &row.meta;
    }
    if let Some(row) = PROVIDER_META
        .iter()
        .find(|entry| entry.provider == provider)
    {
        return &row.meta;
    }
    &EMPTY_META
}

fn spend_class_for(provider: &str, known_row: bool) -> SpendClass {
    if !known_row {
        return SpendClass::Unknown;
    }
    if LOCAL_PROVIDERS.contains(&provider) {
        return SpendClass::Local;
    }
    if SUBSCRIPTION_PROVIDERS.contains(&provider) {
        return SpendClass::Subscription;
    }
    SpendClass::Metered
}

fn counts_as_real(
    spend_class: SpendClass,
    status: PriceStatus,
    input: Option<f64>,
    output: Option<f64>,
) -> bool {
    spend_class == SpendClass::Metered
        && status == PriceStatus::Verified
        && input.is_some()
        && output.is_some()
}

fn split_provider_model<'a>(provider: &'a str, model: &'a str) -> (&'a str, &'a str) {
    if let Some((base, embedded)) = provider.split_once('/') {
        let name = if model.is_empty() { embedded } else { model };
        (base, name)
    } else {
        (provider, model)
    }
}

fn unknown(base: &str, model: &str, provider_argument: &str) -> PriceSource {
    PriceSource {
        provider: base.to_string(),
        model: model.to_string(),
        model_key: String::new(),
        input_per_million: None,
        output_per_million: None,
        cache_read_per_million: None,
        cache_write_per_million: None,
        source_url: String::new(),
        checked_at: String::new(),
        status: PriceStatus::Unverified,
        spend_class: SpendClass::Unknown,
        counts_as_real_spend: false,
        note: format!("No pricing data for provider '{provider_argument}' model '{model}'"),
    }
}

fn from_row(base: &str, requested_model: &str, row: &ModelRow, meta: &SourceMeta) -> PriceSource {
    let input = Some(row.price.input);
    let output = Some(row.price.output);
    let status = meta.status;
    let spend_class = spend_class_for(base, true);
    let model = if requested_model.is_empty() {
        row.key.to_string()
    } else {
        requested_model.to_string()
    };
    PriceSource {
        provider: base.to_string(),
        model,
        model_key: row.key.to_string(),
        input_per_million: input,
        output_per_million: output,
        cache_read_per_million: row.price.cache_read,
        cache_write_per_million: row.price.cache_write,
        source_url: meta.source_url.to_string(),
        checked_at: meta.checked_at.to_string(),
        status,
        spend_class,
        counts_as_real_spend: counts_as_real(spend_class, status, input, output),
        note: meta.note.to_string(),
    }
}

/// Provenance for one provider and model.
///
/// `model` may be empty. When `provider` contains `/` and `model` is empty,
/// the string is split on the first slash into the provider and the model
/// (`aliyun/qwen3-coder-plus`). A non-empty `model` is kept as given. An exact
/// model key is used when it is present. Otherwise the provider's `default`
/// row is used when that key exists. A miss has `None` unit prices, status
/// [`PriceStatus::Unverified`], spend class [`SpendClass::Unknown`], and
/// `counts_as_real_spend` false.
#[must_use]
pub fn lookup_price_source(provider: &str, model: &str) -> PriceSource {
    let (base, name) = split_provider_model(provider, model);
    let Some(table) = provider_rows(base) else {
        return unknown(base, name, provider);
    };
    let exact = if name.is_empty() {
        None
    } else {
        model_row(table.models, name)
    };
    let Some(resolved) = exact.or_else(|| model_row(table.models, "default")) else {
        return unknown(base, name, provider);
    };
    from_row(base, name, resolved, meta_for(base, resolved.key))
}

/// One provenance row for every explicit key in the copied rate list.
#[must_use]
pub fn list_price_sources() -> Vec<PriceSource> {
    let mut rows = Vec::new();
    for provider in CATALOG {
        for model in provider.models {
            rows.push(lookup_price_source(provider.provider, model.key));
        }
    }
    rows
}

/// USD for a verified metered row, or `None` when the row is not real spend.
///
/// Estimated, subscription, local, unknown, and zero-price promo rows return
/// `None`. The total is input tokens times the input unit price plus output
/// tokens times the output unit price, each divided by 1,000,000. Cache rates
/// are not part of this total.
#[must_use]
pub fn real_metered_spend_usd(
    source: &PriceSource,
    input_tokens: u64,
    output_tokens: u64,
) -> Option<f64> {
    if !source.counts_as_real_spend {
        return None;
    }
    let input_rate = source.input_per_million?;
    let output_rate = source.output_per_million?;
    let per_million = 1_000_000_f64;
    Some(
        (input_tokens as f64 / per_million) * input_rate
            + (output_tokens as f64 / per_million) * output_rate,
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::float_cmp, clippy::too_many_lines)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use vyane_core::{ModelId, Usage};

    use super::{
        CHECKED_TODAY, OPENROUTER_SYNC_CHECKED, PriceSource, PriceStatus, SpendClass,
        list_price_sources, lookup_price_source, real_metered_spend_usd,
    };
    use crate::cost::PriceTable;

    fn usage(input: u64, output: u64) -> Usage {
        Usage {
            input_tokens: input,
            output_tokens: output,
            reasoning_tokens: None,
            cached_input_tokens: None,
        }
    }

    macro_rules! price {
        ($p:literal, $m:literal, $i:literal, $o:literal) => {
            ($p, $m, $i, $o, None, None)
        };
        ($p:literal, $m:literal, $i:literal, $o:literal, r $r:literal) => {
            ($p, $m, $i, $o, Some($r), None)
        };
        ($p:literal, $m:literal, $i:literal, $o:literal, r $r:literal, w $w:literal) => {
            ($p, $m, $i, $o, Some($r), Some($w))
        };
    }

    type ExpectedPrice = (
        &'static str,
        &'static str,
        f64,
        f64,
        Option<f64>,
        Option<f64>,
    );

    fn expected_prices() -> Vec<ExpectedPrice> {
        vec![
            price!("codex", "default", 2.0, 8.0),
            price!("codex", "gpt-6.1-sol", 2.0, 10.0, r 0.10, w 2.50),
            price!("codex", "gpt-6-astra", 10.0, 50.0, r 1.00, w 12.50),
            price!("codex", "gpt-4.1", 2.0, 8.0, r 0.5),
            price!("codex", "gpt-4.1-mini", 0.4, 1.6, r 0.1),
            price!("codex", "gpt-4.1-nano", 0.1, 0.4, r 0.025),
            price!("codex", "gpt-5.5", 5.0, 30.0, r 0.5),
            price!("codex", "gpt-5.6-terra", 2.0, 12.0, r 0.2, w 2.5),
            price!("codex", "gpt-5.6-sol", 2.0, 10.0, r 0.2, w 2.5),
            price!("codex", "gpt-5.6-luna", 0.2, 1.2, r 0.02, w 0.25),
            price!("codex", "o3", 2.0, 8.0, r 0.5),
            price!("codex", "o4-mini", 1.1, 4.4, r 0.275),
            price!("gemini", "default", 1.25, 10.0),
            price!("gemini", "gemini-2.5-pro", 1.25, 10.0, r 0.125, w 0.375),
            price!("gemini", "gemini-2.5-flash", 0.3, 2.5, r 0.03, w 0.0833333333),
            price!("gemini", "gemini-3.1-pro-preview", 2.0, 12.0, r 0.2, w 0.375),
            price!("claude", "default", 3.0, 15.0),
            price!("claude", "claude-sonnet-4-7", 3.0, 15.0),
            price!("claude", "claude-opus-4-8", 5.0, 25.0, r 0.5, w 6.25),
            price!("claude", "claude-haiku-4-5", 1.0, 5.0, r 0.1, w 1.25),
            price!("claude", "claude-opus-5", 5.0, 25.0, r 0.5, w 6.25),
            price!("claude", "claude-fable-5", 10.0, 50.0, r 1.0, w 12.5),
            price!("claude", "claude-sonnet-5", 2.0, 10.0, r 0.2, w 2.5),
            price!("ollama", "default", 0.0, 0.0),
            price!("openrouter", "stealth/ox-alpha", 0.0, 0.0),
            price!("openrouter", "openai/gpt-4o-mini", 0.15, 0.6, r 0.075),
            price!("aliyun", "default", 0.0, 0.0),
            price!("aliyun", "qwen3-coder-plus", 0.0, 0.0),
            price!("aliyun", "qwen3-coder-next", 0.0, 0.0),
            price!("aliyun", "qwen3.5-plus", 0.0, 0.0),
            price!("aliyun", "qwen3-max-2026-01-23", 0.0, 0.0),
            price!("aliyun", "qwen3.7-max", 0.0, 0.0),
            price!("aliyun", "kimi-k2.5", 0.0, 0.0),
            price!("aliyun", "glm-5.2", 0.0, 0.0),
            price!("aliyun", "MiniMax-M2.5", 0.0, 0.0),
            price!("kimi-code", "default", 0.0, 0.0),
            price!("agy", "default", 0.0, 0.0),
            price!("grok-build", "default", 0.0, 0.0),
            price!("deepseek", "default", 0.435, 0.87),
            price!("deepseek", "deepseek-v4-flash", 0.14, 0.28),
            price!("deepseek", "DeepSeek-V4-Flash", 0.14, 0.28),
            price!("deepseek", "deepseek-v4-pro", 0.435, 0.87),
            price!("deepseek", "DeepSeek-V4-Pro", 0.435, 0.87),
            price!("mimo", "default", 0.0, 0.0),
            price!("mimo", "mimo-v2.5-pro", 0.0, 0.0),
        ]
    }

    fn assert_row(
        src: &PriceSource,
        input: f64,
        output: f64,
        cache_read: Option<f64>,
        cache_write: Option<f64>,
    ) {
        assert_eq!(
            (
                src.input_per_million,
                src.output_per_million,
                src.cache_read_per_million,
                src.cache_write_per_million
            ),
            (Some(input), Some(output), cache_read, cache_write),
            "{} / {}",
            src.provider,
            src.model_key
        );
    }

    #[test]
    fn listed_numbers_are_the_pricing_table() {
        let expected = expected_prices();
        let listed = list_price_sources();
        assert_eq!(listed.len(), 45);
        assert_eq!(expected.len(), 45);
        for (src, row) in listed.iter().zip(expected.iter()) {
            assert_eq!(src.provider, row.0);
            assert_eq!(src.model_key, row.1);
            assert_eq!(src.model, row.1);
            assert_row(src, row.2, row.3, row.4, row.5);
            let again = lookup_price_source(row.0, row.1);
            assert_row(&again, row.2, row.3, row.4, row.5);
        }
    }

    #[test]
    fn every_pricing_key_is_queryable() {
        let listed = list_price_sources();
        let keys: BTreeSet<_> = listed
            .iter()
            .map(|src| (src.provider.as_str(), src.model_key.as_str()))
            .collect();
        assert_eq!(keys.len(), 45);
        let mut counts = BTreeMap::<&str, usize>::new();
        for src in &listed {
            *counts.entry(src.provider.as_str()).or_default() += 1;
            assert!(matches!(
                src.status,
                PriceStatus::Verified | PriceStatus::Estimated | PriceStatus::Unverified
            ));
            assert!(matches!(
                src.spend_class,
                SpendClass::Metered
                    | SpendClass::Subscription
                    | SpendClass::Local
                    | SpendClass::Unknown
            ));
            let dict = src.to_dict();
            assert!(dict.get("source_url").is_some());
            assert!(dict.get("checked_at").is_some());
            assert_eq!(dict.as_object().unwrap().len(), 13);
        }
        assert_eq!(counts.len(), 11);
        assert_eq!(counts["codex"], 12);
        assert_eq!(counts["gemini"], 4);
        assert_eq!(counts["claude"], 7);
        assert_eq!(counts["ollama"], 1);
        assert_eq!(counts["openrouter"], 2);
        assert_eq!(counts["aliyun"], 9);
        assert_eq!(counts["kimi-code"], 1);
        assert_eq!(counts["agy"], 1);
        assert_eq!(counts["grok-build"], 1);
        assert_eq!(counts["deepseek"], 5);
        assert_eq!(counts["mimo"], 2);

        let verified: BTreeSet<_> = listed
            .iter()
            .filter(|src| src.status == PriceStatus::Verified)
            .map(|src| (src.provider.as_str(), src.model_key.as_str()))
            .collect();
        assert_eq!(
            verified,
            BTreeSet::from([
                ("claude", "claude-opus-4-8"),
                ("claude", "claude-haiku-4-5"),
                ("claude", "claude-opus-5"),
                ("claude", "claude-fable-5"),
                ("claude", "claude-sonnet-5"),
                ("codex", "gpt-5.6-terra"),
                ("codex", "gpt-5.6-luna"),
                ("openrouter", "openai/gpt-4o-mini"),
            ])
        );
        let unverified: BTreeSet<_> = listed
            .iter()
            .filter(|src| src.status == PriceStatus::Unverified)
            .map(|src| (src.provider.as_str(), src.model_key.as_str()))
            .collect();
        assert_eq!(
            unverified,
            BTreeSet::from([
                ("ollama", "default"),
                ("kimi-code", "default"),
                ("grok-build", "default"),
                ("agy", "default"),
            ])
        );
    }

    #[test]
    fn unknown_provider_is_not_zero_real_spend() {
        let model = ModelId::new("no-such-provider");
        let tokens = usage(1_000_000, 1_000_000);
        assert_eq!(PriceTable::builtins().estimate(&model, &tokens), None);
        let src = lookup_price_source("no-such-provider", "x");
        assert_eq!(src.provider, "no-such-provider");
        assert_eq!(src.model, "x");
        assert_eq!(src.model_key, "");
        assert_eq!(src.input_per_million, None);
        assert_eq!(src.output_per_million, None);
        assert_eq!(src.cache_read_per_million, None);
        assert_eq!(src.cache_write_per_million, None);
        assert_eq!(src.status, PriceStatus::Unverified);
        assert_eq!(src.spend_class, SpendClass::Unknown);
        assert!(!src.counts_as_real_spend);
        assert_eq!(src.checked_at, "");
        assert_eq!(src.source_url, "");
        assert_eq!(
            src.note,
            "No pricing data for provider 'no-such-provider' model 'x'"
        );
        assert_eq!(real_metered_spend_usd(&src, 1_000_000, 1_000_000), None);
        let dict = src.to_dict();
        assert!(dict["input_per_million"].is_null());
        assert!(dict["output_per_million"].is_null());
        assert!(dict["cache_read_per_million"].is_null());
        assert!(dict["cache_write_per_million"].is_null());
        assert_eq!(dict["status"], "unverified");
        assert_eq!(dict["spend_class"], "unknown");
        assert_eq!(dict["counts_as_real_spend"], false);
        assert_eq!(
            serde_json::to_value(&src).unwrap(),
            dict,
            "Serialize and to_dict stay the same object"
        );
    }

    #[test]
    fn openrouter_unlisted_model_is_not_zero_real_spend() {
        let src = lookup_price_source("openrouter", "openai/gpt-5-not-in-table");
        assert_eq!(src.input_per_million, None);
        assert_eq!(src.output_per_million, None);
        assert_eq!(src.model_key, "");
        assert_eq!(src.spend_class, SpendClass::Unknown);
        assert_eq!(src.status, PriceStatus::Unverified);
        assert!(!src.counts_as_real_spend);
        assert_eq!(real_metered_spend_usd(&src, 10, 10), None);
        assert!(src.to_dict()["input_per_million"].is_null());
    }

    #[test]
    fn verified_anthropic_and_openai_rows() {
        let opus = lookup_price_source("claude", "claude-opus-4-8");
        assert_eq!(opus.status, PriceStatus::Verified);
        assert!(opus.source_url.ends_with("/about-claude/pricing"));
        assert_eq!(
            opus.source_url,
            "https://docs.anthropic.com/en/docs/about-claude/pricing"
        );
        assert_eq!(opus.checked_at, "2026-09-12");
        assert_eq!(opus.checked_at, CHECKED_TODAY);
        assert_eq!(opus.input_per_million, Some(5.0));
        assert_eq!(opus.output_per_million, Some(25.0));
        assert_eq!(opus.cache_read_per_million, Some(0.5));
        assert_eq!(opus.cache_write_per_million, Some(6.25));
        assert!(opus.counts_as_real_spend);
        assert_eq!(
            real_metered_spend_usd(&opus, 1_000_000, 1_000_000),
            Some(30.0)
        );
        assert_eq!(opus.to_dict()["status"], "verified");
        assert_eq!(opus.to_dict()["input_per_million"], serde_json::json!(5.0));

        let terra = lookup_price_source("codex", "gpt-5.6-terra");
        assert_eq!(terra.status, PriceStatus::Verified);
        assert_eq!(terra.input_per_million, Some(2.0));
        assert_eq!(terra.output_per_million, Some(12.0));
        assert_eq!(terra.cache_read_per_million, Some(0.2));
        assert_eq!(terra.cache_write_per_million, Some(2.5));
        assert!(terra.counts_as_real_spend);
        assert_eq!(terra.source_url, "https://platform.openai.com/docs/pricing");
        assert_eq!(
            terra.note,
            "Official standard short-context: input $2 / output $12 per 1M."
        );
    }

    #[test]
    fn estimated_rows_are_not_real_spend() {
        let sol = lookup_price_source("codex", "gpt-5.6-sol");
        assert_eq!(sol.status, PriceStatus::Estimated);
        assert_eq!(sol.input_per_million, Some(2.0));
        assert_eq!(sol.output_per_million, Some(10.0));
        assert!(!sol.counts_as_real_spend);
        assert_eq!(real_metered_spend_usd(&sol, 1_000_000, 1_000_000), None);
        assert_eq!(
            sol.note,
            "Official standard short-context is $4 / $20; table $2 / $10 matches Flex/Batch, not Standard. Values left unchanged."
        );

        let flash = lookup_price_source("deepseek", "deepseek-v4-flash");
        assert_eq!(flash.status, PriceStatus::Estimated);
        assert!(flash.source_url.ends_with("/quick_start/pricing"));
        assert_eq!(
            flash.source_url,
            "https://api-docs.deepseek.com/quick_start/pricing"
        );
        assert_eq!(flash.input_per_million, Some(0.14));
        assert_eq!(flash.output_per_million, Some(0.28));
        assert!(!flash.counts_as_real_spend);
        let upper = lookup_price_source("deepseek", "DeepSeek-V4-Flash");
        assert_eq!(upper.model_key, "DeepSeek-V4-Flash");
        assert_eq!(upper.input_per_million, Some(0.14));
        assert_eq!(upper.output_per_million, Some(0.28));
    }

    #[test]
    fn subscription_zero_is_not_unknown_real_spend() {
        let src = lookup_price_source("mimo", "mimo-v2.5-pro");
        assert_eq!(src.spend_class, SpendClass::Subscription);
        assert_eq!(src.status, PriceStatus::Estimated);
        assert_eq!(src.input_per_million, Some(0.0));
        assert_eq!(src.output_per_million, Some(0.0));
        assert!(!src.counts_as_real_spend);
        assert_eq!(
            src.source_url,
            "https://mimo.mi.com/docs/en-US/price/token-plan"
        );
        assert!(!src.source_url.is_empty());
        assert_eq!(real_metered_spend_usd(&src, 1_000_000, 1_000_000), None);
        assert_eq!(
            src.note,
            "Official MiMo Token Plan page. Token $0 is plan-covered."
        );
    }

    #[test]
    fn stealth_zero_promo_is_not_verified_real_spend() {
        let src = lookup_price_source("openrouter", "stealth/ox-alpha");
        assert_eq!(src.input_per_million, Some(0.0));
        assert_eq!(src.output_per_million, Some(0.0));
        assert_eq!(src.status, PriceStatus::Estimated);
        assert!(!src.counts_as_real_spend);
        assert_eq!(real_metered_spend_usd(&src, 1_000_000, 1_000_000), None);
        assert_eq!(
            src.note,
            "2026-09-12 public models API no longer lists this id. Table $0 is a kept promo row, not current verified spend."
        );
    }

    #[test]
    fn openrouter_gpt4o_mini_verified_against_public_api() {
        let src = lookup_price_source("openrouter", "openai/gpt-4o-mini");
        assert_eq!(src.status, PriceStatus::Verified);
        assert_eq!(src.checked_at, "2026-09-12");
        assert_ne!(src.checked_at, OPENROUTER_SYNC_CHECKED);
        assert_eq!(OPENROUTER_SYNC_CHECKED, "2026-09-02");
        assert_eq!(src.input_per_million, Some(0.15));
        assert_eq!(src.output_per_million, Some(0.6));
        assert_eq!(src.cache_read_per_million, Some(0.075));
        assert_eq!(src.cache_write_per_million, None);
        assert!(src.counts_as_real_spend);
        assert_eq!(src.source_url, "https://openrouter.ai/api/v1/models");
    }

    #[test]
    fn provider_slash_model_form() {
        let src = lookup_price_source("aliyun/qwen3-coder-plus", "");
        assert_eq!(src.provider, "aliyun");
        assert_eq!(src.model, "qwen3-coder-plus");
        assert_eq!(src.model_key, "qwen3-coder-plus");
        assert_eq!(src.spend_class, SpendClass::Subscription);
        assert_eq!(src.input_per_million, Some(0.0));

        let explicit = lookup_price_source("aliyun/qwen3-coder-plus", "qwen3.5-plus");
        assert_eq!(explicit.provider, "aliyun");
        assert_eq!(explicit.model, "qwen3.5-plus");
        assert_eq!(explicit.model_key, "qwen3.5-plus");

        let nested = lookup_price_source("openrouter/openai/gpt-4o-mini", "");
        assert_eq!(nested.provider, "openrouter");
        assert_eq!(nested.model_key, "openai/gpt-4o-mini");
        assert_eq!(nested.status, PriceStatus::Verified);

        let missing = lookup_price_source("missing/embedded", "");
        assert_eq!(missing.provider, "missing");
        assert_eq!(missing.model, "embedded");
        assert_eq!(missing.model_key, "");
        assert_eq!(
            missing.note,
            "No pricing data for provider 'missing/embedded' model 'embedded'"
        );
        let kept = lookup_price_source("missing/embedded", "explicit");
        assert_eq!(kept.provider, "missing");
        assert_eq!(kept.model, "explicit");
        assert_eq!(
            kept.note,
            "No pricing data for provider 'missing/embedded' model 'explicit'"
        );
    }

    #[test]
    fn verified_rows_have_url_and_date() {
        for src in list_price_sources() {
            if src.status == PriceStatus::Verified {
                assert!(src.source_url.starts_with("https://"), "{}", src.model_key);
                assert_eq!(src.checked_at, "2026-09-12");
                assert_eq!(src.spend_class, SpendClass::Metered);
                assert!(src.counts_as_real_spend);
                assert!(src.input_per_million.is_some());
                assert!(src.output_per_million.is_some());
            }
            if src.spend_class != SpendClass::Metered || src.status != PriceStatus::Verified {
                assert!(!src.counts_as_real_spend, "{}", src.model_key);
            }
        }
    }

    #[test]
    fn kimi_and_grok_plan_urls_unverified() {
        let kimi = lookup_price_source("kimi-code", "default");
        let grok = lookup_price_source("grok-build", "default");
        assert_eq!(kimi.status, PriceStatus::Unverified);
        assert_eq!(kimi.source_url, "");
        assert_eq!(kimi.checked_at, "");
        assert_eq!(kimi.spend_class, SpendClass::Subscription);
        assert!(!kimi.counts_as_real_spend);
        assert_eq!(
            kimi.note,
            "Coding Plan login; no official plan URL confirmed 2026-09-12."
        );
        assert_eq!(grok.status, PriceStatus::Unverified);
        assert_eq!(grok.source_url, "");
        assert_eq!(grok.checked_at, "");
        assert!(!grok.counts_as_real_spend);
        assert_eq!(grok.spend_class, SpendClass::Subscription);
    }

    #[test]
    fn copied_cache_rates_match_the_source_literals() {
        let flash = lookup_price_source("gemini", "gemini-2.5-flash");
        assert_eq!(flash.input_per_million, Some(0.3));
        assert_eq!(flash.output_per_million, Some(2.5));
        assert_eq!(flash.cache_read_per_million, Some(0.03));
        assert_eq!(flash.cache_write_per_million, Some(0.0833333333));

        let astra = lookup_price_source("codex", "gpt-6-astra");
        assert_eq!(astra.input_per_million, Some(10.0));
        assert_eq!(astra.output_per_million, Some(50.0));
        assert_eq!(astra.cache_read_per_million, Some(1.00));
        assert_eq!(astra.cache_write_per_million, Some(12.50));

        let nano = lookup_price_source("codex", "gpt-4.1-nano");
        assert_eq!(nano.cache_read_per_million, Some(0.025));
        assert_eq!(nano.cache_write_per_million, None);

        let mini = lookup_price_source("codex", "o4-mini");
        assert_eq!(mini.input_per_million, Some(1.1));
        assert_eq!(mini.output_per_million, Some(4.4));
        assert_eq!(mini.cache_read_per_million, Some(0.275));

        let pro = lookup_price_source("deepseek", "default");
        assert_eq!(pro.input_per_million, Some(0.435));
        assert_eq!(pro.output_per_million, Some(0.87));
        assert_eq!(pro.cache_read_per_million, None);
        assert_eq!(pro.cache_write_per_million, None);
    }

    #[test]
    fn row_meta_overrides_provider_meta_and_empty_model_uses_default() {
        let claude_alias = lookup_price_source("claude", "not-a-published-id");
        assert_eq!(claude_alias.model_key, "default");
        assert_eq!(claude_alias.model, "not-a-published-id");
        assert_eq!(claude_alias.input_per_million, Some(3.0));
        assert_eq!(claude_alias.output_per_million, Some(15.0));
        assert_eq!(claude_alias.cache_read_per_million, None);
        assert_eq!(claude_alias.status, PriceStatus::Estimated);
        assert_eq!(claude_alias.spend_class, SpendClass::Metered);
        assert!(!claude_alias.counts_as_real_spend);
        assert_eq!(claude_alias.note, "Fallback row, not a published model id.");

        let named_default = lookup_price_source("claude", "");
        assert_eq!(named_default.model, "default");
        assert_eq!(named_default.model_key, "default");
        assert_eq!(
            named_default.note,
            "Fallback row, not a published model id."
        );

        let codex_alias = lookup_price_source("codex", "not-a-published-id");
        assert_eq!(codex_alias.model_key, "default");
        assert_eq!(codex_alias.input_per_million, Some(2.0));
        assert_eq!(codex_alias.output_per_million, Some(8.0));
        assert_eq!(codex_alias.cache_read_per_million, None);
        assert_eq!(
            codex_alias.note,
            "2026-09-12 jina extract of OpenAI API pricing; most Codex aliases were not in the extracted tables. Do not treat as vendor-billed."
        );

        let ollama = lookup_price_source("ollama", "llama3");
        assert_eq!(ollama.model_key, "default");
        assert_eq!(ollama.model, "llama3");
        assert_eq!(ollama.spend_class, SpendClass::Local);
        assert_eq!(ollama.status, PriceStatus::Unverified);
        assert_eq!(ollama.input_per_million, Some(0.0));
        assert!(!ollama.counts_as_real_spend);
        assert_eq!(
            ollama.note,
            "Local runtime. Table $0 is not a verified metered rate."
        );
    }

    #[test]
    fn agy_known_row_is_metered_and_unverified() {
        let src = lookup_price_source("agy", "default");
        assert_eq!(src.spend_class, SpendClass::Metered);
        assert_eq!(src.status, PriceStatus::Unverified);
        assert_eq!(src.input_per_million, Some(0.0));
        assert_eq!(src.output_per_million, Some(0.0));
        assert_eq!(src.source_url, "");
        assert_eq!(src.checked_at, "");
        assert_eq!(src.note, "");
        assert!(!src.counts_as_real_spend);
        assert_eq!(real_metered_spend_usd(&src, 1_000_000, 1_000_000), None);
    }

    #[test]
    fn module_does_not_touch_payer_or_subscription_config() {
        let text = include_str!("pricing_source.rs");
        for needle in [
            concat!("normalize_", "payer"),
            concat!("Usage", "Subscription"),
            concat!("load_usage_", "subscriptions"),
            concat!("save_usage_", "subscriptions"),
            concat!("usage_", "summary"),
        ] {
            assert!(!text.contains(needle), "{needle}");
        }
    }

    #[test]
    fn pricing_source_and_cost_builtins_do_not_write_into_each_other() {
        let pricing = include_str!("pricing_source.rs");
        let production = pricing.split("#[cfg(test)]").next().unwrap();
        for line in production.lines() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") {
                continue;
            }
            assert!(
                !line.contains("PriceTable"),
                "code mentions PriceTable: {line}"
            );
            assert!(
                !line.contains("crate::cost"),
                "code mentions crate::cost: {line}"
            );
            assert!(
                !line.contains("estimate_configured_cost"),
                "code mentions estimate_configured_cost: {line}"
            );
            assert!(!line.contains(".estimate("), "code calls estimate: {line}");
            assert!(
                !line.contains("ModelPricing"),
                "code mentions ModelPricing: {line}"
            );
        }

        let catalog_at = production.find("// price-catalog-start").unwrap();
        let catalog_end = production[catalog_at..]
            .find("// price-catalog-end")
            .map(|offset| catalog_at + offset)
            .unwrap();
        let catalog = &production[catalog_at..catalog_end];
        for needle in [
            "claude-3-5-sonnet-20241022",
            "claude-3-5-haiku-20241022",
            "claude-sonnet-4-20250514",
            "claude-opus-4-1-20250805",
            "gemini-2.0-flash",
            "gemini-1.5-pro",
            "o3-mini",
            "\"gpt-4o\"",
            "\"gpt-4o-mini\"",
            "\"o1\"",
        ] {
            assert!(!catalog.contains(needle), "{needle} in copied catalog");
        }

        let cost = include_str!("cost.rs");
        for needle in [
            "pricing_source",
            "PriceSource",
            "lookup_price_source",
            "list_price_sources",
            "real_metered_spend_usd",
            "CHECKED_TODAY",
            "gpt-5.6-terra",
            "gpt-5.6-luna",
            "gpt-5.6-sol",
            "gpt-6.1-sol",
            "gpt-6-astra",
            "claude-opus-4-8",
            "claude-fable-5",
            "claude-sonnet-5",
            "gemini-3.1-pro-preview",
            "deepseek-v4-flash",
            "qwen3-coder-plus",
            "mimo-v2.5-pro",
            "stealth/ox-alpha",
            "0.0833333333",
            "0.435",
        ] {
            assert!(!cost.contains(needle), "{needle} in cost.rs");
        }

        let builtins = between(cost, "pub fn builtins()", "pub fn insert(");
        let estimate = between(cost, "pub fn estimate(", "\nimpl PriceTable");
        assert!(builtins.contains("\"gpt-4o\""));
        assert!(builtins.contains("claude-3-5-sonnet-20241022"));
        assert!(builtins.contains("gemini-2.0-flash"));
        assert!(estimate.contains("input_per_1m"));
        for body in [builtins, estimate] {
            assert!(!body.contains("pricing_source"));
            assert!(!body.contains("PriceSource"));
            assert!(!body.contains("lookup_price_source"));
            assert!(!body.contains("gpt-5.6-terra"));
            assert!(!body.contains("claude-opus-4-8"));
            assert!(!body.contains("0.435"));
        }

        let table = PriceTable::builtins();
        let gpt4o = table.get("gpt-4o").unwrap();
        assert_eq!(gpt4o.input_per_1m, 2.50);
        assert_eq!(gpt4o.output_per_1m, 10.00);
        assert_eq!(gpt4o.cache_read_per_1m, None);
        assert_eq!(gpt4o.reasoning_per_1m, None);
        let sonnet = table.get("claude-3-5-sonnet-20241022").unwrap();
        assert_eq!(sonnet.input_per_1m, 3.00);
        assert_eq!(sonnet.output_per_1m, 15.00);
        let flash = table.get("gemini-2.0-flash").unwrap();
        assert_eq!(flash.input_per_1m, 0.10);
        assert_eq!(flash.output_per_1m, 0.40);
        let gpt41 = table.get("gpt-4.1").unwrap();
        assert_eq!(gpt41.input_per_1m, 2.00);
        assert_eq!(gpt41.output_per_1m, 8.00);
        assert_eq!(gpt41.cache_read_per_1m, None);

        let copied = lookup_price_source("codex", "gpt-4.1");
        assert_eq!(copied.input_per_million, Some(2.0));
        assert_eq!(copied.output_per_million, Some(8.0));
        assert_eq!(copied.cache_read_per_million, Some(0.5));
        assert_eq!(copied.cache_write_per_million, None);

        for id in [
            "gpt-5.6-terra",
            "claude-opus-4-8",
            "gemini-3.1-pro-preview",
            "deepseek-v4-flash",
            "qwen3-coder-plus",
            "mimo-v2.5-pro",
            "stealth/ox-alpha",
            "gpt-6.1-sol",
            "openai/gpt-4o-mini",
        ] {
            assert!(!table.contains(id), "{id}");
            assert_eq!(
                table.estimate(&ModelId::new(id), &usage(1_000_000, 1_000_000)),
                None,
                "{id}"
            );
        }

        let cached_input = Usage {
            input_tokens: 1_000_000,
            output_tokens: 0,
            reasoning_tokens: None,
            cached_input_tokens: Some(1_000_000),
        };
        assert_eq!(
            table.estimate(&ModelId::new("gpt-4.1"), &cached_input),
            Some(2.0)
        );
        let _ = list_price_sources();
        let again = PriceTable::builtins();
        assert_eq!(again.get("gpt-4o").unwrap().input_per_1m, 2.50);
        assert_eq!(again.get("gpt-4o").unwrap().output_per_1m, 10.00);
        assert_eq!(
            again.estimate(&ModelId::new("gpt-4o"), &usage(1_000_000, 1_000_000)),
            Some(12.5)
        );
    }

    fn between<'a>(source: &'a str, start: &str, end: &str) -> &'a str {
        let start_at = source
            .find(start)
            .unwrap_or_else(|| panic!("missing {start}"));
        let rest = &source[start_at..];
        let end_at = rest.find(end).unwrap_or_else(|| panic!("missing {end}"));
        &rest[..end_at]
    }
}
