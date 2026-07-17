//! Pure search/replace logic for a future native file-edit built-in.
//!
//! This module resolves a single `old_string -> new_string` edit against an
//! in-memory `&str` and returns a fresh `String`. It performs **no filesystem
//! I/O** — no reads, writes, canonicalization, or metadata — and it never
//! panics on model input. Reading the file, external-modification detection,
//! atomic write-back, permission gating, and tool-registry wiring all belong to
//! a later card; here only the matching-and-splicing decision lives.
//!
//! ## Fail before write
//!
//! Every guard runs to a decision *before* any output `String` is built. The
//! entry point [`compute_edit`] resolves all match spans through the strictness
//! ladder, validates uniqueness / ambiguity / empty-old / no-op, and only then
//! splices. On any failure it returns a structured [`EditError`] and produces no
//! content at all — partiality is structurally impossible because the function
//! is pure and the error path allocates nothing.
//!
//! ## The strictness ladder
//!
//! [`locate`] tries four rungs of decreasing confidence and stops at the first
//! that yields at least one validated span:
//!
//! 1. [`MatchPass::Exact`] — byte-for-byte substring.
//! 2. [`MatchPass::TrailingWhitespace`] — tolerates trailing-whitespace and
//!    CRLF-vs-LF drift.
//! 3. [`MatchPass::LineTrim`] — tolerates leading/trailing indentation drift on
//!    every line of a block.
//! 4. [`MatchPass::UnicodeNormalized`] — additionally folds typographic
//!    confusables (dashes, curly quotes, exotic spaces) to ASCII.
//!
//! Each rung is the same five steps: normalize the content into a
//! `(normalized_text, offset_map)` pair, normalize the pattern the same way,
//! scan for non-overlapping occurrences in normalized space, remap each hit back
//! to original byte coordinates through the offset map, and roundtrip-validate
//! that the remapped original slice re-normalizes to the pattern. The roundtrip
//! is the safety net that makes fuzzy folding safe: it rejects partial-expansion
//! hits (for example the pattern `"-"` landing inside an em-dash that folds to
//! `"--"`) as [`MatchSearch::Ambiguous`] rather than silently mis-replacing.

use std::ops::Range;

use thiserror::Error;

/// A borrowed, allocation-free description of one search/replace edit.
///
/// Purely data: it holds no file handle and performs no I/O. `content` is the
/// current file text, `old_string` is the text to find, `new_string` is the
/// replacement, and `replace_all` toggles between requiring a unique match
/// (`false`) and replacing every non-overlapping match (`true`).
///
/// ```
/// use vyane_harness::native::{EditRequest, MatchPass, compute_edit};
///
/// let request = EditRequest {
///     content: "let x = 1;\n",
///     old_string: "x = 1",
///     new_string: "x = 2",
///     replace_all: false,
/// };
/// let outcome = compute_edit(&request).expect("unique exact match");
/// assert_eq!(outcome.new_content, "let x = 2;\n");
/// assert_eq!(outcome.replacements, 1);
/// assert_eq!(outcome.matched_pass, MatchPass::Exact);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EditRequest<'a> {
    /// The current file text to edit.
    pub content: &'a str,
    /// The text to find. Empty is the new-file/insert sentinel; see
    /// [`compute_edit`].
    pub old_string: &'a str,
    /// The replacement text, inserted exactly as given.
    pub new_string: &'a str,
    /// `false` requires a unique match; `true` replaces every non-overlapping
    /// match found by the winning pass.
    pub replace_all: bool,
}

/// The successful result of [`compute_edit`].
///
/// `new_content` is the complete new file text. `replacements` is how many
/// occurrences were replaced (`1` for a unique match or a new-file write, `N`
/// for `replace_all`). `matched_pass` reports which rung of the ladder won — the
/// observability contract a later telemetry layer uses to flag edits that only
/// matched under fuzzy or unicode normalization. `spans` records, per
/// replacement, the original byte range removed and the new byte range inserted,
/// for later snippet or diff rendering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EditOutcome {
    /// The full new file text.
    pub new_content: String,
    /// Number of occurrences replaced.
    pub replacements: usize,
    /// Which strictness rung produced the match.
    pub matched_pass: MatchPass,
    /// Byte-accurate record of every replacement, in ascending order.
    pub spans: Vec<ReplacedSpan>,
}

/// Byte-accurate record of one replacement.
///
/// `original` is the removed byte range in the input `content`; `new_start` and
/// `new_len` locate the inserted text within [`EditOutcome::new_content`]. All
/// offsets are byte offsets that land on UTF-8 character boundaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplacedSpan {
    /// The removed byte range in the original content.
    pub original: Range<usize>,
    /// Byte offset of the inserted text within the new content.
    pub new_start: usize,
    /// Byte length of the inserted text.
    pub new_len: usize,
}

/// Which rung of the strictness ladder produced a match.
///
/// Ordered by decreasing confidence. Used internally to short-circuit at the
/// first matching rung and externally as the confidence signal in
/// [`EditOutcome::matched_pass`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchPass {
    /// Byte-for-byte substring match. Highest confidence.
    Exact,
    /// Match after tolerating trailing-whitespace and CRLF-vs-LF drift.
    TrailingWhitespace,
    /// Match after tolerating leading/trailing whitespace drift per line.
    LineTrim,
    /// Match after line-trimming and folding typographic confusables to ASCII.
    /// Lowest confidence.
    UnicodeNormalized,
    /// The empty-`old_string` new-file/insert case; no ladder rung ran.
    NewContent,
}

/// The result of [`locate`].
///
/// `Found` carries the winning pass and one or more non-overlapping spans in
/// original byte coordinates. `NoMatch` means no rung produced any candidate.
/// `Ambiguous` means a rung produced candidates that failed the safety net
/// (partial-expansion roundtrip failure, or overlapping remapped spans) — a
/// distinct signal so a caller learns "present but refused as unsafe" rather
/// than "absent".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchSearch {
    /// At least one validated, non-overlapping span in the winning pass.
    Found {
        /// The rung that produced the spans.
        pass: MatchPass,
        /// Non-overlapping match spans in original byte coordinates, ascending.
        spans: Vec<Range<usize>>,
    },
    /// No rung produced any candidate.
    NoMatch,
    /// A rung produced only unsafe candidates; failed closed.
    Ambiguous,
}

/// A recoverable, model-facing edit failure.
///
/// Every variant names an actionable reason. The type derives [`PartialEq`] and
/// [`Eq`] so a caller (or test) can match on the exact variant, mirroring the
/// crate's existing tool-error convention.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum EditError {
    /// No ladder rung produced any candidate span.
    #[error("old_string was not found in content")]
    NoMatch,
    /// The winning pass produced more than one non-overlapping match while
    /// `replace_all` was `false`. Carries the count so the caller knows how
    /// ambiguous the target is.
    #[error(
        "old_string is not unique: {count} matches found — add surrounding context or set replace_all=true"
    )]
    NotUnique {
        /// How many non-overlapping matches the winning pass found.
        count: usize,
    },
    /// A pass produced candidates that all failed the roundtrip safety net (a
    /// partial-expansion match or overlapping remapped spans). Failed closed and
    /// distinct from [`EditError::NoMatch`] so the caller retries with an exact
    /// string rather than assuming the text is absent.
    #[error("old_string matched only under ambiguous normalization; edit refused for safety")]
    Ambiguous,
    /// `old_string` was empty but `content` was non-empty. An empty
    /// `old_string` is only the new-file/insert sentinel; against existing
    /// content it would match at every boundary, so it is rejected.
    #[error(
        "old_string is empty but content is non-empty; empty old_string is only valid to create new content"
    )]
    EmptyOldStringOnNonEmptyContent,
    /// `old_string` equals `new_string` (both non-empty); the edit would change
    /// nothing.
    #[error("old_string and new_string are identical; edit would change nothing")]
    NoOpEdit,
}

/// Resolve one search/replace edit into new content, or a structured error.
///
/// This is the primary entry point. It handles the empty-`old_string` sentinel
/// and the no-op guard, runs [`locate`] through the strictness ladder, enforces
/// the `replace_all` uniqueness rule, and only then splices `new_string` into a
/// fresh `String`. It never reads or writes the filesystem and never panics on
/// any `&str` input. On any guard failure it returns an [`EditError`] and builds
/// no output.
///
/// # Empty `old_string`
///
/// An empty `old_string` is the new-file / insert sentinel and is accepted only
/// when `content` is empty, producing [`MatchPass::NewContent`] with the whole
/// `new_string` as the new content (an empty-to-empty write yields an empty
/// file). Against non-empty content it is [`EditError::EmptyOldStringOnNonEmptyContent`].
/// `replace_all` is ignored on this path.
///
/// ```
/// use vyane_harness::native::{EditError, EditRequest, MatchPass, compute_edit};
///
/// // New-file write.
/// let created = compute_edit(&EditRequest {
///     content: "",
///     old_string: "",
///     new_string: "hello\n",
///     replace_all: false,
/// })
/// .expect("empty content accepts empty old_string");
/// assert_eq!(created.new_content, "hello\n");
/// assert_eq!(created.matched_pass, MatchPass::NewContent);
///
/// // Not-unique without replace_all is refused with a count and no content.
/// let ambiguous = compute_edit(&EditRequest {
///     content: "a\na\n",
///     old_string: "a",
///     new_string: "b",
///     replace_all: false,
/// });
/// assert_eq!(ambiguous, Err(EditError::NotUnique { count: 2 }));
/// ```
pub fn compute_edit(request: &EditRequest<'_>) -> Result<EditOutcome, EditError> {
    let EditRequest {
        content,
        old_string,
        new_string,
        replace_all,
    } = *request;

    // Empty old_string is the new-file/insert sentinel, valid only against empty
    // content. Guarded first so an empty pattern never reaches the matcher.
    if old_string.is_empty() {
        if content.is_empty() {
            return Ok(EditOutcome {
                new_content: new_string.to_string(),
                replacements: 1,
                matched_pass: MatchPass::NewContent,
                spans: vec![ReplacedSpan {
                    original: 0..0,
                    new_start: 0,
                    new_len: new_string.len(),
                }],
            });
        }
        return Err(EditError::EmptyOldStringOnNonEmptyContent);
    }

    // No-op guard on the non-empty-old path, checked before locate to save work.
    if old_string == new_string {
        return Err(EditError::NoOpEdit);
    }

    match locate(content, old_string) {
        MatchSearch::NoMatch => Err(EditError::NoMatch),
        MatchSearch::Ambiguous => Err(EditError::Ambiguous),
        MatchSearch::Found { pass, spans } => {
            // Fail before write: the uniqueness verdict is reached before any
            // output String is built.
            if !replace_all && spans.len() > 1 {
                return Err(EditError::NotUnique { count: spans.len() });
            }
            Ok(splice(content, &spans, new_string, pass))
        }
    }
}

/// Run the strictness ladder and report the winning pass with every
/// non-overlapping match span, in original byte coordinates.
///
/// This is the lower-level matcher, exposed independently of the splice and
/// uniqueness logic so ladder ordering, remapping, and ambiguity can be tested
/// on their own. Empty-`old_string` semantics are [`compute_edit`]'s concern;
/// `locate` on an empty `old_string` simply returns [`MatchSearch::NoMatch`].
///
/// ```
/// use vyane_harness::native::{MatchPass, MatchSearch, locate};
///
/// // A pattern using ASCII "--" matches a real em-dash region under folding.
/// match locate("a\u{2014}b", "a--b") {
///     MatchSearch::Found { pass, spans } => {
///         assert_eq!(pass, MatchPass::UnicodeNormalized);
///         assert_eq!(spans, vec![0..5]);
///     }
///     other => panic!("expected a folded match, got {other:?}"),
/// }
///
/// // A lone "-" landing inside an em-dash is refused, not silently replaced.
/// assert_eq!(locate("\u{2014}", "-"), MatchSearch::Ambiguous);
/// ```
pub fn locate(content: &str, old_string: &str) -> MatchSearch {
    if old_string.is_empty() {
        return MatchSearch::NoMatch;
    }

    for pass in LADDER {
        match try_pass(pass, content, old_string) {
            PassResult::Found(spans) => return MatchSearch::Found { pass, spans },
            PassResult::Ambiguous => return MatchSearch::Ambiguous,
            PassResult::NoCandidates => {}
        }
    }
    MatchSearch::NoMatch
}

/// The four ladder rungs in fixed, decreasing-confidence order.
const LADDER: [MatchPass; 4] = [
    MatchPass::Exact,
    MatchPass::TrailingWhitespace,
    MatchPass::LineTrim,
    MatchPass::UnicodeNormalized,
];

/// The verdict of a single ladder rung.
enum PassResult {
    /// One or more validated, non-overlapping spans (original coordinates).
    Found(Vec<Range<usize>>),
    /// The rung produced raw candidates but all were rejected by the safety net,
    /// or validated spans overlapped. Fail closed; do not fall through.
    Ambiguous,
    /// The rung produced no raw candidate at all; fall through to the next rung.
    NoCandidates,
}

/// Run one rung's five steps: normalize, scan, remap, roundtrip-validate, then
/// classify the surviving spans.
fn try_pass(pass: MatchPass, content: &str, old_string: &str) -> PassResult {
    let (normalized_text, offset_map) = normalize(pass, content);
    let (normalized_pattern, _) = normalize(pass, old_string);

    // A pattern that normalizes to empty (for example an all-whitespace
    // old_string under a stripping rung) would match at every boundary. Treat it
    // as no candidate so the ladder can fall through safely.
    if normalized_pattern.is_empty() {
        return PassResult::NoCandidates;
    }

    let raw_starts = non_overlapping_matches(&normalized_text, &normalized_pattern);
    if raw_starts.is_empty() {
        return PassResult::NoCandidates;
    }

    let mut validated = Vec::with_capacity(raw_starts.len());
    for normalized_start in raw_starts {
        let normalized_end = normalized_start + normalized_pattern.len();
        let original_start = offset_map[normalized_start];
        let original_end = offset_map[normalized_end];

        // Reject a zero-length or inverted remap (the partial-expansion case)
        // and any candidate whose original slice does not re-normalize to the
        // pattern. This roundtrip is the safety net that makes folding sound.
        if original_end <= original_start {
            continue;
        }
        if normalize(pass, &content[original_start..original_end]).0 != normalized_pattern {
            continue;
        }
        validated.push(original_start..original_end);
    }

    if validated.is_empty() {
        // Raw candidates existed but none survived: present-but-unsafe, not
        // absent. Fail closed rather than fall through.
        return PassResult::Ambiguous;
    }
    if has_overlap(&validated) {
        return PassResult::Ambiguous;
    }
    PassResult::Found(validated)
}

/// Copy `content` into a fresh string, replacing each span with `new_string`.
///
/// Spans arrive ascending and non-overlapping from [`locate`]. Every span
/// boundary is an offset-map value, so it always lands on a UTF-8 character
/// boundary and the result is valid UTF-8 by construction.
fn splice(content: &str, spans: &[Range<usize>], new_string: &str, pass: MatchPass) -> EditOutcome {
    let mut new_content = String::with_capacity(content.len());
    let mut replaced = Vec::with_capacity(spans.len());
    let mut cursor = 0;
    for span in spans {
        new_content.push_str(&content[cursor..span.start]);
        let new_start = new_content.len();
        new_content.push_str(new_string);
        replaced.push(ReplacedSpan {
            original: span.clone(),
            new_start,
            new_len: new_string.len(),
        });
        cursor = span.end;
    }
    new_content.push_str(&content[cursor..]);
    EditOutcome {
        new_content,
        replacements: spans.len(),
        matched_pass: pass,
        spans: replaced,
    }
}

/// Collect the start offsets of every non-overlapping occurrence of `pattern`
/// in `text`, advancing past each hit by the pattern length. `pattern` must be
/// non-empty (the caller guards this), which guarantees termination.
fn non_overlapping_matches(text: &str, pattern: &str) -> Vec<usize> {
    let mut starts = Vec::new();
    let mut cursor = 0;
    while let Some(relative) = text[cursor..].find(pattern) {
        let start = cursor + relative;
        starts.push(start);
        cursor = start + pattern.len();
    }
    starts
}

/// Whether any two ascending spans overlap. Because spans are sorted by start,
/// an overlap exists exactly when a span begins before its predecessor ends.
fn has_overlap(spans: &[Range<usize>]) -> bool {
    spans.windows(2).any(|pair| pair[1].start < pair[0].end)
}

/// Normalize `input` for one rung into `(normalized_text, offset_map)`.
///
/// `offset_map` has `normalized_text.len() + 1` entries: entry `i` is the byte
/// offset in `input` of the source character that emitted normalized byte `i`,
/// and the final entry is `input.len()`. Expanding folds (em-dash to `"--"`)
/// give consecutive emitted bytes the same source offset; stripping folds
/// (whitespace removal) simply emit nothing for the removed source bytes.
fn normalize(pass: MatchPass, input: &str) -> (String, Vec<usize>) {
    match pass {
        MatchPass::Exact | MatchPass::NewContent => identity_map(input),
        MatchPass::TrailingWhitespace => strip_line_edges(input, LineEdge::TrailingOnly),
        MatchPass::LineTrim => strip_line_edges(input, LineEdge::BothSides),
        MatchPass::UnicodeNormalized => {
            // Fold confusables first, then line-trim the folded text so that an
            // exotic space folded to ASCII can itself be trimmed. Compose the two
            // offset maps back onto the original input coordinates.
            let (folded, fold_map) = fold_confusables(input);
            let (trimmed, trim_map) = strip_line_edges(&folded, LineEdge::BothSides);
            let composed = trim_map
                .iter()
                .map(|&folded_offset| fold_map[folded_offset])
                .collect();
            (trimmed, composed)
        }
    }
}

/// Identity normalization: the text is copied verbatim and every byte maps to
/// its own offset.
fn identity_map(input: &str) -> (String, Vec<usize>) {
    (input.to_string(), (0..=input.len()).collect())
}

/// Which edges of each line to strip whitespace from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LineEdge {
    /// Strip only the trailing run before each newline (and at end of input).
    /// Tolerates trailing-whitespace drift and, by removing a `\r` before `\n`,
    /// CRLF-vs-LF drift.
    TrailingOnly,
    /// Strip both the leading and trailing run of each line. Tolerates
    /// indentation drift on interior lines of a block.
    BothSides,
}

/// Strip whitespace from the requested edges of every line, preserving line
/// structure and interior content, and record the source offset of each kept
/// byte. Only the ASCII bytes space, tab, and carriage-return are stripped, so a
/// strip boundary can never fall inside a multibyte character.
fn strip_line_edges(input: &str, mode: LineEdge) -> (String, Vec<usize>) {
    let bytes = input.as_bytes();
    let mut text = String::with_capacity(input.len());
    let mut map = Vec::with_capacity(input.len() + 1);
    let mut line_start = 0;
    loop {
        let line_end = next_newline(bytes, line_start);
        let mut content_start = line_start;
        if matches!(mode, LineEdge::BothSides) {
            while content_start < line_end && is_edge_whitespace(bytes[content_start]) {
                content_start += 1;
            }
        }
        let mut content_end = line_end;
        while content_end > content_start && is_edge_whitespace(bytes[content_end - 1]) {
            content_end -= 1;
        }
        text.push_str(&input[content_start..content_end]);
        map.extend(content_start..content_end);
        if line_end < bytes.len() {
            text.push('\n');
            map.push(line_end);
            line_start = line_end + 1;
        } else {
            break;
        }
    }
    map.push(input.len());
    (text, map)
}

/// The byte offset of the next `\n` at or after `from`, or `bytes.len()` if the
/// remaining input has none.
fn next_newline(bytes: &[u8], from: usize) -> usize {
    bytes[from..]
        .iter()
        .position(|&byte| byte == b'\n')
        .map_or(bytes.len(), |relative| from + relative)
}

/// Whether a byte is one of the ASCII line-edge whitespace bytes.
fn is_edge_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\r')
}

/// Fold typographic confusables to their ASCII equivalents, recording the source
/// offset of every emitted byte. Non-confusable characters (including all
/// multibyte CJK, emoji, and accented latin) pass through byte-for-byte.
fn fold_confusables(input: &str) -> (String, Vec<usize>) {
    let mut text = String::with_capacity(input.len());
    let mut map = Vec::with_capacity(input.len() + 1);
    for (offset, character) in input.char_indices() {
        match fold_character(character) {
            Some(folded) => {
                text.push_str(folded);
                map.extend(std::iter::repeat_n(offset, folded.len()));
            }
            None => {
                text.push(character);
                map.extend(std::iter::repeat_n(offset, character.len_utf8()));
            }
        }
    }
    map.push(input.len());
    (text, map)
}

/// The ASCII fold for one confusable character, or `None` to pass it through.
///
/// This is vyane-harness's own decision to use the *expanding* fold: em-dash and
/// horizontal-bar expand to `"--"` and the ellipsis to `"..."`, which is
/// typographically faithful and forces the roundtrip and ambiguity machinery in
/// [`try_pass`] to keep partial-expansion matches safe.
fn fold_character(character: char) -> Option<&'static str> {
    match character {
        // Hyphen-like dashes fold to a single ASCII hyphen.
        '\u{2010}' | '\u{2011}' | '\u{2012}' | '\u{2013}' | '\u{2212}' => Some("-"),
        // Em-dash and horizontal bar expand to two ASCII hyphens.
        '\u{2014}' | '\u{2015}' => Some("--"),
        // Horizontal ellipsis expands to three ASCII dots.
        '\u{2026}' => Some("..."),
        // Single curly and low quotation marks fold to an ASCII apostrophe.
        '\u{2018}' | '\u{2019}' | '\u{201A}' | '\u{201B}' => Some("'"),
        // Double curly and low quotation marks fold to an ASCII double quote.
        '\u{201C}' | '\u{201D}' | '\u{201E}' | '\u{201F}' => Some("\""),
        // No-break, en/em and other exotic spaces fold to an ASCII space.
        '\u{00A0}' | '\u{2002}'..='\u{200A}' | '\u{202F}' | '\u{205F}' | '\u{3000}' => Some(" "),
        _ => None,
    }
}
