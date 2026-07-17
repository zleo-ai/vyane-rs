//! Independent behavioral tests for the pure search/replace edit module.
//!
//! Authored by a separate agent from the implementation: every case asserts on
//! observable behavior (exact bytes, exact variant, exact pass, exact span
//! coordinates), never on the module's self-report. The `test_matrix` rows
//! `T01..T20` are covered directly; the `MV01..MV10` mutation rows are covered
//! by assertions engineered to flip red under the described mutation — see the
//! `mutation_note` on each such test for which invariant it pins.

#![allow(clippy::unwrap_used)]

use vyane_harness::native::{EditError, EditRequest, MatchPass, MatchSearch, compute_edit, locate};

/// Build a non-`replace_all` request.
fn edit<'a>(content: &'a str, old: &'a str, new: &'a str) -> EditRequest<'a> {
    EditRequest {
        content,
        old_string: old,
        new_string: new,
        replace_all: false,
    }
}

/// Build a `replace_all` request.
fn edit_all<'a>(content: &'a str, old: &'a str, new: &'a str) -> EditRequest<'a> {
    EditRequest {
        content,
        old_string: old,
        new_string: new,
        replace_all: true,
    }
}

// ---------------------------------------------------------------------------
// T01 — Exact unique match, replace_all = false.
// ---------------------------------------------------------------------------
#[test]
fn t01_exact_unique_match() {
    let out = compute_edit(&edit("let x = 1;\n", "x = 1", "x = 2")).unwrap();
    assert_eq!(out.new_content, "let x = 2;\n");
    assert_eq!(out.replacements, 1);
    assert_eq!(out.matched_pass, MatchPass::Exact);
    assert_eq!(out.spans.len(), 1);
    assert_eq!(out.spans[0].original, 4..9);
    // Inserted text is recorded where it actually landed.
    assert_eq!(
        &out.new_content[out.spans[0].new_start..][..out.spans[0].new_len],
        "x = 2"
    );
}

// ---------------------------------------------------------------------------
// T02 — Exact multiple matches, replace_all = false -> NotUnique, no content.
// mutation_note (MV02): if the uniqueness guard were moved AFTER splicing, this
// would return Ok with the first `a` replaced; asserting the exact Err variant
// (and that no Ok content exists) pins guard-order = fail-before-write.
// ---------------------------------------------------------------------------
#[test]
fn t02_exact_multiple_not_unique() {
    let result = compute_edit(&edit("a\na\n", "a", "b"));
    assert_eq!(result, Err(EditError::NotUnique { count: 2 }));
    assert!(
        result.is_err(),
        "no content may be materialized on the non-unique path"
    );
}

// ---------------------------------------------------------------------------
// T03 — Exact multiple matches, replace_all = true.
// ---------------------------------------------------------------------------
#[test]
fn t03_exact_multiple_replace_all() {
    let out = compute_edit(&edit_all("a\na\n", "a", "b")).unwrap();
    assert_eq!(out.new_content, "b\nb\n");
    assert_eq!(out.replacements, 2);
    assert_eq!(out.matched_pass, MatchPass::Exact);
}

// ---------------------------------------------------------------------------
// T04 — No match on any rung.
// ---------------------------------------------------------------------------
#[test]
fn t04_no_match() {
    assert_eq!(
        compute_edit(&edit("foo\n", "zzz", "q")),
        Err(EditError::NoMatch)
    );
}

// ---------------------------------------------------------------------------
// T05 — Empty old_string + empty content (new file).
// ---------------------------------------------------------------------------
#[test]
fn t05_empty_old_empty_content_new_file() {
    let out = compute_edit(&edit("", "", "hello\n")).unwrap();
    assert_eq!(out.new_content, "hello\n");
    assert_eq!(out.replacements, 1);
    assert_eq!(out.matched_pass, MatchPass::NewContent);
    assert_eq!(out.spans.len(), 1);
    assert_eq!(out.spans[0].original, 0..0);
    assert_eq!(out.spans[0].new_start, 0);
    assert_eq!(out.spans[0].new_len, "hello\n".len());
}

// ---------------------------------------------------------------------------
// T06 — Empty old_string + non-empty content.
// mutation_note (MV06): removing the empty-old guard would let the empty pattern
// reach the matcher (panic/loop/prepend); asserting the exact Err variant pins
// the guard.
// ---------------------------------------------------------------------------
#[test]
fn t06_empty_old_nonempty_content() {
    assert_eq!(
        compute_edit(&edit("x", "", "y")),
        Err(EditError::EmptyOldStringOnNonEmptyContent)
    );
}

// ---------------------------------------------------------------------------
// T07 — old_string == new_string (non-empty).
// mutation_note (MV10): removing the no-op guard would return Ok with unchanged
// content; asserting the exact Err variant pins the guard.
// ---------------------------------------------------------------------------
#[test]
fn t07_no_op_edit() {
    assert_eq!(
        compute_edit(&edit("foo", "foo", "foo")),
        Err(EditError::NoOpEdit)
    );
}

// ---------------------------------------------------------------------------
// T08 — Pass 2: pattern carries trailing whitespace the file lacks.
// mutation_note (MV09): asserts a specifically non-Exact pass; a hardcoded
// `matched_pass = Exact` would fail here.
// ---------------------------------------------------------------------------
#[test]
fn t08_trailing_whitespace_pass() {
    let out = compute_edit(&edit("let x = 1\n", "let x = 1  ", "let x = 2")).unwrap();
    assert_eq!(out.matched_pass, MatchPass::TrailingWhitespace);
    assert_eq!(out.replacements, 1);
    assert_eq!(out.new_content, "let x = 2\n");
}

// ---------------------------------------------------------------------------
// T09 — Pass 3: multi-line block with interior indentation drift.
// The 4-space-indented original block is the replaced span; leading whitespace
// inside the span is consumed, output uses new_string verbatim.
// ---------------------------------------------------------------------------
#[test]
fn t09_line_trim_pass() {
    let content = "if x:\n    foo()\n    bar()\n";
    let old = "if x:\n  foo()\n  bar()";
    let new = "if x:\n    baz()";
    let out = compute_edit(&edit(content, old, new)).unwrap();
    assert_eq!(out.matched_pass, MatchPass::LineTrim);
    assert_eq!(out.replacements, 1);
    assert_eq!(out.new_content, "if x:\n    baz()\n");
    // The whole indented block (up to but excluding the trailing newline) is the span.
    assert_eq!(out.spans[0].original, 0..25);
}

// ---------------------------------------------------------------------------
// T10 — CRLF file, LF pattern.
// mutation_note (MV08): dropping '\r' from the pass-2 strip set turns this into
// Err(NoMatch); asserting Ok + TrailingWhitespace pins the CR handling.
// ---------------------------------------------------------------------------
#[test]
fn t10_crlf_file_lf_pattern() {
    let out = compute_edit(&edit("a\r\nb\r\n", "a\nb", "c\nd")).unwrap();
    assert_eq!(out.matched_pass, MatchPass::TrailingWhitespace);
    assert_eq!(out.replacements, 1);
    assert_eq!(out.new_content, "c\nd\n");
}

// ---------------------------------------------------------------------------
// T11 — Pass 4: file has smart quotes, pattern ASCII quotes.
// mutation_note (MV07): splicing on normalized offsets instead of remapped
// original offsets would slice mid-codepoint; asserting exact bytes + UTF-8
// validity pins the remap.
// ---------------------------------------------------------------------------
#[test]
fn t11_unicode_smart_quotes() {
    let content = "say \u{201C}hi\u{201D} ok";
    let out = compute_edit(&edit(content, "\"hi\"", "'yo'")).unwrap();
    assert_eq!(out.matched_pass, MatchPass::UnicodeNormalized);
    assert_eq!(out.replacements, 1);
    assert_eq!(out.new_content, "say 'yo' ok");
    assert!(std::str::from_utf8(out.new_content.as_bytes()).is_ok());
}

// ---------------------------------------------------------------------------
// T12 — Pass 4 full-expansion: em-dash region, pattern '--'.
// mutation_note (MV07): normalized-offset splice would corrupt the multibyte
// em-dash; asserting exact bytes + UTF-8 validity pins the remap.
// ---------------------------------------------------------------------------
#[test]
fn t12_unicode_em_dash_full_expansion() {
    let out = compute_edit(&edit("a\u{2014}b", "a--b", "a-b")).unwrap();
    assert_eq!(out.matched_pass, MatchPass::UnicodeNormalized);
    assert_eq!(out.new_content, "a-b");
    assert!(std::str::from_utf8(out.new_content.as_bytes()).is_ok());
}

// ---------------------------------------------------------------------------
// T13 — Pass 4 nbsp -> space.
// ---------------------------------------------------------------------------
#[test]
fn t13_unicode_nbsp() {
    let out = compute_edit(&edit("hello\u{00A0}world", "hello world", "hi")).unwrap();
    assert_eq!(out.matched_pass, MatchPass::UnicodeNormalized);
    assert_eq!(out.replacements, 1);
    assert_eq!(out.new_content, "hi");
}

// ---------------------------------------------------------------------------
// T14 — Partial-expansion refused (Ambiguous, NOT NoMatch, NOT bogus replace).
// mutation_note (MV03): removing the step-5 roundtrip check accepts a garbage
// span (returns Ok/corrupt). mutation_note (MV04): returning NoMatch on
// all-rejected instead of Ambiguous also fails this. Both are pinned by
// asserting the exact Ambiguous variant.
// ---------------------------------------------------------------------------
#[test]
fn t14_partial_expansion_refused() {
    assert_eq!(
        compute_edit(&edit("\u{2014}", "-", "x")),
        Err(EditError::Ambiguous)
    );
    // Same fact at the locate layer, distinguishing Ambiguous from NoMatch.
    assert_eq!(locate("\u{2014}", "-"), MatchSearch::Ambiguous);
}

// ---------------------------------------------------------------------------
// T15 — Exact wins over fuzzy; ladder stops at Exact.
// mutation_note (MV01): reorder/union the ladder and the winning pass or span
// set changes; asserting Found{Exact, [0..3, 6..9]} at the locate layer plus
// NotUnique{2} at compute_edit pins exact-first short-circuit.
// ---------------------------------------------------------------------------
#[test]
fn t15_exact_wins_over_fuzzy() {
    let content = "foo  \nfoo\n";
    // Literal "foo" occurs exactly twice, so pass 1 already resolves it and the
    // whitespace rung is never consulted.
    assert_eq!(
        locate(content, "foo"),
        MatchSearch::Found {
            pass: MatchPass::Exact,
            spans: vec![0..3, 6..9],
        }
    );
    assert_eq!(
        compute_edit(&edit(content, "foo", "bar")),
        Err(EditError::NotUnique { count: 2 })
    );
}

// ---------------------------------------------------------------------------
// T16 — Non-overlapping match count.
// mutation_note (MV05): advancing the scan cursor by 1 instead of the pattern
// length would report 3 overlapping matches; asserting count == 2 and "bb" pins
// non-overlapping scanning.
// ---------------------------------------------------------------------------
#[test]
fn t16_non_overlapping_count() {
    let out = compute_edit(&edit_all("aaaa", "aa", "b")).unwrap();
    assert_eq!(out.replacements, 2);
    assert_eq!(out.new_content, "bb");
    assert_eq!(
        locate("aaaa", "aa"),
        MatchSearch::Found {
            pass: MatchPass::Exact,
            spans: vec![0..2, 2..4],
        }
    );
}

// ---------------------------------------------------------------------------
// T17 — Deletion (empty new_string).
// ---------------------------------------------------------------------------
#[test]
fn t17_deletion() {
    let out = compute_edit(&edit("keep DROP keep", " DROP", "")).unwrap();
    assert_eq!(out.new_content, "keep keep");
    assert_eq!(out.replacements, 1);
    assert_eq!(out.spans[0].new_len, 0);
}

// ---------------------------------------------------------------------------
// T18 — Match at start and at end of content.
// ---------------------------------------------------------------------------
#[test]
fn t18_match_at_boundaries() {
    let start = compute_edit(&edit("X mid Y", "X", "Z")).unwrap();
    assert_eq!(start.spans[0].original, 0..1);
    assert_eq!(start.new_content, "Z mid Y");

    let end = compute_edit(&edit("X mid Y", "Y", "Z")).unwrap();
    assert_eq!(end.spans[0].original, 6..7);
    assert_eq!(end.new_content, "X mid Z");
}

// ---------------------------------------------------------------------------
// T19 — Non-confusable multibyte preserved byte-exactly.
// ---------------------------------------------------------------------------
#[test]
fn t19_multibyte_preserved() {
    let out = compute_edit(&edit("前缀 target 后缀", "target", "目标")).unwrap();
    assert_eq!(out.new_content, "前缀 目标 后缀");
    assert_eq!(out.matched_pass, MatchPass::Exact);
    assert!(std::str::from_utf8(out.new_content.as_bytes()).is_ok());
}

// ---------------------------------------------------------------------------
// T20 — locate() direct: winning pass + spans in original coordinates.
// ---------------------------------------------------------------------------
#[test]
fn t20_locate_reports_pass_and_spans() {
    // "a" + em-dash(3 bytes) + "b" = 0..5 in original coordinates.
    match locate("a\u{2014}b", "a--b") {
        MatchSearch::Found { pass, spans } => {
            assert_eq!(pass, MatchPass::UnicodeNormalized);
            assert_eq!(spans.len(), 1);
            assert_eq!(spans[0], 0..5);
        }
        other => panic!("expected a folded match, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Extra guards for the mutation matrix that need their own inputs.
// ---------------------------------------------------------------------------

/// locate on an empty old_string is defined to be NoMatch (compute_edit owns the
/// empty-old semantics). Pins the documented contract of the lower-level fn.
#[test]
fn locate_empty_old_is_no_match() {
    assert_eq!(locate("anything", ""), MatchSearch::NoMatch);
}

/// New-file write from empty to empty yields an empty file (NewContent path).
#[test]
fn empty_to_empty_new_file() {
    let out = compute_edit(&edit("", "", "")).unwrap();
    assert_eq!(out.new_content, "");
    assert_eq!(out.matched_pass, MatchPass::NewContent);
}
