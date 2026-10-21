# README translation maintenance

[README.md](../README.md) is the English source. Initial locales cover Simplified
Chinese, Japanese, Korean, Spanish, French, German and Brazilian Portuguese for
contributors who prefer those languages. This selection is an initial coverage
choice, not a measured user-demographic claim.

## Coverage and review status

All versions cover the same sections: purpose/audience, the local fixture demo,
three uses, source installation and offline first run, current boundaries,
deep-document links and a collapsed evidence snapshot, Python migration boundary, contribution/security and licensing.
Full provider setup, architecture and parity detail live in linked English guides.

| Locale | File | Content source | Review status |
| --- | --- | --- | --- |
| English | [README.md](../README.md) | Canonical | Source and command verification recorded in the PR |
| 简体中文 | [README.zh-CN.md](../README.zh-CN.md) | English README | Machine-assisted draft; native-speaker review pending |
| 日本語 | [README.ja.md](../README.ja.md) | English README | Machine-assisted draft; native-speaker review pending |
| 한국어 | [README.ko.md](../README.ko.md) | English README | Machine-assisted draft; native-speaker review pending |
| Español | [README.es.md](../README.es.md) | English README | Machine-assisted draft; native-speaker review pending |
| Français | [README.fr.md](../README.fr.md) | English README | Machine-assisted draft; native-speaker review pending |
| Deutsch | [README.de.md](../README.de.md) | English README | Machine-assisted draft; native-speaker review pending |
| Português (Brasil) | [README.pt-BR.md](../README.pt-BR.md) | English README | Machine-assisted draft; native-speaker review pending |

Each README records source baseline `fe43941`, workspace `0.1.0`, and update date
2026-10-15. The baseline identifies the implementation whose facts were checked;
the English text is the README in the same documentation revision. The source
text hash in `translation-source.json` detects English text drift, not semantic
accuracy. Git history identifies the exact English documentation revision.

## Update rules

1. Change English facts first and update all affected translations in the same
   PR, especially installation, permissions/cost, compatibility and limits.
   If a translation cannot be synchronized, mark it stale at its top and in this
   table; link readers to current English. Never silently retain a green status.
2. Keep code fences byte-identical, including prompts, flags, paths, identifiers,
   environment variable names, API names and placeholder values. Translate the
   surrounding explanation. Keep destination links identical where possible.
3. Retain the same section order, capability rows and limitation qualifiers.
   Explain that a source milestone is not a release and a fixture match is not
   full migration readiness. Do not add local claims or provider recommendations.
4. Review terminology and meaning, not only file presence or hashes. A fluent
   human may record the locale, reviewed source revision and scope in a PR;
   until that happens, retain the pending native-review label. AI review is not
   native human review.
5. Recheck local links/anchors and code-fence equality, then refresh the source
   hash only for translations actually synchronized. Record command verification
   separately from translation review. Do not infer one from the other.

## Terminology

| Term | Meaning to preserve |
| --- | --- |
| provider | Endpoint, credential source and billing identity; not a model or CLI. |
| protocol | HTTP wire format. |
| harness | Execution adapter around a coding CLI; `none` is direct HTTP. |
| model | Identifier within one provider's namespace. |
| profile / target | A named configuration / its resolved execution binding. |
| failover | An explicit ordered alternate-target attempt, subject to policy. |
| normalized match | Agreement only after the fixture's declared normalization. |
| partial / different | Incomplete capability / explicit contract difference; never “done”. |

Detailed guides and the changelog currently remain in their existing languages;
this first translation batch covers the complete README, not the whole docs tree.
