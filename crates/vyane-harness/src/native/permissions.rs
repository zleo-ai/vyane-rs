use std::collections::BTreeMap;
use std::fmt;
use std::sync::OnceLock;

use regex::Regex;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use super::{ToolCall, ToolContext};

/// Result of applying one native-harness permission policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionEffect {
    Allow,
    Ask,
    Deny,
}

impl PermissionEffect {
    fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Ask => "ask",
            Self::Deny => "deny",
        }
    }
}

impl fmt::Display for PermissionEffect {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Error)]
pub enum PermissionRuleError {
    #[error("tool pattern must not be empty")]
    EmptyToolPattern,
    #[error("argument name must not be empty")]
    EmptyArgumentName,
    #[error("invalid regex for argument `{argument}`: {source}")]
    InvalidArgumentRegex {
        argument: String,
        #[source]
        source: regex::Error,
    },
    #[error("a non-overridable permission floor rule must deny")]
    FloorRuleMustDeny,
}

#[derive(Debug, Clone)]
struct ArgumentMatcher {
    argument: String,
    source: String,
    regex: Regex,
}

#[derive(Debug, Clone, Copy)]
enum BuiltinMatcher {
    RiskyRecursiveRemove,
}

#[derive(Debug, Clone)]
enum GlobToken {
    AnySequence,
    AnyCharacter,
    CharacterClass {
        negated: bool,
        items: Vec<CharacterClassItem>,
    },
    Literal(char),
}

#[derive(Debug, Clone)]
enum CharacterClassItem {
    Literal(char),
    Range(char, char),
}

/// One ordered permission rule. Later matching rules override earlier ones.
#[derive(Debug, Clone)]
pub struct PermissionRule {
    tool_pattern: String,
    tool_matcher: Vec<GlobToken>,
    argument_matchers: Vec<ArgumentMatcher>,
    builtin_matcher: Option<BuiltinMatcher>,
    effect: PermissionEffect,
}

impl PermissionRule {
    pub fn new(
        tool_pattern: impl Into<String>,
        effect: PermissionEffect,
    ) -> Result<Self, PermissionRuleError> {
        let tool_pattern = tool_pattern.into();
        if tool_pattern.is_empty() {
            return Err(PermissionRuleError::EmptyToolPattern);
        }
        Ok(Self {
            tool_matcher: parse_fnmatch_pattern(&tool_pattern),
            tool_pattern,
            argument_matchers: Vec::new(),
            builtin_matcher: None,
            effect,
        })
    }

    /// Require one argument to exist and match `pattern`.
    pub fn with_argument_pattern(
        mut self,
        argument: impl Into<String>,
        pattern: impl Into<String>,
    ) -> Result<Self, PermissionRuleError> {
        let argument = argument.into();
        if argument.is_empty() {
            return Err(PermissionRuleError::EmptyArgumentName);
        }
        let source = pattern.into();
        let regex =
            Regex::new(&source).map_err(|source| PermissionRuleError::InvalidArgumentRegex {
                argument: argument.clone(),
                source,
            })?;
        self.argument_matchers.push(ArgumentMatcher {
            argument,
            source,
            regex,
        });
        Ok(self)
    }

    pub fn tool_pattern(&self) -> &str {
        &self.tool_pattern
    }

    pub fn effect(&self) -> PermissionEffect {
        self.effect
    }

    fn matches(&self, call: &ToolCall) -> bool {
        fnmatch_matches(&self.tool_matcher, &call.name)
            && self.argument_matchers.iter().all(|matcher| {
                call.arguments.get(&matcher.argument).is_some_and(|value| {
                    matcher.regex.is_match(&matchable_argument_value(
                        &call.name,
                        &matcher.argument,
                        value,
                    ))
                })
            })
            && self
                .builtin_matcher
                .is_none_or(|matcher| matcher.matches(call))
    }

    fn description(&self) -> String {
        if self.argument_matchers.is_empty() {
            return format!("{} => {}", self.tool_pattern, self.effect);
        }
        let arguments = self
            .argument_matchers
            .iter()
            .map(|matcher| format!("{}~{}", matcher.argument, matcher.source))
            .collect::<Vec<_>>()
            .join(",");
        format!("{}[{arguments}] => {}", self.tool_pattern, self.effect)
    }

    fn risky_recursive_remove() -> Result<Self, PermissionRuleError> {
        let mut rule = Self::new("run_bash", PermissionEffect::Ask)?;
        rule.builtin_matcher = Some(BuiltinMatcher::RiskyRecursiveRemove);
        Ok(rule)
    }
}

impl BuiltinMatcher {
    fn matches(self, call: &ToolCall) -> bool {
        match self {
            Self::RiskyRecursiveRemove => risky_recursive_remove_matches(call),
        }
    }
}

/// Ordered tool-call policy. Its default is explicit and rules use
/// find-last semantics so a narrow rule can override a broad baseline.
#[derive(Debug, Clone)]
pub struct PermissionPolicy {
    rules: Vec<PermissionRule>,
    floor_rules: Vec<PermissionRule>,
    default_effect: PermissionEffect,
}

impl Default for PermissionPolicy {
    fn default() -> Self {
        // The Python native-harness hook is an opt-in convenience above an OS
        // sandbox and therefore defaults to allow. This public Rust execution
        // seam can host arbitrary third-party tools before that sandbox layer
        // exists, so its implicit/default policy is deliberately fail-closed.
        // `allow_by_default()` exposes only the legacy low-level hook default;
        // the Python adapter's protected-path floor is built explicitly by
        // `protected_paths_policy()`.
        Self::deny_by_default()
    }
}

impl PermissionPolicy {
    pub fn new(default_effect: PermissionEffect) -> Self {
        Self {
            rules: Vec::new(),
            floor_rules: Vec::new(),
            default_effect,
        }
    }

    pub fn allow_by_default() -> Self {
        Self::new(PermissionEffect::Allow)
    }

    pub fn deny_by_default() -> Self {
        Self::new(PermissionEffect::Deny)
    }

    pub fn push_rule(&mut self, rule: PermissionRule) {
        self.rules.push(rule);
    }

    /// Add a non-overridable deny rule evaluated after ordinary find-last
    /// rules. This is reserved for security floors such as protected paths.
    pub fn push_floor_rule(&mut self, rule: PermissionRule) -> Result<(), PermissionRuleError> {
        if rule.effect != PermissionEffect::Deny {
            return Err(PermissionRuleError::FloorRuleMustDeny);
        }
        self.floor_rules.push(rule);
        Ok(())
    }

    #[must_use]
    pub fn with_rule(mut self, rule: PermissionRule) -> Self {
        self.push_rule(rule);
        self
    }

    pub fn decide(&self, call: &ToolCall, context: &ToolContext) -> PermissionDecision {
        let matched = self
            .rules
            .iter()
            .enumerate()
            .rfind(|(_, rule)| rule.matches(call));
        let floor = self
            .floor_rules
            .iter()
            .enumerate()
            .rfind(|(_, rule)| rule.matches(call));
        let (effect, matched_rule_index, matched_rule, matched_floor) = match floor {
            Some((index, rule)) => (PermissionEffect::Deny, Some(index), Some(rule), true),
            None => match matched {
                Some((index, rule)) => (rule.effect, Some(index), Some(rule), false),
                None => (self.default_effect, None, None, false),
            },
        };
        let approval = if effect == PermissionEffect::Ask {
            ApprovalPlan::new(call, context, matched_rule.map(PermissionRule::description))
        } else {
            None
        };
        PermissionDecision {
            effect,
            matched_rule_index,
            matched_floor,
            approval,
        }
    }
}

// A protected basename must be separated from adjacent filename characters,
// not just from `/`. Shell syntax is deliberately treated as a boundary too:
// `.git;`, `(.ssh)`, and `GIT_DIR=.git` are all protected while `.gitfoo` is
// a distinct basename and remains outside this floor.
const PROTECTED_PATH_PATTERN: &str = r#"(?i)(^|[^A-Za-z0-9_.-])((?:\.git|\.ssh|\.aws|\.codex|\.vyane)(?:$|[^A-Za-z0-9_.-])|(?:\.env(?:[._-][A-Za-z0-9_-]+)*|secrets?[A-Za-z0-9_.-]*\.env(?:[._-][A-Za-z0-9_-]+)*)(?:$|[^A-Za-z0-9_.-]))"#;

const RISKY_COMMAND_PATTERNS: &[&str] = &[
    r"(?i)\bgit\b[^\n;&|]*(?:\s-c\b|\s--config\b)[^\n;&|]*\balias\.",
    r"(?i)\bgit\b[^\n;&|]*\bpush\b[^\n;&|]*(?:--force(?:-with-lease)?\b|-[A-Za-z]*f[A-Za-z]*\b|\+[^\s;&|]+)",
    r"(?i)\bgit\b[^\n;&|]*\bpush\b[^\n;&|]*(?:\$\(|`|\$\{)",
    r"(?i)\bgit\b[^\n;&|]*\breset\b[^\n;&|]*--hard\b",
    r"(?i)\bgit\b[^\n;&|]*\bclean\b[^\n;&|]*(?:-[A-Za-z]*f[A-Za-z]*[^\n;&|]*-[A-Za-z]*d|-[A-Za-z]*d[A-Za-z]*[^\n;&|]*-[A-Za-z]*f|-[A-Za-z]*f[A-Za-z]*d|-[A-Za-z]*d[A-Za-z]*f)",
    r"(?i)\brm\b[^\n;&|]*(?:\$\(|`|\$\{)",
    r"(?i)\b(?:chmod|chown)\b[^\n;&|]*(?:-[A-Za-z]*R[A-Za-z]*\b|--recursive\b)",
    r"(?i)\bmkfs(?:\.[A-Za-z0-9_]+)?\b",
    r"(?i)\bdd\b[^\n;&|]*\bif=",
];

/// Protect native write tools and fail closed for shell execution.
///
/// A shell can synthesize protected names through globbing, substitutions, or
/// another interpreter, so text classification cannot enforce path safety.
/// Until the native harness has an OS-level path-capability sandbox, enabling
/// this security floor intentionally denies every `run_bash` invocation.
pub fn protected_paths_policy() -> Result<PermissionPolicy, PermissionRuleError> {
    let write = PermissionRule::new("write_*", PermissionEffect::Deny)?
        .with_argument_pattern("path", PROTECTED_PATH_PATTERN)?;
    let shell = PermissionRule::new("run_bash", PermissionEffect::Deny)?;
    let mut policy = PermissionPolicy::allow_by_default();
    policy.push_floor_rule(write)?;
    policy.push_floor_rule(shell)?;
    Ok(policy)
}

/// Opt-in approval classifier for high-impact shell operations.
///
/// This preset deliberately does not include [`protected_paths_policy`] and is
/// not a standalone security profile. It may be used only when an independent
/// OS sandbox/path-capability layer constrains `run_bash`; otherwise use the
/// protected-path policy, which currently denies shell execution entirely.
pub fn risky_operations_policy() -> Result<PermissionPolicy, PermissionRuleError> {
    let mut policy = PermissionPolicy::allow_by_default();
    for pattern in RISKY_COMMAND_PATTERNS {
        policy.push_rule(
            PermissionRule::new("run_bash", PermissionEffect::Ask)?
                .with_argument_pattern("command", *pattern)?,
        );
    }
    policy.push_rule(PermissionRule::risky_recursive_remove()?);
    Ok(policy)
}

#[derive(Debug, Clone, PartialEq)]
pub struct PermissionDecision {
    pub effect: PermissionEffect,
    pub matched_rule_index: Option<usize>,
    pub matched_floor: bool,
    pub approval: Option<ApprovalPlan>,
}

/// Canonical, hash-bound plan emitted by an `ask` decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalPlan {
    pub schema: u32,
    pub tool: String,
    pub arguments: BTreeMap<String, Value>,
    pub cwd: String,
    pub tool_call_id: String,
    pub matched_rule: Option<String>,
    /// Cross-language plan digest compatible with the original Python
    /// canonical JSON contract for the supported integer-only JSON subset.
    pub canonical_plan_hash: String,
    /// Versioned invocation binding that also includes the tool-call id. Future
    /// approval execution must consume this hash, not the reusable plan hash.
    pub approval_binding_hash: String,
}

impl ApprovalPlan {
    fn new(call: &ToolCall, context: &ToolContext, matched_rule: Option<String>) -> Option<Self> {
        let cwd = context.workdir().to_str()?.to_string();
        let canonical = canonical_plan_json(&call.name, &call.arguments, &cwd)?;
        let canonical_plan_hash = approval_plan_hash(&canonical);
        let approval_binding_hash = approval_binding_hash(&canonical, &call.id);
        Some(Self {
            schema: 1,
            tool: call.name.clone(),
            arguments: call.arguments.clone(),
            cwd,
            tool_call_id: call.id.clone(),
            matched_rule,
            canonical_plan_hash,
            approval_binding_hash,
        })
    }
}

fn approval_plan_hash(canonical_plan_json: &[u8]) -> String {
    Sha256::digest(canonical_plan_json)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn approval_binding_hash(canonical_plan_json: &[u8], tool_call_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"vyane.native.approval-binding.v1\0");
    digest.update(canonical_plan_json);
    digest.update([0]);
    digest.update(tool_call_id.as_bytes());
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn canonical_plan_json(
    tool: &str,
    arguments: &BTreeMap<String, Value>,
    cwd: &str,
) -> Option<Vec<u8>> {
    let mut root = BTreeMap::new();
    let arguments = arguments
        .iter()
        .map(|(key, value)| Some((key.clone(), canonical_approval_value(value)?)))
        .collect::<Option<BTreeMap<_, _>>>()?;
    root.insert("arguments", serde_json::to_value(arguments).ok()?);
    root.insert("cwd", Value::String(cwd.to_string()));
    root.insert("tool", Value::String(tool.to_string()));
    serde_json::to_vec(&root).ok()
}

fn canonical_approval_value(value: &Value) -> Option<Value> {
    match value {
        Value::Null | Value::Bool(_) | Value::String(_) => Some(value.clone()),
        Value::Number(number) if number.as_i64().is_some() || number.as_u64().is_some() => {
            Some(value.clone())
        }
        Value::Number(_) => None,
        Value::Array(values) => values
            .iter()
            .map(canonical_approval_value)
            .collect::<Option<Vec<_>>>()
            .map(Value::Array),
        Value::Object(values) => {
            let sorted = values
                .iter()
                .map(|(key, value)| Some((key.clone(), canonical_approval_value(value)?)))
                .collect::<Option<BTreeMap<_, _>>>()?;
            serde_json::to_value(sorted).ok()
        }
    }
}

fn matchable_argument_value(tool_name: &str, argument: &str, value: &Value) -> String {
    let text = python_argument_text(value);
    if tool_name == "run_bash" && argument == "command" {
        let stripped = strip_shell_quote_syntax(&text);
        let expanded = expand_shell_variable_literal_tokens(&stripped);
        return format!("{stripped}\n{}", shell_unspaced_token_view(&expanded));
    }
    text
}

fn python_argument_text(value: &Value) -> String {
    match value {
        Value::Null => "None".into(),
        Value::Bool(true) => "True".into(),
        Value::Bool(false) => "False".into(),
        Value::Number(value) => python_number_text(value),
        Value::String(value) => value.clone(),
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(python_repr)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::Object(values) => format!(
            "{{{}}}",
            values
                .iter()
                .map(|(key, value)| format!("{}: {}", python_string_repr(key), python_repr(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn python_number_text(value: &serde_json::Number) -> String {
    let text = value.to_string();
    let Some((mantissa, exponent)) = text.split_once('e').or_else(|| text.split_once('E')) else {
        return text;
    };
    let Ok(exponent) = exponent.parse::<i32>() else {
        return text;
    };
    format!("{mantissa}e{exponent:+03}")
}

fn python_repr(value: &Value) -> String {
    match value {
        Value::String(value) => python_string_repr(value),
        other => python_argument_text(other),
    }
}

fn python_string_repr(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn strip_shell_quote_syntax(command: &str) -> String {
    let mut output = String::with_capacity(command.len());
    let mut quote = None;
    let mut escaped = false;
    for character in command.chars() {
        if escaped {
            output.push(character);
            escaped = false;
            continue;
        }
        if character == '\\' && quote != Some('\'') {
            escaped = true;
            continue;
        }
        if let Some(active_quote) = quote {
            if character == active_quote {
                quote = None;
            } else {
                output.push(character);
            }
            continue;
        }
        if matches!(character, '\'' | '"') {
            quote = Some(character);
        } else {
            output.push(character);
        }
    }
    if escaped {
        output.push('\\');
    }
    output
}

fn expand_shell_variable_literal_tokens(command: &str) -> String {
    static ASSIGNMENT: OnceLock<Option<Regex>> = OnceLock::new();
    static VARIABLE: OnceLock<Option<Regex>> = OnceLock::new();
    let Some(assignment) = ASSIGNMENT.get_or_init(|| {
        Regex::new(r"(?m)(^|[^A-Za-z0-9_.-])([A-Za-z_][A-Za-z0-9_]*)=([A-Za-z0-9_.-]+)").ok()
    }) else {
        return command.to_string();
    };
    let assignments = assignment
        .captures_iter(command)
        .filter_map(|captures| {
            Some((
                captures.get(2)?.as_str().to_string(),
                captures.get(3)?.as_str().to_string(),
            ))
        })
        .collect::<BTreeMap<_, _>>();
    if assignments.is_empty() {
        return command.to_string();
    }
    let Some(variable) = VARIABLE.get_or_init(|| {
        Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)").ok()
    }) else {
        return command.to_string();
    };
    let expanded = variable.replace_all(command, |captures: &regex::Captures<'_>| {
        let name = captures
            .get(1)
            .or_else(|| captures.get(2))
            .map(|capture| capture.as_str())
            .unwrap_or_default();
        assignments
            .get(name)
            .cloned()
            .unwrap_or_else(|| captures[0].to_string())
    });
    format!("{command}\n{expanded}")
}

fn shell_unspaced_token_view(command: &str) -> String {
    let mut output = Vec::new();
    for line in command.lines() {
        output.push(line.to_string());
        let compact = line
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect::<String>();
        if compact != line {
            output.push(compact);
        }
    }
    output.join("\n")
}

fn risky_recursive_remove_matches(call: &ToolCall) -> bool {
    if call.name != "run_bash" {
        return false;
    }
    let Some(command) = call.arguments.get("command") else {
        return false;
    };
    let stripped = strip_shell_quote_syntax(&python_argument_text(command));
    let expanded = expand_shell_variable_literal_tokens(&stripped);
    expanded
        .split(['\n', ';', '&', '|'])
        .any(command_segment_has_recursive_force_rm)
}

fn command_segment_has_recursive_force_rm(segment: &str) -> bool {
    let tokens = segment.split_whitespace().collect::<Vec<_>>();
    for (index, token) in tokens.iter().enumerate() {
        let executable = token.rsplit(['/', '\\']).next().unwrap_or_default();
        if !executable.eq_ignore_ascii_case("rm") {
            continue;
        }
        let mut recursive = false;
        let mut force = false;
        for option in tokens.iter().skip(index + 1) {
            if *option == "--" {
                break;
            }
            if !option.starts_with('-') || *option == "-" {
                continue;
            }
            if let Some(long) = option.strip_prefix("--") {
                let name = long.split('=').next().unwrap_or_default();
                recursive |= name.eq_ignore_ascii_case("recursive");
                force |= name.eq_ignore_ascii_case("force");
            } else {
                recursive |= option[1..]
                    .chars()
                    .any(|character| character.eq_ignore_ascii_case(&'r'));
                force |= option[1..]
                    .chars()
                    .any(|character| character.eq_ignore_ascii_case(&'f'));
            }
        }
        if recursive && force {
            return true;
        }
    }
    false
}

/// Match Python `fnmatchcase`'s `*`, `?`, `[seq]`, and `[!seq]` forms,
/// case-sensitively. An unmatched `[` remains a literal.
fn fnmatch_matches(pattern: &[GlobToken], candidate: &str) -> bool {
    let candidate = candidate.chars().collect::<Vec<_>>();
    let mut previous = vec![false; candidate.len() + 1];
    previous[0] = true;
    for token in pattern {
        let mut current = vec![false; candidate.len() + 1];
        if matches!(token, GlobToken::AnySequence) {
            current[0] = previous[0];
        }
        for index in 1..=candidate.len() {
            current[index] = match token {
                GlobToken::AnySequence => previous[index] || current[index - 1],
                GlobToken::AnyCharacter => previous[index - 1],
                GlobToken::CharacterClass { negated, items } => {
                    previous[index - 1]
                        && character_class_matches(items, candidate[index - 1]) != *negated
                }
                GlobToken::Literal(literal) => {
                    previous[index - 1] && *literal == candidate[index - 1]
                }
            };
        }
        previous = current;
    }
    previous[candidate.len()]
}

fn parse_fnmatch_pattern(pattern: &str) -> Vec<GlobToken> {
    let chars = pattern.chars().collect::<Vec<_>>();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < chars.len() {
        match chars[index] {
            '*' => {
                if !matches!(tokens.last(), Some(GlobToken::AnySequence)) {
                    tokens.push(GlobToken::AnySequence);
                }
                index += 1;
            }
            '?' => {
                tokens.push(GlobToken::AnyCharacter);
                index += 1;
            }
            '[' => {
                let Some((token, next)) = parse_character_class(&chars, index) else {
                    tokens.push(GlobToken::Literal('['));
                    index += 1;
                    continue;
                };
                tokens.push(token);
                index = next;
            }
            literal => {
                tokens.push(GlobToken::Literal(literal));
                index += 1;
            }
        }
    }
    tokens
}

fn parse_character_class(chars: &[char], start: usize) -> Option<(GlobToken, usize)> {
    let mut cursor = start.checked_add(1)?;
    let negated = chars.get(cursor) == Some(&'!');
    if negated {
        cursor += 1;
    }
    let content_start = cursor;
    if chars.get(cursor) == Some(&']') {
        cursor += 1;
    }
    while chars.get(cursor).is_some_and(|character| *character != ']') {
        cursor += 1;
    }
    if cursor >= chars.len() || cursor == content_start {
        return None;
    }
    let content = &chars[content_start..cursor];
    let mut items = Vec::new();
    let mut index = 0;
    while index < content.len() {
        if index + 2 < content.len() && content[index + 1] == '-' {
            let start = content[index];
            let end = content[index + 2];
            if start <= end {
                items.push(CharacterClassItem::Range(start, end));
            } else {
                items.extend([
                    CharacterClassItem::Literal(start),
                    CharacterClassItem::Literal('-'),
                    CharacterClassItem::Literal(end),
                ]);
            }
            index += 3;
        } else {
            items.push(CharacterClassItem::Literal(content[index]));
            index += 1;
        }
    }
    Some((GlobToken::CharacterClass { negated, items }, cursor + 1))
}

fn character_class_matches(items: &[CharacterClassItem], candidate: char) -> bool {
    items.iter().any(|item| match item {
        CharacterClassItem::Literal(literal) => *literal == candidate,
        CharacterClassItem::Range(start, end) => (*start..=*end).contains(&candidate),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn call(arguments: BTreeMap<String, Value>) -> ToolCall {
        ToolCall {
            id: "call-1".into(),
            name: "run_bash".into(),
            arguments,
        }
    }

    fn context() -> ToolContext {
        ToolContext::new(std::env::current_dir().unwrap()).unwrap()
    }

    #[test]
    fn later_matching_rule_overrides_broad_baseline() {
        let ask_push = PermissionRule::new("run_*", PermissionEffect::Ask)
            .unwrap()
            .with_argument_pattern("command", r"(?i)\bgit\s+push\b")
            .unwrap();
        let deny_force = PermissionRule::new("run_bash", PermissionEffect::Deny)
            .unwrap()
            .with_argument_pattern("command", r"--force")
            .unwrap();
        let policy = PermissionPolicy::allow_by_default()
            .with_rule(ask_push)
            .with_rule(deny_force);

        let safe = call(BTreeMap::from([(
            "command".into(),
            Value::String("cargo test".into()),
        )]));
        assert_eq!(
            policy.decide(&safe, &context()).effect,
            PermissionEffect::Allow
        );

        let push = call(BTreeMap::from([(
            "command".into(),
            Value::String("git push origin main".into()),
        )]));
        assert_eq!(
            policy.decide(&push, &context()).effect,
            PermissionEffect::Ask
        );

        let forced = call(BTreeMap::from([(
            "command".into(),
            Value::String("git push --force origin main".into()),
        )]));
        let decision = policy.decide(&forced, &context());
        assert_eq!(decision.effect, PermissionEffect::Deny);
        assert_eq!(decision.matched_rule_index, Some(1));
    }

    #[test]
    fn approval_hash_is_stable_across_argument_insertion_order() {
        let policy = PermissionPolicy::new(PermissionEffect::Ask);
        let first = call(BTreeMap::from([
            ("z".into(), Value::from(1)),
            ("a".into(), Value::String("雪".into())),
        ]));
        let mut second_args = BTreeMap::new();
        second_args.insert("a".into(), Value::String("雪".into()));
        second_args.insert("z".into(), Value::from(1));
        let second = call(second_args);

        let first_plan = policy.decide(&first, &context()).approval.unwrap();
        let second_plan = policy.decide(&second, &context()).approval.unwrap();
        assert_eq!(
            first_plan.canonical_plan_hash,
            second_plan.canonical_plan_hash
        );
        assert_eq!(first_plan.canonical_plan_hash.len(), 64);
        let mut different_call = first.clone();
        different_call.id = "call-2".into();
        let different_binding = policy.decide(&different_call, &context()).approval.unwrap();
        assert_eq!(
            first_plan.canonical_plan_hash,
            different_binding.canonical_plan_hash
        );
        assert_ne!(
            first_plan.approval_binding_hash,
            different_binding.approval_binding_hash
        );

        let elsewhere_dir = tempfile::tempdir().unwrap();
        let elsewhere = ToolContext::new(elsewhere_dir.path()).unwrap();
        let other_plan = policy.decide(&first, &elsewhere).approval.unwrap();
        assert_ne!(
            first_plan.canonical_plan_hash,
            other_plan.canonical_plan_hash
        );
    }

    #[test]
    fn approval_hash_matches_the_python_canonical_json_contract() {
        let canonical = canonical_plan_json(
            "run_bash",
            &BTreeMap::from([("command".into(), Value::String("git push".into()))]),
            "/workspace",
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(canonical.clone()).unwrap(),
            r#"{"arguments":{"command":"git push"},"cwd":"/workspace","tool":"run_bash"}"#
        );
        assert_eq!(
            approval_plan_hash(&canonical),
            "fae3d894460fc8165be733ef27950737568eb63e93e5b31a2a2085696b016769"
        );
    }

    #[test]
    fn invalid_argument_regex_is_rejected_when_rule_is_built() {
        assert!(matches!(
            PermissionRule::new("*", PermissionEffect::Deny)
                .unwrap()
                .with_argument_pattern("path", "("),
            Err(PermissionRuleError::InvalidArgumentRegex { .. })
        ));
    }

    #[test]
    fn wildcard_matching_is_case_sensitive_and_supports_question_mark() {
        assert!(fnmatch_matches(
            &parse_fnmatch_pattern("write_*"),
            "write_file"
        ));
        assert!(fnmatch_matches(&parse_fnmatch_pattern("tool_?"), "tool_x"));
        assert!(!fnmatch_matches(
            &parse_fnmatch_pattern("WRITE_*"),
            "write_file"
        ));
        assert!(!fnmatch_matches(
            &parse_fnmatch_pattern("tool_?"),
            "tool_xy"
        ));
    }

    #[test]
    fn fnmatch_character_classes_match_python_policy_patterns() {
        assert!(fnmatch_matches(
            &parse_fnmatch_pattern("run_[ab]*"),
            "run_admin"
        ));
        assert!(!fnmatch_matches(
            &parse_fnmatch_pattern("run_[!a]*"),
            "run_admin"
        ));
        assert!(fnmatch_matches(
            &parse_fnmatch_pattern("run_[!a]*"),
            "run_bash"
        ));
        assert!(fnmatch_matches(
            &parse_fnmatch_pattern("literal[[]name"),
            "literal[name"
        ));
        assert!(fnmatch_matches(
            &parse_fnmatch_pattern("unmatched["),
            "unmatched["
        ));
    }

    #[test]
    fn argument_matching_uses_python_text_and_shell_normalization() {
        assert_eq!(python_argument_text(&Value::from(1e-7)), "1e-07");
        let boolean = PermissionPolicy::allow_by_default().with_rule(
            PermissionRule::new("set_flag", PermissionEffect::Deny)
                .unwrap()
                .with_argument_pattern("enabled", r"^True$")
                .unwrap(),
        );
        let mut boolean_call = call(BTreeMap::from([("enabled".into(), Value::Bool(true))]));
        boolean_call.name = "set_flag".into();
        assert_eq!(
            boolean.decide(&boolean_call, &context()).effect,
            PermissionEffect::Deny
        );

        let protected = PermissionPolicy::allow_by_default().with_rule(
            PermissionRule::new("run_bash", PermissionEffect::Deny)
                .unwrap()
                .with_argument_pattern("command", r"(?i)\.env|--force")
                .unwrap(),
        );
        for command in ["x=env; cat .$x", "git push --fo''rce origin main"] {
            assert_eq!(
                protected
                    .decide(
                        &call(BTreeMap::from([(
                            "command".into(),
                            Value::String(command.into()),
                        )])),
                        &context(),
                    )
                    .effect,
                PermissionEffect::Deny
            );
        }
    }

    #[test]
    fn unsupported_float_approval_plan_fails_closed() {
        let policy = PermissionPolicy::new(PermissionEffect::Ask);
        let decision = policy.decide(
            &call(BTreeMap::from([("threshold".into(), Value::from(1e-7))])),
            &context(),
        );
        assert_eq!(decision.effect, PermissionEffect::Ask);
        assert!(decision.approval.is_none());
    }

    #[test]
    fn protected_path_floor_cannot_be_reopened_by_later_allow() {
        let mut policy = protected_paths_policy().unwrap();
        policy.push_rule(PermissionRule::new("*", PermissionEffect::Allow).unwrap());
        for (tool, argument, value) in [
            ("write_file", "path", ".git/hooks/pre-commit"),
            ("write_file", "path", "Secret.PROD.env"),
            ("run_bash", "command", "cat .git; true"),
            ("run_bash", "command", "cp -r .ssh backup"),
            ("run_bash", "command", "(cat .git)"),
            ("run_bash", "command", "cat $(find .ssh)"),
            (
                "run_bash",
                "command",
                "GIT_DIR=.git git config core.hooksPath hooks",
            ),
            (
                "run_bash",
                "command",
                "git --git-dir .git config core.hooksPath hooks",
            ),
            ("run_bash", "command", "x=env; cat .$x"),
            ("run_bash", "command", "name=git; cat .$name/config"),
            ("run_bash", "command", "dir=ssh; cat .${dir}/id_rsa"),
        ] {
            let mut invocation = call(BTreeMap::from([(
                argument.into(),
                Value::String(value.into()),
            )]));
            invocation.name = tool.into();
            let decision = policy.decide(&invocation, &context());
            assert_eq!(decision.effect, PermissionEffect::Deny, "{value}");
            assert!(decision.matched_floor);
        }
        let mut safe = call(BTreeMap::from([(
            "path".into(),
            Value::String("src/lib.rs".into()),
        )]));
        safe.name = "write_file".into();
        assert_eq!(
            policy.decide(&safe, &context()).effect,
            PermissionEffect::Allow
        );
        let mut similarly_named = call(BTreeMap::from([(
            "path".into(),
            Value::String(".gitfoo/config".into()),
        )]));
        similarly_named.name = "write_file".into();
        assert_eq!(
            policy.decide(&similarly_named, &context()).effect,
            PermissionEffect::Allow
        );

        for command in [
            "git status",
            "git --git-dir .g?t config core.hooksPath hooks",
            "cat .e?v",
            "cp -r .s?h backup",
            "git --git-dir .g{it,xx} config core.hooksPath hooks",
            "python -c 'open(chr(46)+\"git/config\", \"w\")'",
        ] {
            let mut shell = call(BTreeMap::from([(
                "command".into(),
                Value::String(command.into()),
            )]));
            shell.name = "run_bash".into();
            let decision = policy.decide(&shell, &context());
            assert_eq!(decision.effect, PermissionEffect::Deny, "{command}");
            assert!(decision.matched_floor, "{command}");
        }
    }

    #[test]
    fn risky_operations_request_approval_after_shell_normalization() {
        let policy = risky_operations_policy().unwrap();
        for command in [
            "git push --force origin main",
            "git push -uf origin main",
            "git push origin +main",
            "git push --fo''rce origin main",
            "git -c alias.p='push --force' p origin main",
            "git push $(printf %s --fo)$(printf %s rce) origin main",
            "git reset --hard HEAD~1",
            "git clean -fd build",
            "git clean -f -d build",
            "rm -fr build",
            "rm --recursive --force build",
            "rm -v -r -f build",
            "rm --verbose --recursive --force build",
            "rm --recursive --verbose --force build",
            "rm harmless -r -f build",
            "rm -r harmless -f build",
            "rm -r''f build",
            "rm -$(printf %s rf) build",
            "chmod -Rv 700 scripts",
            "chown --recursive user .",
            "mkfs.ext4 /dev/x",
            "dd bs=1m if=/dev/zero of=disk.img",
        ] {
            assert_eq!(
                policy
                    .decide(
                        &call(BTreeMap::from([(
                            "command".into(),
                            Value::String(command.into()),
                        )])),
                        &context(),
                    )
                    .effect,
                PermissionEffect::Ask,
                "{command}"
            );
        }
        for command in [
            "git status",
            "git clean -n docs",
            "rm -i foo bar",
            "rm -- -rf",
        ] {
            assert_eq!(
                policy
                    .decide(
                        &call(BTreeMap::from([(
                            "command".into(),
                            Value::String(command.into()),
                        )])),
                        &context(),
                    )
                    .effect,
                PermissionEffect::Allow,
                "{command}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_cwd_cannot_produce_an_ambiguous_approval_hash() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt as _;

        let directory = tempfile::tempdir().unwrap();
        let non_utf8 = directory
            .path()
            .join(OsString::from_vec(b"dir-\x80".to_vec()));
        std::fs::create_dir(&non_utf8).unwrap();
        let context = ToolContext::new(non_utf8).unwrap();
        let decision =
            PermissionPolicy::new(PermissionEffect::Ask).decide(&call(BTreeMap::new()), &context);
        assert_eq!(decision.effect, PermissionEffect::Ask);
        assert!(decision.approval.is_none());
    }
}
