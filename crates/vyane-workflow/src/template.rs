use std::collections::{BTreeMap, BTreeSet};

use crate::error::{WorkflowError, WorkflowResult};
use crate::journal::JournalStep;

#[derive(Debug, Clone)]
pub struct RenderedStepOutputs {
    pub output: Option<String>,
    pub outputs: Option<Vec<(String, String)>>,
}

pub(crate) fn validate_template(
    owner_step: &str,
    template: &str,
    workflow_name: &str,
    vars: &BTreeMap<String, String>,
    available_steps: &BTreeSet<String>,
    step_is_fan_out: &dyn Fn(&str) -> Option<bool>,
) -> Vec<String> {
    let mut problems = Vec::new();
    let placeholders = match placeholders(template) {
        Ok(placeholders) => placeholders,
        Err(mut errors) => {
            problems.append(&mut errors);
            Vec::new()
        }
    };
    for placeholder in placeholders {
        match parse_placeholder(&placeholder) {
            Placeholder::WorkflowName => {
                if workflow_name.is_empty() {
                    problems.push(format!(
                        "step `{owner_step}` references {{{{workflow.name}}}} but [workflow].name is missing"
                    ));
                }
            }
            Placeholder::Var(key) => {
                if !vars.contains_key(key) {
                    problems.push(format!(
                        "step `{owner_step}` references unknown variable `{key}`"
                    ));
                }
            }
            Placeholder::StepOutput { step, fan_out } => {
                if !available_steps.contains(step) {
                    problems.push(format!(
                        "step `{owner_step}` references `{step}` but it is not in its transitive needs"
                    ));
                    continue;
                }
                match step_is_fan_out(step) {
                    Some(true) if !fan_out => problems.push(format!(
                        "step `{owner_step}` references fan_out step `{step}` with `.output`; use `.outputs`"
                    )),
                    Some(false) if fan_out => problems.push(format!(
                        "step `{owner_step}` references single-target step `{step}` with `.outputs`; use `.output`"
                    )),
                    Some(_) => {}
                    None => problems.push(format!(
                        "step `{owner_step}` references unknown step `{step}`"
                    )),
                }
            }
            Placeholder::Unknown(raw) => problems.push(format!(
                "step `{owner_step}` uses unknown placeholder `{{{{{raw}}}}}`"
            )),
        }
    }
    problems
}

pub fn render_template(
    template: &str,
    workflow_name: &str,
    vars: &BTreeMap<String, String>,
    steps: &BTreeMap<String, JournalStep>,
) -> WorkflowResult<String> {
    render_template_inner(template, workflow_name, vars, steps)
}

pub(crate) fn render_template_inner(
    template: &str,
    workflow_name: &str,
    vars: &BTreeMap<String, String>,
    steps: &BTreeMap<String, JournalStep>,
) -> WorkflowResult<String> {
    let mut out = String::new();
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        rest = &rest[start..];
        if rest.starts_with("{{{{") {
            out.push_str("{{");
            rest = &rest[4..];
            continue;
        }
        let Some(end) = rest[2..].find("}}") else {
            return Err(WorkflowError::validation(vec![
                "template has an unclosed placeholder".to_string(),
            ]));
        };
        let raw = rest[2..2 + end].trim();
        let replacement = match parse_placeholder(raw) {
            Placeholder::WorkflowName => workflow_name.to_string(),
            Placeholder::Var(key) => vars.get(key).cloned().ok_or_else(|| {
                WorkflowError::validation(vec![format!("unknown workflow variable `{key}`")])
            })?,
            Placeholder::StepOutput { step, fan_out } => {
                let step_state = steps.get(step).ok_or_else(|| {
                    WorkflowError::validation(vec![format!("unknown workflow step `{step}`")])
                })?;
                if fan_out {
                    render_fan_out(step_state)
                } else {
                    step_state.output.clone().ok_or_else(|| {
                        WorkflowError::validation(vec![format!(
                            "workflow step `{step}` has no recorded output"
                        )])
                    })?
                }
            }
            Placeholder::Unknown(raw) => {
                return Err(WorkflowError::validation(vec![format!(
                    "unknown placeholder `{{{{{raw}}}}}`"
                )]));
            }
        };
        out.push_str(&replacement);
        rest = &rest[2 + end + 2..];
    }
    out.push_str(rest);
    Ok(out)
}

fn render_fan_out(step: &JournalStep) -> String {
    let mut out = String::new();
    if let Some(outputs) = step.outputs.as_ref() {
        for item in outputs.iter().filter(|item| item.ok) {
            if let Some(text) = item.output.as_deref() {
                out.push_str("## ");
                out.push_str(&item.target);
                out.push('\n');
                out.push_str(text);
                if !text.ends_with('\n') {
                    out.push('\n');
                }
            }
        }
    }
    out
}

fn placeholders(template: &str) -> Result<Vec<String>, Vec<String>> {
    let mut out = Vec::new();
    let mut errors = Vec::new();
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        rest = &rest[start..];
        if rest.starts_with("{{{{") {
            rest = &rest[4..];
            continue;
        }
        let Some(end) = rest[2..].find("}}") else {
            errors.push("template has an unclosed placeholder".to_string());
            break;
        };
        out.push(rest[2..2 + end].trim().to_string());
        rest = &rest[2 + end + 2..];
    }
    if errors.is_empty() {
        Ok(out)
    } else {
        Err(errors)
    }
}

enum Placeholder<'a> {
    WorkflowName,
    Var(&'a str),
    StepOutput { step: &'a str, fan_out: bool },
    Unknown(&'a str),
}

fn parse_placeholder(raw: &str) -> Placeholder<'_> {
    if raw == "workflow.name" {
        return Placeholder::WorkflowName;
    }
    if let Some(key) = raw.strip_prefix("vars.") {
        if !key.is_empty() {
            return Placeholder::Var(key);
        }
    }
    if let Some(rest) = raw.strip_prefix("steps.") {
        if let Some(step) = rest.strip_suffix(".outputs") {
            if !step.is_empty() {
                return Placeholder::StepOutput {
                    step,
                    fan_out: true,
                };
            }
        }
        if let Some(step) = rest.strip_suffix(".output") {
            if !step.is_empty() {
                return Placeholder::StepOutput {
                    step,
                    fan_out: false,
                };
            }
        }
    }
    Placeholder::Unknown(raw)
}
