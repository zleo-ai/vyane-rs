//! Built-in review pipeline: a multi-model review workflow (implement → fan-out
//! review → synthesize) that runs on top of the existing `vyane-workflow` engine
//! without requiring the user to write a TOML file.
//!
//! The pipeline builds three [`WorkflowStep`]s programmatically and runs them
//! through the same [`WorkflowEngine`] the `vyane workflow run` command uses,
//! so journal/resume/observer semantics are identical.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use vyane_core::Sandbox;
use vyane_workflow::{
    OnError, StepTargets, TargetResolver, Workflow, WorkflowEngine, WorkflowOutcome, WorkflowStep,
};

/// Parameters for the `vyane review` command.
#[derive(Debug, Clone)]
pub struct ReviewArgs {
    pub task: String,
    pub implementer: String,
    /// Comma-separated reviewer targets (e.g. "opus,gpt,sonnet").
    pub reviewers: Vec<String>,
    pub synthesizer: String,
    pub workdir: Option<PathBuf>,
    pub timeout_secs: Option<u64>,
}

impl ReviewArgs {
    /// Parse the `--reviewers` comma-separated string into a Vec.
    pub fn parse_reviewers(raw: &str) -> Vec<String> {
        raw.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }
}

/// Build the three-step review workflow from the args.
pub fn build_review_workflow(args: &ReviewArgs) -> Workflow {
    let timeout = args.timeout_secs.map(Duration::from_secs);
    let reviewers = args.reviewers.clone();
    let workdir = args.workdir.clone();

    let steps = vec![
        // Step 1: implement
        WorkflowStep {
            index: 0,
            id: "implement".into(),
            needs: vec![],
            targets: StepTargets::Single(args.implementer.clone()),
            prompt: Some("{{ vars.task }}".into()),
            prompt_file: None,
            prompt_template: Some("{{ vars.task }}".into()),
            system: None,
            workdir: workdir.clone(),
            sandbox: Sandbox::ReadOnly,
            timeout,
            on_error: OnError::Abort,
            route: Default::default(),
        },
        // Step 2: fan out to independent reviewers
        WorkflowStep {
            index: 1,
            id: "review".into(),
            needs: vec!["implement".into()],
            targets: StepTargets::FanOut(reviewers),
            prompt: Some(
                "Review this implementation for correctness, security, and quality.\n\n\
                 {{ steps.implement.output }}\n\n\
                 Report findings by severity (BLOCKER / MAJOR / MINOR) with specific \
                 file:line references."
                    .into(),
            ),
            prompt_file: None,
            prompt_template: Some(
                "Review this implementation for correctness, security, and quality.\n\n\
                 {{ steps.implement.output }}\n\n\
                 Report findings by severity (BLOCKER / MAJOR / MINOR) with specific \
                 file:line references."
                    .into(),
            ),
            system: Some(
                "You are an independent code reviewer. Be thorough and specific. \
                 Report BLOCKER, MAJOR, and MINOR findings."
                    .into(),
            ),
            workdir: workdir.clone(),
            sandbox: Sandbox::ReadOnly,
            timeout,
            on_error: OnError::Continue,
            route: Default::default(),
        },
        // Step 3: synthesize the reviews
        WorkflowStep {
            index: 2,
            id: "synthesize".into(),
            needs: vec!["review".into()],
            targets: StepTargets::Single(args.synthesizer.clone()),
            prompt: Some(
                "Synthesize these independent code reviews into a single verdict.\n\n\
                 Implementation:\n\
                 {{ steps.implement.output }}\n\n\
                 Reviews from independent reviewers:\n\
                 {{ steps.review.outputs }}\n\n\
                 Produce a final verdict: APPROVE or REQUEST_CHANGES, with a summary of \
                 the most important findings."
                    .into(),
            ),
            prompt_file: None,
            prompt_template: Some(
                "Synthesize these independent code reviews into a single verdict.\n\n\
                 Implementation:\n\
                 {{ steps.implement.output }}\n\n\
                 Reviews from independent reviewers:\n\
                 {{ steps.review.outputs }}\n\n\
                 Produce a final verdict: APPROVE or REQUEST_CHANGES, with a summary of \
                 the most important findings."
                    .into(),
            ),
            system: Some(
                "You are a review synthesizer. Consolidate multiple independent reviews \
                 into one actionable verdict."
                    .into(),
            ),
            workdir,
            sandbox: Sandbox::ReadOnly,
            timeout,
            on_error: OnError::Abort,
            route: Default::default(),
        },
    ];

    Workflow {
        name: "review-pipeline".into(),
        description: Some("Multi-model review: implement → fan-out review → synthesize".into()),
        max_concurrency: 4,
        steps,
        file_path: PathBuf::from("(builtin:review)"),
        legacy_file_sha256: None,
        file_sha256: String::new(),
    }
}

/// Run the review pipeline and return the outcome.
pub async fn run_review(
    args: ReviewArgs,
    dispatcher: Arc<vyane_kernel::Dispatcher>,
    resolver: Arc<dyn TargetResolver>,
    journal_dir: PathBuf,
    cancel: vyane_core::CancellationToken,
) -> Result<WorkflowOutcome> {
    let wf = build_review_workflow(&args);
    let mut vars = BTreeMap::new();
    vars.insert("task".into(), args.task);

    let engine = WorkflowEngine::new(dispatcher, resolver, journal_dir);
    let outcome = engine.run(&wf, vars, cancel).await?;
    Ok(outcome)
}
