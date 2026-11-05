use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use vyane_core::{
    AdapterTransport, BoundTarget, Endpoint, GenParams, ModelId, Protocol, ProviderId, Target,
};
use vyane_workflow::{
    JournalStep, JournalStepStatus, OnError, TargetResolver, Workflow, WorkflowError, WorkflowPlan,
    WorkflowPlanFeature, WorkflowPlanFeatureSupport, validate_workflow,
};

pub(super) fn replay(input: &Value, _temp: &Path) -> Value {
    match input["op"].as_str().expect("workflow op") {
        "feature" => feature(input["feature"].as_str().expect("feature")),
        "source" => source(input["source"].as_str().expect("source")),
        "reset" => reset_status(),
        "on_error" => json!({
            "abort": OnError::Abort.as_str(),
            "continue": OnError::Continue.as_str(),
        }),
        other => panic!("unknown workflow op {other}"),
    }
}

fn feature(name: &str) -> Value {
    let feature = match name {
        "declarative_dag" => WorkflowPlanFeature::DeclarativeDag,
        "worktree_isolation" => WorkflowPlanFeature::WorktreeIsolation,
        "dynamic_control_flow" => WorkflowPlanFeature::DynamicControlFlow,
        "nested_workflow" => WorkflowPlanFeature::NestedWorkflow,
        "shared_budget" => WorkflowPlanFeature::SharedBudget,
        other => panic!("unknown feature {other}"),
    };
    match WorkflowPlan::feature_support(feature) {
        WorkflowPlanFeatureSupport::Supported => json!({ "status": "supported" }),
        WorkflowPlanFeatureSupport::Unsupported { reason, .. } => {
            json!({ "reason": reason, "status": "unsupported" })
        }
        other => panic!("unpinned feature support {other:?}"),
    }
}

fn source(name: &str) -> Value {
    let toml = match name {
        "javascript" => "export function run() { return 1 }\n",
        "ignored_script" => {
            "[workflow]\nname = \"demo\"\n\n[[step]]\nid = \"one\"\ntarget = \"relay/model-a\"\nprompt = \"ping\"\nscript = \"print(1)\"\n"
        }
        "cycle" => {
            "[workflow]\nname = \"demo\"\n\n[[step]]\nid = \"a\"\nneeds = [\"b\"]\ntarget = \"relay/model-a\"\nprompt = \"ping\"\n\n[[step]]\nid = \"b\"\nneeds = [\"a\"]\ntarget = \"relay/model-a\"\nprompt = \"ping\"\n"
        }
        "duplicate" => {
            "[workflow]\nname = \"demo\"\n\n[[step]]\nid = \"one\"\ntarget = \"relay/model-a\"\nprompt = \"ping\"\n\n[[step]]\nid = \"one\"\ntarget = \"relay/model-a\"\nprompt = \"ping\"\n"
        }
        "both_prompts" => {
            "[workflow]\nname = \"demo\"\n\n[[step]]\nid = \"one\"\ntarget = \"relay/model-a\"\nprompt = \"ping\"\nprompt_file = \"note.txt\"\n"
        }
        "blank_name" => {
            "[workflow]\nname = \"  \"\n\n[[step]]\nid = \"one\"\ntarget = \"relay/model-a\"\nprompt = \"ping\"\n"
        }
        "both_targets" => {
            "[workflow]\nname = \"demo\"\n\n[[step]]\nid = \"one\"\ntarget = \"relay/model-a\"\nfan_out = [\"relay/model-a\"]\nprompt = \"ping\"\n"
        }
        "topo" => {
            "[workflow]\nname = \"demo\"\n\n[[step]]\nid = \"b\"\nneeds = [\"a\"]\ntarget = \"relay/model-a\"\nprompt = \"ping\"\n\n[[step]]\nid = \"a\"\ntarget = \"relay/model-a\"\nprompt = \"ping\"\n"
        }
        other => panic!("unknown workflow source {other}"),
    };
    let workflow = match Workflow::from_bytes(PathBuf::from("workflow.toml"), toml.as_bytes()) {
        Ok(workflow) => workflow,
        Err(error) => return error_value(&error),
    };
    match validate_workflow(&workflow, &BTreeMap::new(), &OfflineResolver) {
        Ok(validated) => json!({
            "class": "ok",
            "problems": [],
            "topo": validated.topo_order,
        }),
        Err(error) => error_value(&error),
    }
}

fn error_value(error: &WorkflowError) -> Value {
    match error {
        WorkflowError::Validation(report) => json!({
            "class": error.as_str(),
            "problems": report.problems,
        }),
        other => json!({ "class": other.as_str() }),
    }
}

fn reset_status() -> Value {
    json!({
        "failed": reset(JournalStepStatus::Failed),
        "success": reset(JournalStepStatus::Success),
    })
}

fn reset(status: JournalStepStatus) -> &'static str {
    let mut step = JournalStep {
        status,
        run_ids: Vec::new(),
        output: None,
        outputs: None,
        error: None,
        worktrees: Vec::new(),
    };
    step.reset_for_rerun();
    step.status.as_str()
}

struct OfflineResolver;

impl TargetResolver for OfflineResolver {
    fn resolve(&self, target: &str) -> vyane_core::Result<Vec<BoundTarget>> {
        let (provider, model) = target.split_once('/').unwrap_or(("relay", "model-a"));
        Ok(vec![BoundTarget {
            target: Target {
                provider: ProviderId::new(provider),
                protocol: Protocol::OpenaiChat,
                harness: None,
                model: ModelId::new(model),
            },
            transport: AdapterTransport::DirectHttp,
            endpoint: Some(Endpoint {
                base_url: "https://offline.example.invalid".to_string(),
                auth: None,
            }),
            params: GenParams::default(),
        }])
    }
}
