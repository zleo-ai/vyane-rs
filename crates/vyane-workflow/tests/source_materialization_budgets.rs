use vyane_workflow::{
    WORKFLOW_PLAN_MAX_STEPS, WORKFLOW_SOURCE_MAX_PROMPT_BYTES, WorkflowError, WorkflowSourceBundle,
    WorkflowSourceEntry,
};

fn repeated_prompt_bundle(steps: usize, prompt_bytes: usize) -> WorkflowSourceBundle {
    let mut workflow_toml = String::from("[workflow]\nname='repeated-prompt'\n");
    for index in 0..steps {
        workflow_toml.push_str(&format!(
            "[[step]]\nid='step-{index}'\ntarget='unconfigured'\nprompt_file='prompt.txt'\n"
        ));
    }
    WorkflowSourceBundle {
        workflow_toml,
        prompt_files: vec![WorkflowSourceEntry {
            path: "prompt.txt".parse().expect("canonical path"),
            content: "x".repeat(prompt_bytes),
        }],
    }
}

#[test]
fn source_materialization_bounds_repeated_prompt_expansion() {
    // Only one 4 MiB input file, but five references would allocate 20 MiB
    // before compile_plan could apply its existing total-content budget.
    let rejected = repeated_prompt_bundle(5, WORKFLOW_SOURCE_MAX_PROMPT_BYTES);
    assert!(matches!(
        rejected.materialize(),
        Err(WorkflowError::InvalidWorkflowPlan { .. })
    ));
    // Exactly 16 MiB of materialized prompts is allowed at this stage. The
    // compiled plan still checks its total strings, including all metadata.
    let accepted = repeated_prompt_bundle(4, WORKFLOW_SOURCE_MAX_PROMPT_BYTES);
    let workflow = accepted
        .materialize()
        .expect("within materialization budget");
    assert_eq!(workflow.steps.len(), 4);
    assert!(
        workflow
            .steps
            .iter()
            .all(|s| s.prompt_template.as_ref().expect("prompt").len()
                == WORKFLOW_SOURCE_MAX_PROMPT_BYTES)
    );
}

#[test]
fn source_materialization_bounds_step_count_before_building_steps() {
    let rejected = repeated_prompt_bundle(WORKFLOW_PLAN_MAX_STEPS + 1, 1);
    assert!(matches!(
        rejected.materialize(),
        Err(WorkflowError::InvalidWorkflowPlan { .. })
    ));
    assert_eq!(
        repeated_prompt_bundle(WORKFLOW_PLAN_MAX_STEPS, 1)
            .materialize()
            .expect("maximum steps")
            .steps
            .len(),
        WORKFLOW_PLAN_MAX_STEPS
    );
}
