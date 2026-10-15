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

#[test]
fn source_materialization_rejects_diagnostic_metadata_amplification() {
    for metadata in [
        format!("needs=[{}]", vec!["'missing'"; 1024].join(",")),
        format!("[step.route]\ntags=[{}]", vec!["''"; 256].join(",")),
    ] {
        let bundle = WorkflowSourceBundle {
            workflow_toml: format!(
                "[workflow]\nname='bounded'\n[[step]]\nid='{}'\ntarget='auto'\nprompt='x'\n{metadata}\n",
                "x".repeat(16 * 1024)
            ),
            prompt_files: vec![],
        };
        let error = bundle
            .materialize()
            .expect_err("reject before graph/route diagnostics");
        assert!(matches!(error, WorkflowError::InvalidWorkflowPlan { .. }));
        assert!(error.to_string().len() < 256, "fixed bounded error");
    }
}

#[test]
fn source_materialization_preserves_per_step_and_global_metadata_limits() {
    fn bundle(metadata: &str, steps: usize) -> WorkflowSourceBundle {
        let mut source = String::from("[workflow]\nname='counts'\n");
        for index in 0..steps {
            source.push_str(&format!(
                "[[step]]\nid='s{index}'\nprompt='x'\n{metadata}\n"
            ));
        }
        WorkflowSourceBundle {
            workflow_toml: source,
            prompt_files: vec![],
        }
    }
    for (field, max, total_steps) in [
        ("needs", 1024, 16),
        ("fan_out", 256, 64),
        ("route.tags", 256, 64),
        ("route.candidates", 256, 64),
    ] {
        let at_limit = format!("{field}=[{}]", vec!["'x'"; max].join(","));
        bundle(&at_limit, 1).materialize().expect("per-step limit");
        bundle(&at_limit, total_steps)
            .materialize()
            .expect("global limit");
        let excess = format!("{field}=[{}]", vec!["'x'"; max + 1].join(","));
        assert!(
            bundle(&excess, 1).materialize().is_err(),
            "{field} per-step excess"
        );
        assert!(
            bundle(&at_limit, total_steps + 1).materialize().is_err(),
            "{field} global excess"
        );
    }
    // stage and tier count toward the same global route budget as list values.
    let route = format!(
        "route.tags=[{}]\nroute.stage='s'\nroute.tier='t'",
        vec!["'x'"; 256].join(",")
    );
    assert!(bundle(&route, 64).materialize().is_err());
    // Target cardinality includes both fan-out and explicit selectors even if
    // a later semantic validation would reject using both together.
    let targets = format!("fan_out=[{}]\ntarget='x'", vec!["'x'"; 256].join(","));
    assert!(bundle(&targets, 64).materialize().is_err());
    bundle("needs=[]", 1)
        .materialize()
        .expect("empty dependency list");
}
