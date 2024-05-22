/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::FalkorValue;

/// An execution plan, storing both the specific string details for each step, and also a formatted plaintext string for pretty display
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    text: String,
    steps: Vec<String>,
}

impl ExecutionPlan {
    /// Returns a pretty-print version of the execution plan
    pub fn text(&self) -> &str {
        self.text.as_str()
    }

    /// Returns a slice of strings representing each step in the execution plan, which can be iterated.
    pub fn steps(&self) -> &[String] {
        self.steps.as_slice()
    }
}

impl TryFrom<FalkorValue> for ExecutionPlan {
    type Error = anyhow::Error;

    fn try_from(value: FalkorValue) -> Result<Self, Self::Error> {
        let string_vec = value.into_vec()?;

        let (mut execution_plan, mut execution_plan_text) = (
            Vec::with_capacity(string_vec.len()),
            Vec::with_capacity(string_vec.len()),
        );
        execution_plan_text.push(String::new());
        for item in string_vec {
            let raw_text = item.into_string()?;
            execution_plan.push(raw_text.trim().to_string());
            execution_plan_text.push(raw_text);
        }

        Ok(ExecutionPlan {
            steps: execution_plan,
            text: execution_plan_text.join("\n"),
        })
    }
}
