/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{FalkorDBError, FalkorValue};

/// An execution plan, storing both the specific string details for each step, and also a formatted plaintext string for pretty display
#[derive(Debug, Clone, PartialEq)]
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
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> Result<Self, Self::Error> {
        let (execution_plan, execution_plan_text): (Vec<_>, Vec<_>) = value
            .into_vec()?
            .into_iter()
            .flat_map(|item| {
                let item = item.into_string()?;
                Result::<_, FalkorDBError>::Ok((item.trim().to_string(), item))
            })
            .unzip();

        Ok(ExecutionPlan {
            steps: execution_plan,
            text: format!("\n{}", execution_plan_text.join("\n")),
        })
    }
}
