/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use crate::{
    parser::{redis_value_as_string, redis_value_as_vec},
    FalkorDBError, FalkorResult,
};
use regex::Regex;
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    ops::Not,
    rc::Rc,
    sync::{Arc, LazyLock},
};

#[derive(Debug)]
struct IntermediateOperation {
    name: String,
    args: Option<Vec<String>>,
    records_produced: Option<i64>,
    execution_time: Option<f64>,
    depth: usize,
    children: Vec<Rc<RefCell<IntermediateOperation>>>,
}

impl IntermediateOperation {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Create New Operation", skip_all, level = "trace")
    )]
    fn new(
        depth: usize,
        operation_string: &str,
    ) -> FalkorResult<IntermediateOperation> {
        static RECORDS_PRODUCED: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r"Records produced: (\d+)")
                .expect("the records-produced regex is a valid, fixed pattern")
        });
        // The server prints the time with six decimals; accept integers and exponents as well.
        static EXECUTION_TIME: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r"Execution time: (\d+(?:\.\d+)?(?:[eE][+-]?\d+)?) ms")
                .expect("the execution-time regex is a valid, fixed pattern")
        });

        let mut args = operation_string.split('|').collect::<VecDeque<_>>();
        let name = args
            .pop_front()
            .ok_or(FalkorDBError::CorruptExecutionPlan)?
            .trim();

        let (records_produced, execution_time) = match args.pop_back() {
            Some(last_arg) if last_arg.contains("Records produced") => (
                RECORDS_PRODUCED
                    .captures(last_arg)
                    .and_then(|cap| cap.get(1))
                    .and_then(|m| m.as_str().parse().ok()),
                EXECUTION_TIME
                    .captures(last_arg)
                    .and_then(|cap| cap.get(1))
                    .and_then(|m| m.as_str().parse().ok()),
            ),
            Some(last_arg) => {
                args.push_back(last_arg);
                (None, None)
            }
            None => (None, None),
        };

        Ok(Self {
            name: name.to_string(),
            args: args
                .is_empty()
                .not()
                .then(|| args.into_iter().map(ToString::to_string).collect()),
            records_produced,
            execution_time,
            depth,
            children: vec![],
        })
    }
}

/// A graph operation, with its statistics if available, and pointers to its child operations
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Operation {
    /// The operation name, or string representation
    pub name: String,
    /// All arguments following
    pub args: Option<Vec<String>>,
    /// The amount of records produced by this specific operation(regardless of later filtering), if any
    pub records_produced: Option<i64>,
    /// The time it took to execute this operation, if available
    pub execution_time: Option<f64>,
    /// all child operations performed on data retrieved, filtered or aggregated by this operation
    pub children: Vec<Arc<Operation>>,
    depth: usize,
}

/// An execution plan, allowing access both to the human-readable text representation, access to a per-operation map, or traversable operation tree
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionPlan {
    string_representation: String,
    plan: Vec<String>,
    operations: HashMap<String, Vec<Arc<Operation>>>,
    operation_tree: Arc<Operation>,
}

impl ExecutionPlan {
    /// Returns the plan as a slice of human-readable strings
    pub fn plan(&self) -> &[String] {
        self.plan.as_slice()
    }

    /// Returns a slice of strings representing each step in the execution plan, which can be iterated.
    pub fn operations(&self) -> &HashMap<String, Vec<Arc<Operation>>> {
        &self.operations
    }

    /// Returns a shared pointer to the operation tree, allowing easy immutable traversal
    pub fn operation_tree(&self) -> &Arc<Operation> {
        &self.operation_tree
    }

    /// Returns a string representation of the entire execution plan
    pub fn string_representation(&self) -> &str {
        self.string_representation.as_str()
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Finalize Operation", skip_all, level = "debug")
    )]
    fn finalize_operation(
        current_refcell: Rc<RefCell<IntermediateOperation>>
    ) -> FalkorResult<Arc<Operation>> {
        let current_op = Rc::try_unwrap(current_refcell)
            .map_err(|_| FalkorDBError::RefCountBooBoo)?
            .into_inner();

        let children_count = current_op.children.len();
        Ok(Arc::new(Operation {
            name: current_op.name,
            args: current_op.args,
            records_produced: current_op.records_produced,
            execution_time: current_op.execution_time,
            depth: current_op.depth,
            children: current_op.children.into_iter().try_fold(
                Vec::with_capacity(children_count),
                |mut acc, child| {
                    acc.push(Self::finalize_operation(child)?);
                    Result::<_, FalkorDBError>::Ok(acc)
                },
            )?,
        }))
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Parse Operation Tree To Map", skip_all, level = "trace")
    )]
    fn operations_map_from_tree(
        current_branch: &Arc<Operation>,
        map: &mut HashMap<String, Vec<Arc<Operation>>>,
    ) {
        map.entry(current_branch.name.clone())
            .or_default()
            .push(Arc::clone(current_branch));

        for child in &current_branch.children {
            Self::operations_map_from_tree(child, map);
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Parse Execution Plan", skip_all, level = "info")
    )]
    pub(crate) fn parse(value: redis::Value) -> FalkorResult<Self> {
        let redis_value_vec = redis_value_as_vec(value)?;

        let mut string_representation = Vec::with_capacity(redis_value_vec.len() + 1);
        let mut root_node = None;
        // The last operation parsed and its ancestors, one per level of the plan.
        let mut current_traversal_stack: Vec<Rc<RefCell<IntermediateOperation>>> = vec![];
        for node in redis_value_vec {
            let node_string = redis_value_as_string(node)?;

            // The server indents each level by four spaces. Count only the leading ones: an
            // argument can hold four spaces in a row too.
            let depth = (node_string.len() - node_string.trim_start_matches(' ').len()) / 4;
            let new_node = Rc::new(RefCell::new(IntermediateOperation::new(
                depth,
                node_string.trim(),
            )?));

            // The parent is the closest operation above this one with a smaller depth.
            while current_traversal_stack
                .last()
                .is_some_and(|operation| operation.borrow().depth >= depth)
            {
                current_traversal_stack.pop();
            }
            match current_traversal_stack.last() {
                // Too big a skip
                Some(parent) if depth - parent.borrow().depth > 1 => {
                    return Err(FalkorDBError::CorruptExecutionPlan);
                }
                Some(parent) => parent.borrow_mut().children.push(Rc::clone(&new_node)),
                None if root_node.is_none() => root_node = Some(Rc::clone(&new_node)),
                // A second root, or an operation above the root
                None => return Err(FalkorDBError::CorruptExecutionPlan),
            }
            current_traversal_stack.push(new_node);

            string_representation.push(node_string);
        }

        // Must drop traversal stack first
        drop(current_traversal_stack);
        let operation_tree =
            Self::finalize_operation(root_node.ok_or(FalkorDBError::CorruptExecutionPlan)?)?;

        let mut operations = HashMap::new();
        Self::operations_map_from_tree(&operation_tree, &mut operations);

        Ok(ExecutionPlan {
            string_representation: format!("\n{}", string_representation.join("\n")),
            plan: string_representation,
            operations,
            operation_tree,
        })
    }
}

/// Execution-plan steps without the `Results` root operation that FalkorDB up to 4.2 puts above
/// every plan and newer servers no longer plan. `GRAPH.PROFILE` appends per-operation statistics
/// to the step, hence the second form. Test-only: the client is indifferent to the root, only
/// assertions on a plan's shape have to be.
#[cfg(test)]
pub(crate) fn skip_results_root(steps: &[String]) -> &[String] {
    match steps.first() {
        Some(root) if root == "Results" || root.starts_with("Results |") => &steps[1..],
        _ => steps,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_default() {
        let op = Operation::default();
        assert_eq!(op.name, "");
        assert!(op.args.is_none());
        assert!(op.records_produced.is_none());
        assert!(op.execution_time.is_none());
        assert!(op.children.is_empty());
    }

    #[test]
    fn test_operation_clone() {
        let op = Operation {
            name: "Scan".to_string(),
            args: Some(vec!["arg1".to_string()]),
            records_produced: Some(100),
            execution_time: Some(1.5),
            children: vec![],
            depth: 0,
        };

        let op_clone = op.clone();
        assert_eq!(op, op_clone);
    }

    #[test]
    fn test_operation_debug() {
        let op = Operation {
            name: "Filter".to_string(),
            args: None,
            records_produced: Some(50),
            execution_time: Some(0.5),
            children: vec![],
            depth: 1,
        };

        let debug_str = format!("{:?}", op);
        assert!(debug_str.contains("Filter"));
        assert!(debug_str.contains("50"));
    }

    #[test]
    fn test_operation_with_children() {
        let child = Arc::new(Operation {
            name: "Child".to_string(),
            args: None,
            records_produced: None,
            execution_time: None,
            children: vec![],
            depth: 1,
        });

        let parent = Operation {
            name: "Parent".to_string(),
            args: None,
            records_produced: None,
            execution_time: None,
            children: vec![child],
            depth: 0,
        };

        assert_eq!(parent.children.len(), 1);
        assert_eq!(parent.children[0].name, "Child");
    }

    #[test]
    fn test_execution_plan_methods() {
        let op = Arc::new(Operation::default());
        let plan = ExecutionPlan {
            string_representation: "Test Plan".to_string(),
            plan: vec!["Step 1".to_string(), "Step 2".to_string()],
            operations: HashMap::new(),
            operation_tree: op.clone(),
        };

        assert_eq!(plan.string_representation(), "Test Plan");
        assert_eq!(plan.plan().len(), 2);
        assert_eq!(plan.plan()[0], "Step 1");
        assert_eq!(plan.operations().len(), 0);
        assert_eq!(plan.operation_tree().name, "");
    }

    #[test]
    fn test_execution_plan_clone() {
        let op = Arc::new(Operation {
            name: "Root".to_string(),
            args: None,
            records_produced: None,
            execution_time: None,
            children: vec![],
            depth: 0,
        });

        let plan = ExecutionPlan {
            string_representation: "Plan".to_string(),
            plan: vec!["Step".to_string()],
            operations: HashMap::new(),
            operation_tree: op,
        };

        let plan_clone = plan.clone();
        assert_eq!(plan, plan_clone);
    }

    #[test]
    fn test_execution_plan_debug() {
        let op = Arc::new(Operation::default());
        let plan = ExecutionPlan {
            string_representation: "Debug Test".to_string(),
            plan: vec![],
            operations: HashMap::new(),
            operation_tree: op,
        };

        let debug_str = format!("{:?}", plan);
        assert!(debug_str.contains("ExecutionPlan"));
    }

    #[test]
    fn test_execution_plan_with_operations_map() {
        let op1 = Arc::new(Operation {
            name: "Scan".to_string(),
            args: None,
            records_produced: Some(100),
            execution_time: None,
            children: vec![],
            depth: 0,
        });

        let op2 = Arc::new(Operation {
            name: "Filter".to_string(),
            args: None,
            records_produced: Some(50),
            execution_time: None,
            children: vec![],
            depth: 1,
        });

        let mut operations = HashMap::new();
        operations.insert("Scan".to_string(), vec![op1.clone()]);
        operations.insert("Filter".to_string(), vec![op2.clone()]);

        let plan = ExecutionPlan {
            string_representation: "Complex Plan".to_string(),
            plan: vec!["Scan".to_string(), "    Filter".to_string()],
            operations,
            operation_tree: op1,
        };

        assert_eq!(plan.operations().len(), 2);
        assert!(plan.operations().contains_key("Scan"));
        assert!(plan.operations().contains_key("Filter"));
    }

    /// FalkorDB up to 4.2 roots every plan in a `Results` operation that newer servers no longer
    /// plan, and `GRAPH.PROFILE` appends statistics to that step. Only one of those forms can be
    /// produced by the server a given test run targets, so both are asserted here directly.
    #[test]
    fn test_skip_results_root() {
        let steps = |steps: &[&str]| steps.iter().map(ToString::to_string).collect::<Vec<_>>();

        assert_eq!(
            skip_results_root(&steps(&["Results", "    Project"])),
            ["    Project"]
        );
        assert_eq!(
            skip_results_root(&steps(&[
                "Results | Records produced: 1001, Execution time: 0.084527 ms",
                "    Project | Records produced: 1001, Execution time: 0.104004 ms",
            ])),
            ["    Project | Records produced: 1001, Execution time: 0.104004 ms"]
        );

        // Already rootless, and an operation merely starting with `Results`: both kept whole.
        assert_eq!(
            skip_results_root(&steps(&["Project", "    Unwind"])),
            ["Project", "    Unwind"]
        );
        assert_eq!(skip_results_root(&steps(&["ResultsFoo"])), ["ResultsFoo"]);
    }

    #[test]
    fn test_execution_plan_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ExecutionPlan>();
        assert_send_sync::<Operation>();
    }

    fn plan_reply(steps: &[&str]) -> redis::Value {
        redis::Value::Array(
            steps
                .iter()
                .map(|step| redis::Value::BulkString(step.as_bytes().to_vec()))
                .collect(),
        )
    }

    /// `operation` and its descendants, as `name(child, child)`.
    fn shape(operation: &Operation) -> String {
        if operation.children.is_empty() {
            return operation.name.clone();
        }
        let children: Vec<_> = operation
            .children
            .iter()
            .map(|child| shape(child))
            .collect();
        format!("{}({})", operation.name, children.join(", "))
    }

    #[test]
    fn test_parse_builds_the_tree_from_indentation() {
        let plan = ExecutionPlan::parse(plan_reply(&[
            "Root",
            "    A",
            "        B",
            "            C",
            "            D",
            "    E",
        ]))
        .expect("the plan parses");

        // Only the first child of each operation used to make it into the tree: D and E were lost.
        assert_eq!(shape(plan.operation_tree()), "Root(A(B(C, D)), E)");
        assert_eq!(plan.operations().values().map(Vec::len).sum::<usize>(), 6);
        assert_eq!(plan.plan().len(), 6);
    }

    #[test]
    fn test_parse_counts_only_leading_indentation() {
        // Four spaces inside an argument counted as a level of indentation, so the second scan
        // became a child of the first.
        let plan = ExecutionPlan::parse(plan_reply(&[
            "Project",
            "    Cartesian Product",
            "        Node By Label Scan | (a:actor)",
            "        Node By Label Scan | (n:a    b)",
        ]))
        .expect("the plan parses");
        assert_eq!(
            shape(plan.operation_tree()),
            "Project(Cartesian Product(Node By Label Scan, Node By Label Scan))"
        );
        assert_eq!(
            plan.operation_tree().children[0].children[1].args,
            Some(vec![" (n:a    b)".to_string()])
        );

        // Two levels too deep, the filter was rejected as a corrupt plan.
        let plan = ExecutionPlan::parse(plan_reply(&[
            "Results",
            "    Filter | (n.pad = '        ')",
        ]))
        .expect("the plan parses");
        assert_eq!(shape(plan.operation_tree()), "Results(Filter)");
    }

    #[test]
    fn test_parse_rejects_corrupt_plans() {
        for steps in [
            &[][..],
            // A step two levels below the previous one.
            &["Project", "        Unwind"],
            // A second root.
            &["Project", "Unwind"],
            // A step above the root.
            &["    Project", "Unwind"],
        ] {
            assert!(
                matches!(
                    ExecutionPlan::parse(plan_reply(steps)),
                    Err(FalkorDBError::CorruptExecutionPlan)
                ),
                "{steps:?}"
            );
        }
    }

    #[test]
    fn test_operation_statistics() {
        let stats = |step: &str| {
            let operation = IntermediateOperation::new(0, step).expect("the step parses");
            (operation.records_produced, operation.execution_time)
        };

        assert_eq!(
            stats("Node By Label Scan | (a:actor) | Records produced: 1317, Execution time: 0.113417 ms"),
            (Some(1317), Some(0.113417))
        );
        // The server prints the time with six decimals, but integers and exponents parse as well.
        assert_eq!(
            stats("Scan | Records produced: 0, Execution time: 1 ms"),
            (Some(0), Some(1.0))
        );
        assert_eq!(
            stats("Scan | Records produced: 10, Execution time: 1e-4 ms"),
            (Some(10), Some(1e-4))
        );
        assert_eq!(stats("Scan | (a:actor)"), (None, None));
    }
}
