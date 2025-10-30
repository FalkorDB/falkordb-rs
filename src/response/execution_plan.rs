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
    cmp::Ordering,
    collections::{HashMap, VecDeque},
    ops::Not,
    rc::Rc,
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
    ) -> FalkorResult<Self> {
        let mut args = operation_string.split('|').collect::<VecDeque<_>>();
        let name = args
            .pop_front()
            .ok_or(FalkorDBError::CorruptExecutionPlan)?
            .trim();

        let (records_produced, execution_time) = match args.pop_back() {
            Some(last_arg) if last_arg.contains("Records produced") => (
                Regex::new(r"Records produced: (\d+)")
                    .map_err(|err| {
                        FalkorDBError::ParsingError(format!("Error constructing regex: {err}"))
                    })?
                    .captures(last_arg.trim())
                    .and_then(|cap| cap.get(1))
                    .and_then(|m| m.as_str().parse().ok()),
                Regex::new(r"Execution time: (\d+\.\d+) ms")
                    .map_err(|err| {
                        FalkorDBError::ParsingError(format!("Error constructing regex: {err}"))
                    })?
                    .captures(last_arg.trim())
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
    pub children: Vec<Rc<Operation>>,
    depth: usize,
}

/// An execution plan, allowing access both to the human-readable text representation, access to a per-operation map, or traversable operation tree
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionPlan {
    string_representation: String,
    plan: Vec<String>,
    operations: HashMap<String, Vec<Rc<Operation>>>,
    operation_tree: Rc<Operation>,
}

impl ExecutionPlan {
    /// Returns the plan as a slice of human-readable strings
    #[must_use]
    pub const fn plan(&self) -> &[String] {
        self.plan.as_slice()
    }

    /// Returns a slice of strings representing each step in the execution plan, which can be iterated.
    #[must_use]
    pub const fn operations(&self) -> &HashMap<String, Vec<Rc<Operation>>> {
        &self.operations
    }

    /// Returns a shared pointer to the operation tree, allowing easy immutable traversal
    #[must_use]
    pub const fn operation_tree(&self) -> &Rc<Operation> {
        &self.operation_tree
    }

    /// Returns a string representation of the entire execution plan
    #[must_use]
    pub const fn string_representation(&self) -> &str {
        self.string_representation.as_str()
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Create Node", skip_all, level = "debug")
    )]
    fn create_node(
        depth: usize,
        operation_string: &str,
        traversal_stack: &mut Vec<Rc<RefCell<IntermediateOperation>>>,
    ) -> FalkorResult<()> {
        let new_node = Rc::new(RefCell::new(IntermediateOperation::new(
            depth,
            operation_string,
        )?));

        traversal_stack.push(Rc::clone(&new_node));
        Ok(())
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "Finalize Operation", skip_all, level = "debug")
    )]
    fn finalize_operation(
        current_refcell: Rc<RefCell<IntermediateOperation>>
    ) -> FalkorResult<Rc<Operation>> {
        let current_op = Rc::try_unwrap(current_refcell)
            .map_err(|_| FalkorDBError::RefCountBooBoo)?
            .into_inner();

        let children_count = current_op.children.len();
        Ok(Rc::new(Operation {
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
        current_branch: &Rc<Operation>,
        map: &mut HashMap<String, Vec<Rc<Operation>>>,
    ) {
        map.entry(current_branch.name.clone())
            .or_default()
            .push(Rc::clone(current_branch));

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
        let mut current_traversal_stack = vec![];
        for node in redis_value_vec {
            let node_string = redis_value_as_string(node)?;

            let depth = node_string.matches("    ").count();
            let node = node_string.trim();

            let Some(current_node) = current_traversal_stack.last().cloned() else {
                current_traversal_stack.push(Rc::new(RefCell::new(IntermediateOperation::new(
                    depth, node,
                )?)));
                string_representation.push(node_string);
                continue;
            };

            let current_depth = current_node.borrow().depth;
            match depth.cmp(&current_depth) {
                Ordering::Less => {
                    let times_to_pop = (current_depth - depth) + 1;
                    if times_to_pop > current_traversal_stack.len() {
                        return Err(FalkorDBError::CorruptExecutionPlan);
                    }
                    for _ in 0..times_to_pop {
                        current_traversal_stack.pop();
                    }

                    // Create this node as a child to the last node with one less depth than the new node
                    Self::create_node(depth, node, &mut current_traversal_stack)?;
                }
                Ordering::Equal => {
                    // Push new node to the parent node
                    current_traversal_stack.pop();
                    Self::create_node(depth, node, &mut current_traversal_stack)?;
                }
                Ordering::Greater => {
                    if depth - current_depth > 1 {
                        // Too big a skip
                        return Err(FalkorDBError::CorruptExecutionPlan);
                    }

                    let new_node = Rc::new(RefCell::new(IntermediateOperation::new(depth, node)?));
                    current_traversal_stack.push(Rc::clone(&new_node));

                    // New node is a child of the current node, so we will push it as a child
                    current_node.borrow_mut().children.push(new_node);
                }
            }

            string_representation.push(node_string);
        }

        // Must drop traversal stack first
        let root_node = current_traversal_stack
            .into_iter()
            .next()
            .ok_or(FalkorDBError::CorruptExecutionPlan)?;
        let operation_tree = Self::finalize_operation(root_node)?;

        let mut operations = HashMap::new();
        Self::operations_map_from_tree(&operation_tree, &mut operations);

        Ok(Self {
            string_representation: format!("\n{}", string_representation.join("\n")),
            plan: string_representation,
            operations,
            operation_tree,
        })
    }
}
