/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the Server Side Public License v1 (SSPLv1).
 */

use crate::{FalkorDBError, FalkorResult, FalkorValue};
use regex::Regex;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::ops::Not;
use std::rc::Rc;

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
    fn new(
        depth: usize,
        operation_string: &str,
    ) -> FalkorResult<IntermediateOperation> {
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
    pub fn plan(&self) -> &[String] {
        self.plan.as_slice()
    }

    /// Returns a slice of strings representing each step in the execution plan, which can be iterated.
    pub fn operations(&self) -> &HashMap<String, Vec<Rc<Operation>>> {
        &self.operations
    }

    /// Returns a shared pointer to the operation tree, allowing easy immutable traversal
    pub fn operation_tree(&self) -> &Rc<Operation> {
        &self.operation_tree
    }

    /// Returns a string representation of the entire execution plan
    pub fn string_representation(&self) -> &str {
        self.string_representation.as_str()
    }

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
}

impl TryFrom<FalkorValue> for ExecutionPlan {
    type Error = FalkorDBError;

    fn try_from(value: FalkorValue) -> Result<Self, Self::Error> {
        let execution_plan_operations: Vec<_> = value
            .into_vec()?
            .into_iter()
            .flat_map(FalkorValue::into_string)
            .collect();

        let string_representation = ["".to_string()]
            .into_iter()
            .chain(execution_plan_operations.iter().cloned())
            .collect::<Vec<_>>()
            .join("\n");

        let mut current_traversal_stack = vec![];
        for node in execution_plan_operations.iter().map(String::as_str) {
            let depth = node.matches("    ").count();
            let node = node.trim();

            let current_node = match current_traversal_stack.last().cloned() {
                None => {
                    current_traversal_stack.push(Rc::new(RefCell::new(
                        IntermediateOperation::new(depth, node)?,
                    )));
                    continue;
                }
                Some(current_node) => current_node,
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
        }

        // Must drop traversal stack first
        let root_node = current_traversal_stack
            .into_iter()
            .next()
            .ok_or(FalkorDBError::CorruptExecutionPlan)?;
        let operation_tree = Self::finalize_operation(root_node)?;

        let mut operations = HashMap::new();
        Self::operations_map_from_tree(&operation_tree, &mut operations);

        Ok(ExecutionPlan {
            string_representation,
            plan: execution_plan_operations,
            operations,
            operation_tree,
        })
    }
}
