/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use falkordb::{FalkorClientBuilder, FalkorResult};

fn main() -> FalkorResult<()> {
    let client = FalkorClientBuilder::new()
        .with_connection_info("falkor://127.0.0.1:6379".try_into()?)
        .build()?;

    // A UDF library is JavaScript: define functions, then register each one with `falkor.register`.
    let script = r#"
function add(a, b) {
    return a + b;
}

function multiply(a, b) {
    return a * b;
}

falkor.register('add', add);
falkor.register('multiply', multiply);
"#;

    // Load the UDF library
    println!("Loading UDF library...");
    client.udf_load("mylib", script, false)?;

    // List all UDF libraries
    println!("\nListing all UDF libraries:");
    let list_result = client.udf_list(None, false)?;
    println!("UDF list: {:?}", list_result);

    // List specific library with code
    println!("\nListing 'mylib' with code:");
    let list_with_code = client.udf_list(Some("mylib"), true)?;
    println!("UDF details: {:?}", list_with_code);

    // Call the functions from Cypher as `<library>.<function>`
    let mut graph = client.select_graph("udf_usage_example");
    let mut result = graph
        .query("RETURN mylib.add(1, 2), mylib.multiply(3, 4)")
        .execute()?;
    if let Some(row) = result.data.next() {
        println!("\nadd(1, 2), multiply(3, 4) = {:?}", row?.into_values());
    }

    // Update the library with replace flag
    let updated_script = r#"
function add(a, b) {
    return a + b;
}

function multiply(a, b) {
    return a * b;
}

function subtract(a, b) {
    return a - b;
}

falkor.register('add', add);
falkor.register('multiply', multiply);
falkor.register('subtract', subtract);
"#;

    println!("\nReplacing UDF library with updated version...");
    client.udf_load("mylib", updated_script, true)?;
    let mut result = graph.query("RETURN mylib.subtract(10, 4)").execute()?;
    if let Some(row) = result.data.next() {
        println!("subtract(10, 4) = {:?}", row?.into_values());
    }
    graph.delete()?;

    // Delete the UDF library
    println!("\nDeleting UDF library...");
    client.udf_delete("mylib")?;

    // Verify it was deleted
    let list_after_delete = client.udf_list(None, false)?;
    println!("UDF list after delete: {:?}", list_after_delete);

    println!("\nUDF operations completed successfully!");

    Ok(())
}
