/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

use falkordb::{FalkorClientBuilder, FalkorResult};

fn main() -> FalkorResult<()> {
    let client = FalkorClientBuilder::new()
        .with_connection_info("falkor://127.0.0.1:6379".try_into()?)
        .build()?;

    // Example UDF script that adds two numbers
    let script = r#"
#!js api_version=1.0 name=mylib

redis.registerFunction('add', function(a, b) {
    return a + b;
});

redis.registerFunction('multiply', function(a, b) {
    return a * b;
});
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

    // Update the library with replace flag
    let updated_script = r#"
#!js api_version=1.0 name=mylib

redis.registerFunction('add', function(a, b) {
    return a + b;
});

redis.registerFunction('multiply', function(a, b) {
    return a * b;
});

redis.registerFunction('subtract', function(a, b) {
    return a - b;
});
"#;

    println!("\nReplacing UDF library with updated version...");
    client.udf_load("mylib", updated_script, true)?;

    // Delete the UDF library
    println!("\nDeleting UDF library...");
    client.udf_delete("mylib")?;

    // Verify it was deleted
    let list_after_delete = client.udf_list(None, false)?;
    println!("UDF list after delete: {:?}", list_after_delete);

    println!("\nUDF operations completed successfully!");

    Ok(())
}
