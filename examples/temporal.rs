/*
 * Copyright FalkorDB Ltd. 2023 - present
 * Licensed under the MIT License.
 */

//! Decoding FalkorDB temporal values and using the type-safe temporal algebra.
//!
//! Run with: `cargo run --example temporal` (needs a FalkorDB server on 127.0.0.1:6379).

use falkordb::{Date, DateTime, Duration, FalkorClientBuilder, FalkorResult, Time};

fn main() -> FalkorResult<()> {
    let client = FalkorClientBuilder::new()
        .with_connection_info("falkor://127.0.0.1:6379".try_into()?)
        .build()?;

    let mut graph = client.select_graph("temporal_example");
    let _ = graph.delete();

    // FalkorDB returns `date` / `time` / `duration` as typed, seconds-based scalars. The client
    // decodes them into the `Date` / `Time` / `Duration` value types rather than leaving them
    // `Unparseable`, so you can read them with strict typed access.
    let mut result = graph
        .query("RETURN date('1947-11-29') AS d, localtime() AS t, duration({days: 3}) AS dur")
        .execute()?;
    let row = result.data.next().expect("expected a row")?;

    let d: Date = row.try_get("d")?;
    let t: Time = row.try_get("t")?;
    let dur: Duration = row.try_get("dur")?;

    // Each value exposes its scalar as a typed `Seconds` (not a bare `i64`) — call `.get()` for the
    // underlying integer. This keeps a temporal scalar from being silently mixed with a plain int.
    println!("date is {} seconds since the Unix epoch", d.seconds().get());
    println!("localtime is {} seconds", t.seconds());
    println!("duration is {} seconds", dur.seconds().get());
    println!(
        "duration as std::time::Duration: {:?}",
        dur.as_std_duration()
    );

    // `DateTime` and `Duration` support a small, type-safe algebra. Subtracting two instants yields
    // a `Duration`; shifting an instant by a `Duration` yields another `DateTime`.
    let instant = DateTime::new(d.seconds().get());
    let earlier = DateTime::new(d.seconds().get() - 60);

    let elapsed: Duration = instant - earlier;
    assert_eq!(elapsed.seconds().get(), 60);
    println!("instant is {} seconds after `earlier`", elapsed.seconds());

    // Shifting forward by a duration and back again is a no-op.
    assert_eq!(instant + dur - dur, instant);

    // Overflow-checked variants return `None` instead of panicking.
    assert!(DateTime::new(i64::MAX).checked_add(dur).is_none());

    // Nonsensical combinations simply have no impl and fail to compile, e.g.:
    //     let _ = instant + earlier; // error: cannot add two instants

    graph.delete()?;
    Ok(())
}
