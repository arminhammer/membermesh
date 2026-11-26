//! Cucumber BDD test runner for `membermesh`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

mod steps;

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use cucumber::{StatsWriter as _, World as _, event::ScenarioFinished};

fn has_tag(tags: &[String], tag: &str) -> bool {
    tags.iter().any(|t| t.trim_start_matches('@') == tag)
}

fn enabled(var: &str) -> bool {
    std::env::var(var).is_ok_and(|v| v == "1")
}

fn skipped(tags: &[String]) -> bool {
    has_tag(tags, "todo")
        || (has_tag(tags, "slow") && !enabled("BDD_SLOW"))
        || (has_tag(tags, "network") && !enabled("BDD_NETWORK"))
}

#[tokio::main]
async fn main() {
    let concurrency = std::env::var("BDD_SCENARIO_CONCURRENCY")
        .ok()
        .and_then(|n| n.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(2);
    let timeout_limit = membermesh_testkit::scenario_timeout_from_env();
    let active: Arc<Mutex<BTreeMap<String, Instant>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let active_before = Arc::clone(&active);
    let active_after = Arc::clone(&active);

    let watchdog = tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let overdue: Vec<String> = {
                let Ok(guard) = active.lock() else {
                    break;
                };
                let now = Instant::now();
                guard
                    .iter()
                    .filter_map(|(name, started)| {
                        let elapsed = now.duration_since(*started);
                        (elapsed > timeout_limit).then(|| format!("{name} ({elapsed:?})"))
                    })
                    .collect()
            };
            if !overdue.is_empty() {
                eprintln!("BDD hang watchdog triggered (limit {timeout_limit:?}): {overdue:?}");
                std::process::exit(1);
            }
        }
    });

    let result = steps::MeshWorld::cucumber()
        .max_concurrent_scenarios(concurrency)
        .fail_on_skipped()
        .before(move |_feature, _rule, scenario, _world| {
            let mut guard = active_before.lock().expect("active scenario map poisoned");
            guard.insert(scenario.name.clone(), Instant::now());
            Box::pin(async {})
        })
        .after(
            move |_feature, _rule, scenario, _ev: &ScenarioFinished, world| {
                {
                    let mut guard = active_after.lock().expect("active scenario map poisoned");
                    guard.remove(&scenario.name);
                }
                let cluster = world.and_then(|w| w.cluster.take());
                Box::pin(async move {
                    if let Some(c) = cluster {
                        c.shutdown_all().await;
                    }
                })
            },
        )
        .filter_run("tests/features", |feature, rule, scenario| {
            !(skipped(&feature.tags)
                || rule.is_some_and(|r| skipped(&r.tags))
                || skipped(&scenario.tags))
        })
        .await;

    watchdog.abort();
    assert!(
        !result.execution_has_failed(),
        "Cucumber BDD run failed (or had skipped/unimplemented steps)"
    );
}
