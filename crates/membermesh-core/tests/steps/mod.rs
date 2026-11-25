use cucumber::{World, given, then, when};

#[derive(Debug, Default, World)]
pub struct CoreWorld {
    version: Option<&'static str>,
}

#[given("the membermesh-core library is loaded")]
fn core_loaded(_world: &mut CoreWorld) {}

#[when("I query the crate version")]
fn query_version(world: &mut CoreWorld) {
    world.version = Some(membermesh_core::crate_version());
}

#[then("a non-empty version string is returned")]
fn assert_version(world: &mut CoreWorld) {
    let version = world.version.expect("version should be populated");
    assert_ne!(version, "", "crate_version must not be empty");
}
