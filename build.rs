#[cfg(feature = "use_capnp")]
extern crate capnpc;

#[cfg(feature = "use_capnp")]
fn main() {
    capnpc::CompilerCommand::new()
        .file("schema/messages.capnp")
        .run()
        .expect("Failed compiling messages schema");
}

#[cfg(not(feature = "use_capnp"))]
fn main() {}
