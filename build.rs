extern crate protobuf_codegen_pure;

fn main() {
    protobuf_codegen_pure::Codegen::new()
        .out_dir("src/protos")
        .inputs(&["protos/a.proto", "protos/b.proto"])
        .include("protos")
        .run()
        .expect("Codegen failed.");
}