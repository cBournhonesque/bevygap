Some misc bash utils I found useful for testing Edgegap bits.

`gen-edgegap-client.sh` regenerates `edgegap_async` from Edgegap's OpenAPI spec.
It downloads the spec into `target/edgegap-openapi/`, runs the pinned OpenAPI
generator container, applies the small Cargo metadata patch we need locally, and
runs `cargo fmt -p edgegap_async`. Generated `edgegap_async/src` files should not
be hand-edited; patch the script or the post-generation step instead.
