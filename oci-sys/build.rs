use std::env;

fn main() {
    if let Ok(lib_dir) = env::var("OCCI_LIB_DIR") {
        println!("cargo:rustc-link-search=native={}", lib_dir);
    } else {
        println!("Please set OCCI_LIB_DIR to build oci-sys");
    }
}

