use std::env;

fn main() {
    if let Ok(lib_dir) = env::var("OCI_LIB_DIR") {
        println!("cargo:rustc-link-search=native={}", lib_dir);
    } else {
        println!("Please set OCI_LIB_DIR to build oci-sys");
    }
    println!("cargo:rustc-link-lib={}", "clntsh");
}
