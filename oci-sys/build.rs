use std::env;

#[cfg(target_os = "windows")]
const OCI_LIB: &str = "oci";

#[cfg(not(target_os = "windows"))]
const OCI_LIB: &str = "clntsh";

fn main() {
    if let Ok(lib_dir) = env::var("OCI_LIB_DIR") {
        println!("cargo:rustc-link-search=native={}", lib_dir);
    } else {
        panic!("Please set OCI_LIB_DIR to build oci-sys");
    }
    println!("cargo:rustc-link-lib={}", OCI_LIB);
}
