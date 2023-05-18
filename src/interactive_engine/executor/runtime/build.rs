//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

extern crate cmake;

use std::path::Path;
use cmake::Config;
use std::env;

fn main() {
    let dst = Config::new("native")
        .build_target("native_store")
        .build();

    println!("cargo:rustc-link-search=/usr/local/lib");
    println!("cargo:rustc-link-search=/usr/local/lib64");
    println!("cargo:rustc-link-search=/opt/homebrew/lib");
    match env::var("VINEYARD_ROOT_DIR") {
        Ok(val) => {
            println!("cargo:rustc-link-search={}/lib", val);
            println!("cargo:rustc-link-search={}/lib64", val);
        }
        Err(_) => ()
    }
    println!("cargo:rustc-link-search={}/build", dst.display());
    println!("cargo:rustc-link-lib=native_store");
}