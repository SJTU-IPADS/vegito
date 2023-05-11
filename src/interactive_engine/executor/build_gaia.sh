export CC=/usr/bin/gcc-8
export CXX=/usr/bin/g++-8

rustup component add rustfmt

cargo build --offline
