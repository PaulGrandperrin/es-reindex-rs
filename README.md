How to prepare the environment
==============================

```sh
wget https://static.rust-lang.org/dist/2016-06-14/rust-nightly-x86_64-unknown-linux-gnu.tar.gz
wget https://static.rust-lang.org/dist/2016-06-14/rust-std-nightly-x86_64-unknown-linux-musl.tar.gz

tar xf rust-nightly-x86_64-unknown-linux-gnu.tar.gz
tar xf rust-std-nightly-x86_64-unknown-linux-musl.tar.gz

sudo rust-nightly-x86_64-unknown-linux-gnu/install.sh
sudo rust-std-nightly-x86_64-unknown-linux-musl/install.sh
```

How to build
============

```sh
# dynamic debug
cargo build

# static release
cargo build --target=x86_64-unknown-linux-musl --release
```
