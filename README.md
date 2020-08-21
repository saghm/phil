[![crates.io](https://img.shields.io/crates/v/phil.svg)](https://crates.io/crates/phil)

# phil - MongoDB cluster initializer

phil is a command-line utility to initialize MongoDB clusters. It uses into [`monger`](https://crates.io/crates/monger-core)]
and [the MongoDB Rust driver](https://crates.io/crates/mongodb) under the hood to start and configure clusters

## Supported platforms

`phil` currently only supports Linux and OS X/MacOS

## Installation

Binary releases for Linux are available [at the GitHub repository](https://github.com/saghm/phil/releases).

Alternately, assuming that you have Rust installed, simply run `cargo install phil`. Note that you'll need to
have `~/.cargo/bin` on your PATH to run phil.
