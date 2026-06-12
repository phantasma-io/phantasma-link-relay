[private]
just:
    just -l

[group('format')]
format:
    cargo fmt

alias f := format

[group('test')]
test:
    cargo test

alias t := test

[group('lint')]
clippy:
    cargo clippy --all-targets -- -D warnings

[group('build')]
build:
    cargo build --release

[group('run')]
run config="":
    cargo run --release -- {{config}}
