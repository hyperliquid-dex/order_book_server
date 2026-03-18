set shell := ["bash", "-lc"]

build:
    cargo build --release

test:
    cargo test --workspace --all-features -- -Z unstable-options --shuffle

fmt:
    cargo fmt --all

lint:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

clean:
    cargo clean

asan:
    ASAN_OPTIONS=detect_leaks=0 \
    RUSTFLAGS="-Zsanitizer=address" \
    RUSTDOCFLAGS="-Zsanitizer=address" \
    cargo +nightly test --workspace --all-features -Z build-std --target x86_64-unknown-linux-gnu

tsan:
    RUSTFLAGS="-Zsanitizer=thread" \
    RUSTDOCFLAGS="-Zsanitizer=thread" \
    cargo +nightly test --workspace --all-features -Z build-std --target x86_64-unknown-linux-gnu
