#!/bin/bash

# Location of Safe Client Libs to run "cargo test --release --features=simulated-payouts"
 if [ -z "$CLIENT_TESTS_PATH" ]; then
    echo "No \"\$CLIENT_TESTS_PATH env var set\""
    exit 1
fi

# What do we want from out integration tests? Just core? Running via rust compilation? Using our CLI? TBD

set -e -x

./scripts/run-vaults.sh

cd $CLIENT_TESTS_PATH && cargo test --release --features=simulated-payouts || pkill -f target/release/safe_vault