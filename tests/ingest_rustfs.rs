//! Optional docker integration test for RustFS ingest.
//! Run with: cargo test --test ingest_rustfs -- --ignored --nocapture

#[test]
#[ignore = "requires docker-compose.objectstorage stack"]
fn ingest_rustfs_smoke_placeholder() {
    // End-to-end verification lives in scripts/test_rustfs_ingest.py
    assert!(true);
}
