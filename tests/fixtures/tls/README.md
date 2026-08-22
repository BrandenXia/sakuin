# Test-only PKI

These certificates and unencrypted private keys exist only for deterministic
loopback mTLS integration tests. The CA is not trusted by Sakuin outside tests,
and none of these files are suitable for deployment.

The worker leaf certificate has common name `worker-1`; tests verify that it
can use `worker-1` and the namespaced identities `worker-1:v4`/`worker-1:v6`,
but cannot impersonate another worker namespace.
