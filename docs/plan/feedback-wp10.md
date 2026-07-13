# WP-10 Feedback

## Proxy variables are an explicit environment-policy choice

`EnvPolicy::BASELINE_ENV` deliberately omits `HTTP_PROXY`, `HTTPS_PROXY`, and
`NO_PROXY`. In networks that require an outbound proxy, a scrubbed coding CLI
may therefore be unable to reach its upstream service.

The public `EnvPolicy.allow` field is the supported per-job escape hatch. The
opt-in real-CLI smoke helper may allow the proxy variable names when the test
environment requires them; no value is logged, persisted, or added to a
fixture.

The proxy variables should not be added to the baseline implicitly. They can
contain credentials or redirect network traffic, so inheriting them changes
the execution authority and secret surface. A future default change requires a
separate policy decision, redacted tests, and explicit documentation of the
trust boundary. Until then, callers opt in by variable name and remain
responsible for the selected proxy.
