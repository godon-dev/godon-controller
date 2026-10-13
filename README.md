# godon-controller

Controller Logic that Manages Godon Systemtender Lifecycles

## What the controller tends

- **Systemtender lifecycles** — create, start/stop, update, purge; the
  shutdown-signaling state table; the systemtender metadata registry.
- **Credentials and targets** — catalog tables for secrets and target hosts.
- **Steerwishes** — the declared-outcome registry: wish records plus their
  lifecycle events (declared, planned, refused, acted, landed, missed,
  re_opened, closed). State derives from the latest event, never stored.
  Windmill scripts: `steerwish_create`, `steerwish_get`, `steerwishes_get`,
  `steerwish_close` (folder `f/controller/`).

## Division of labor on wishes

The controller holds the wish object and its lifecycle — identity,
coordinates (outcome, band, limits, budget, regime), events, timestamps,
adoption lookup, purge cascade. Steering calculation lives in causal,
keyed by wish id — the plan, bars, refusals, verdict stamps, vigilance.
The controller asks; it never computes: it holds wishes as it holds
systemtenders without optimizing and curves without measuring.
