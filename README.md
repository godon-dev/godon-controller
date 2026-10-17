# godon-controller

Controller Logic that Manages Godon Systemtender Lifecycles

## What the controller tends

- **Systemtender lifecycles** — create, start/stop, update, purge; the
  shutdown-signaling state table; the systemtender metadata registry.
- **Credentials and targets** — catalog tables for secrets and target hosts.
- **Wishes** — the declared-intent registry, both grammars: steer
  (an outcome held in its band) and seek (a commissioned measured
  walk). Wish records plus their lifecycle events (declared, planned,
  assigned, refused, acted, landed, missed, re_opened, corrected,
  returned, closed, plan_error). State derives from the latest event,
  never stored. Windmill scripts: `steerwish_create`, `steerwish_get`,
  `steerwish_update`, `steerwish_delete`, `steerwish_plan`,
  `steerwishes_get`, `steerwish_close` (folder `f/controller/`).

## Division of labor on wishes

The controller holds the wish object and its lifecycle — identity,
the declared body (steer: outcome, band, limits, budget, regime;
seek: form, region, criterion, budget, candidates), events,
timestamps, adoption lookup, purge cascade, and the mirror that
pulls a seek walk's return into its event trail. Planning lives in
causal, keyed by wish id — the plan, bars, refusals, verdict stamps,
vigilance. The controller asks; it never computes: it holds wishes
as it holds systemtenders without optimizing and curves without
measuring.
