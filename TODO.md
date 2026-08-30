# Work list

Everything in the design document is implemented and covered by tests.
This is what sits outside them.

## Release

- [ ] Publish the first release to PyPI. The pipeline is ready and the metadata
      is complete; a version number on PyPI cannot be reused, so the timing is a
      decision rather than a step.

## Later

- [ ] **Cluster resharding.** The suite runs against a three-node cluster, so
      MOVED redirects happen and the cross-slot rule has real owners behind it.
      Moving slots between nodes while a worker is consuming is not covered.
- [x] **Metrics.** The health endpoint carries this process's counters,
      including `unknown_task`, retries and DLQ writes by reason. Exporting them
      to Prometheus is left to the application.
- [ ] **A payload store implementation.** The seam and the threshold exist; no
      backend ships, so every user writes the same S3 adapter.
