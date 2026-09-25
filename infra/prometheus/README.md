# Prometheus alerting

[`alerts.yml`](alerts.yml) holds the alerting rules for the pipeline. The
metrics they watch are pushed by
[`data_pipeline/metrics.py`](../../data_pipeline/metrics.py) after every run.

## Wiring it up

The pipeline pushes to a Pushgateway rather than exposing a scrape endpoint —
it is a batch job, so an endpoint would vanish before Prometheus could scrape
it. Point Prometheus at the gateway and load the rules:

```yaml
# prometheus.yml
rule_files:
  - /etc/prometheus/alerts.yml

scrape_configs:
  - job_name: pushgateway
    # Keep the labels the pipeline pushed (job="data_observatory") instead of
    # overwriting them with the gateway's own target labels.
    honor_labels: true
    static_configs:
      - targets: ["pushgateway:9091"]
```

Check the syntax before shipping a change:

```bash
promtool check rules alerts.yml
```

`tests/unit/test_alert_rules.py` resolves every selector in the rules against
samples the exporter actually produces, so a renamed metric or a mistyped
label value fails CI rather than quietly disabling an alert.

## What these rules assume

- **The pipeline runs hourly** (`DAGSTER_CRON`, default `0 * * * *`).
  `PipelineRunMissing` allows three missed runs before firing. Slow the
  schedule down and you must raise that threshold, or it fires every night.
- **Metrics are enabled where the pipeline runs.** They are off by default;
  without `METRICS_ENABLED=true` and `PROMETHEUS_PUSHGATEWAY_URL`, nothing is
  pushed and `PipelineMetricsAbsent` is the only rule that will ever fire.
- **One grouping key per deployment.** Each push replaces every metric under
  its `job` label, so two environments pushing as the same job overwrite each
  other. Give staging its own `METRICS_JOB_NAME`.

## Why a dead man's switch is the important rule

Pushed metrics never go stale: the gateway serves the last sample forever. A
scheduler that stops firing therefore changes *nothing* a counter-based rule
can see — the failure looks exactly like a quiet, healthy system. The age of
`observatory_pipeline_last_run_timestamp_seconds` is the only honest signal
that the job is still alive, and `PipelineRunMissing` is the rule that watches
it.
