# Football Match Data Pipeline

Automated, validated and reproducible football match data pipeline.

The project collects football match data from officially configured open
data sources, normalizes the records into a common structure, removes
duplicates, validates the resulting dataset, and publishes the final
`matches.json` artifact.

The pipeline is designed to run automatically through GitHub Actions.

---

## 1. Purpose

The purpose of this repository is to provide a controlled pipeline for
producing a canonical football match dataset that can be consumed by an
Android application or another downstream client.

The pipeline follows this principle:

> Fetch → Normalize → Deduplicate → Validate → Build → Publish

No raw external source is exposed directly to the application.

The application consumes only the validated canonical dataset:

```text
data/matches.json
