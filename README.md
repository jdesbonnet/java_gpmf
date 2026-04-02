# gopro-gpmf

Standalone Java library for extracting GoPro GPMF telemetry.

Primary upstream reference:

- official GoPro parser: `https://github.com/gopro/gpmf-parser`

## Build

```bash
mvn package
```

## CLI demo

This repository includes a small demo CLI:

```bash
java -jar target/gopro-gpmf-0.1.0-SNAPSHOT-all.jar frames-json \
  --input /path/to/GS010199.360 \
  --output /tmp/GS010199_frames.json
```

The command writes pretty-printed JSON with one record per frame.
Missing telemetry fields are written as `null`.

## Technical notes

Detailed notes on:

- absolute time derivation
- timelapse capture timing
- heading, roll, and pitch conventions
- JSON output fields

are in:

- `docs/technical_notes.md`
