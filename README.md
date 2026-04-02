# gopro-gpmf

Standalone Java core library for extracting GoPro GPMF telemetry.

Primary upstream reference:

- official GoPro parser: `https://github.com/gopro/gpmf-parser`

## Build

```bash
mvn package
```

## CLI demo

This repository includes a very small demo CLI under:

- `ie.strix.gpmf.cli`

The current command writes a pretty-printed JSON file containing one record per frame:

```bash
java -jar target/gopro-gpmf-0.1.0-SNAPSHOT-all.jar frames-json \
  --input /path/to/GS010199.360 \
  --output /tmp/GS010199_frames.json
```

The output is a JSON array. Each element contains:

- `frameIndex`
- `pts`
- `relativeTime`
- `timeUtc`
- `timeUtcMode`
- `timeUtcConfidence`
- `gps9`
- `mnor`
- `grav`
- derived example angles:
  - `magneticHeading`
  - `magneticPitch`
  - `gravityRoll`
  - `gravityPitch`

If telemetry is missing for a frame, those fields are written as `null`.

## Per-frame metadata

The library also provides a higher-level per-frame metadata product.

This is intentionally a separate layer above raw telemetry extraction.

It uses:

- GNSS time as the absolute UTC anchor
- retrograde extrapolation when the first trusted GNSS anchor occurs later in the video

Important distinction:

- `pts`
- playback timeline
- `relative_time`
- capture-relative timeline

For timelapse-like files, `relative_time` may intentionally diverge from playback `pts`.

## Heading, roll, and pitch note

The library's primary outputs are the raw vectors:

- `MNOR`
- `GRAV`

Any conversion of those vectors into heading, roll, or pitch depends on axis conventions chosen by the consuming application.

The demo CLI currently uses these example formulas:

```text
magneticHeading = atan2(mnor.z, mnor.x) * 180 / pi - 180
magneticPitch   = atan(mnor.y / mnor.z) * 180 / pi

gravityRoll     = atan2(-grav.x, grav.y) * 180 / pi
gravityPitch    = atan2(grav.z, grav.y) * 180 / pi
```

These are practical demo conventions, not universal truth.

In particular:

- heading from `MNOR` depends on how camera/body axes are mapped
- roll/pitch from `GRAV` depend on the chosen interpretation of the gravity vector
- different projects may legitimately use different formulas
