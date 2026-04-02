# gopro-gpmf

Standalone Java core library for extracting GoPro GPMF telemetry.

Primary upstream reference:

- official GoPro parser: `https://github.com/gopro/gpmf-parser`

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

