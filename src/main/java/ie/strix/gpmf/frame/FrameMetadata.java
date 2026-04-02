package ie.strix.gpmf.frame;

import java.time.Instant;

public record FrameMetadata(
        int frameIndex,
        double pts,
        double relativeTime,
        Instant timeUtc,
        String timeUtcMode,
        String timeUtcConfidence,
        GnssFix gps9,
        FrameVector3 mnor,
        FrameVector3 grav) {
}
