package ie.strix.gpmf.frame;

import java.util.List;

public record FrameMetadataSeries(
        String videoId,
        Integer imageWidth,
        Integer imageHeight,
        String timeUtcMode,
        String timeUtcConfidence,
        Integer trustedGnssAnchorCount,
        Integer firstTrustedFrameIndex,
        List<FrameMetadata> frames,
        List<String> warnings) {
}
