package ie.strix.gpmf.model;

import java.util.List;
import java.util.Map;

public record ExtractionProvenance(
        String backendRequested,
        String backendUsed,
        String videoId,
        Integer gpmdStreamIndex,
        Integer gpmdTrackId,
        boolean usedPacketSizes,
        boolean usedMp4BoxFallback,
        Double frameRate,
        Integer videoFrameCount,
        Double videoDurationSeconds,
        Integer imageWidth,
        Integer imageHeight,
        Map<String, Integer> signalEntryCounts,
        Map<String, Integer> signalSampleCounts,
        List<String> warnings) {
}
