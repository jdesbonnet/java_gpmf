package ie.strix.gpmf.frame;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ie.strix.gpmf.decode.DecodeOptions;
import ie.strix.gpmf.decode.GpmfDecoder;
import ie.strix.gpmf.model.DecodedTelemetry;
import ie.strix.gpmf.model.TelemetryEntry;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class FrameMetadataExtractor {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long GPS_EPOCH_MILLIS = Instant.parse("2000-01-01T00:00:00Z").toEpochMilli();
    private static final long TRUSTED_OFFSET_CLUSTER_TOLERANCE_MS = 5_000L;
    private static final int MIN_TRUSTED_ANCHOR_COUNT = 3;
    private static final double TIMELAPSE_CADENCE_RATIO_THRESHOLD = 3.0d;

    public FrameMetadataSeries extract(Path videoFile) throws Exception {
        GpmfDecoder decoder = new GpmfDecoder();
        DecodedTelemetry telemetry = decoder.decode(videoFile, DecodeOptions.defaults());
        return extract(videoFile, telemetry);
    }

    public FrameMetadataSeries extract(Path videoFile, DecodedTelemetry telemetry) throws Exception {
        Objects.requireNonNull(videoFile, "videoFile");
        Objects.requireNonNull(telemetry, "telemetry");
        List<Double> framePts = approximateFramePts(telemetry);
        return build(videoFile, telemetry, framePts);
    }

    public FrameMetadataSeries extractExact(Path videoFile, DecodedTelemetry telemetry) throws Exception {
        Objects.requireNonNull(videoFile, "videoFile");
        Objects.requireNonNull(telemetry, "telemetry");
        List<Double> framePts = readFramePts(videoFile);
        return build(videoFile, telemetry, framePts);
    }

    public FrameMetadataSeries build(Path videoFile,
                                     DecodedTelemetry telemetry,
                                     List<Double> framePts) {
        Objects.requireNonNull(videoFile, "videoFile");
        Objects.requireNonNull(telemetry, "telemetry");
        List<Double> normalizedFramePts = sanitizeFramePts(framePts);
        List<String> warnings = new ArrayList<>();
        if (telemetry.provenance() != null && telemetry.provenance().warnings() != null) {
            warnings.addAll(telemetry.provenance().warnings());
        }
        if (normalizedFramePts.isEmpty()) {
            warnings.add("No video frame PTS values were available.");
            return new FrameMetadataSeries(
                    telemetry.provenance() == null ? stripExtension(videoFile.getFileName().toString()) : telemetry.provenance().videoId(),
                    telemetry.provenance() == null ? null : telemetry.provenance().imageWidth(),
                    telemetry.provenance() == null ? null : telemetry.provenance().imageHeight(),
                    "unavailable",
                    "none",
                    0,
                    null,
                    List.of(),
                    List.copyOf(warnings));
        }

        double videoDuration = normalizedFramePts.get(normalizedFramePts.size() - 1);
        if (videoDuration < 0.0d) {
            videoDuration = 0.0d;
        }

        CaptureTimeline captureTimeline = deriveCaptureTimeline(telemetry, normalizedFramePts, warnings);
        List<Double> relativeFrameTimes = captureTimeline.relativeFrameTimes();

        List<SignalSamplePoint> gps9 = buildSignalSamplePoints(telemetry.signals().get("GPS9"), normalizedFramePts, videoDuration, captureTimeline);
        List<SignalSamplePoint> mnor = buildSignalSamplePoints(telemetry.signals().get("MNOR"), normalizedFramePts, videoDuration, captureTimeline);
        List<SignalSamplePoint> grav = buildSignalSamplePoints(telemetry.signals().get("GRAV"), normalizedFramePts, videoDuration, captureTimeline);

        AnchorCluster trustedAnchor = selectTrustedGnssAnchor(gps9);
        String overallTimeMode = trustedAnchor == null
                ? "untrusted_or_missing_gnss"
                : ("capture_timeline".equals(captureTimeline.mode()) ? "gps9_capture_timeline_anchored" : "gps9_pts_anchored");
        String overallTimeConfidence = trustedAnchor == null ? "none" : confidenceLabel(trustedAnchor.members().size());
        Integer firstTrustedFrameIndex = trustedAnchor == null ? null : firstTrustedFrameIndex(relativeFrameTimes, trustedAnchor.earliestPts());

        MedianSpacing gpsSpacing = medianSpacing(gps9);
        List<FrameMetadata> frames = new ArrayList<>(normalizedFramePts.size());
        for (int frameIndex = 0; frameIndex < normalizedFramePts.size(); frameIndex++) {
            double pts = normalizedFramePts.get(frameIndex);
            double relativeTime = relativeFrameTimes.get(frameIndex);
            Instant timeUtc = null;
            String timeUtcMode = "null";
            String timeUtcConfidence = "none";
            if (trustedAnchor != null) {
                long timeUtcMillis = Math.round(trustedAnchor.offsetMillis() + relativeTime * 1000.0d);
                timeUtc = Instant.ofEpochMilli(timeUtcMillis);
                timeUtcMode = relativeTime + 1e-9 < trustedAnchor.earliestPts() ? "retrograde_extrapolated" : "gnss_anchored";
                timeUtcConfidence = confidenceLabel(trustedAnchor.members().size());
            }

            GnssFix gpsFix = nearestGnssFix(gps9, gpsSpacing, relativeTime);
            FrameVector3 mnorVector = nearestVector(mnor, relativeTime);
            FrameVector3 gravVector = nearestVector(grav, relativeTime);
            frames.add(new FrameMetadata(
                    frameIndex,
                    pts,
                    relativeTime,
                    timeUtc,
                    timeUtcMode,
                    timeUtcConfidence,
                    gpsFix,
                    mnorVector,
                    gravVector));
        }

        return new FrameMetadataSeries(
                telemetry.provenance() == null ? stripExtension(videoFile.getFileName().toString()) : telemetry.provenance().videoId(),
                telemetry.provenance() == null ? null : telemetry.provenance().imageWidth(),
                telemetry.provenance() == null ? null : telemetry.provenance().imageHeight(),
                overallTimeMode,
                overallTimeConfidence,
                trustedAnchor == null ? 0 : trustedAnchor.members().size(),
                firstTrustedFrameIndex,
                List.copyOf(frames),
                List.copyOf(warnings));
    }

    private static List<Double> readFramePts(Path videoFile) throws Exception {
        List<String> cmd = List.of(
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_frames",
                "-show_entries", "frame=best_effort_timestamp_time,pkt_dts_time,pkt_pts_time",
                "-print_format", "json",
                videoFile.toAbsolutePath().toString());
        Process process = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String json;
        try (InputStream is = process.getInputStream()) {
            json = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IllegalStateException("ffprobe frame scan failed with exit code " + exitCode);
        }
        JsonNode root = MAPPER.readTree(json);
        JsonNode frames = root.get("frames");
        if (frames == null || !frames.isArray()) {
            return List.of();
        }
        List<Double> out = new ArrayList<>();
        for (JsonNode frame : frames) {
            Double pts = parseDoubleField(frame, "best_effort_timestamp_time");
            if (pts == null) {
                pts = parseDoubleField(frame, "pkt_pts_time");
            }
            if (pts == null) {
                pts = parseDoubleField(frame, "pkt_dts_time");
            }
            if (pts != null && Double.isFinite(pts)) {
                out.add(pts);
            }
        }
        return out;
    }

    static List<Double> approximateFramePts(DecodedTelemetry telemetry) {
        Objects.requireNonNull(telemetry, "telemetry");
        Integer frameCount = deriveApproximateFrameCount(telemetry);
        if (frameCount == null || frameCount <= 0) {
            return List.of();
        }
        Double frameRate = telemetry.provenance() == null ? null : telemetry.provenance().frameRate();
        Double durationSeconds = telemetry.provenance() == null ? null : telemetry.provenance().videoDurationSeconds();
        if (frameRate != null && Double.isFinite(frameRate) && frameRate > 0.0d) {
            List<Double> out = new ArrayList<>(frameCount);
            for (int i = 0; i < frameCount; i++) {
                out.add(i / frameRate);
            }
            return out;
        }
        if (durationSeconds != null && Double.isFinite(durationSeconds) && durationSeconds > 0.0d) {
            double step = frameCount <= 1 ? 0.0d : durationSeconds / Math.max(1, frameCount - 1);
            List<Double> out = new ArrayList<>(frameCount);
            for (int i = 0; i < frameCount; i++) {
                out.add(i * step);
            }
            return out;
        }
        List<Double> out = new ArrayList<>(frameCount);
        for (int i = 0; i < frameCount; i++) {
            out.add((double) i);
        }
        return out;
    }

    private static Integer deriveApproximateFrameCount(DecodedTelemetry telemetry) {
        if (telemetry.provenance() != null) {
            Integer exactFrameCount = telemetry.provenance().videoFrameCount();
            if (exactFrameCount != null && exactFrameCount > 0) {
                return exactFrameCount;
            }
        }
        Map<String, List<TelemetryEntry>> signals = telemetry.signals();
        int best = 0;
        for (String key : List.of("GPS9", "MNOR", "GRAV", "SHUT", "WBAL", "WRGB", "ISOE", "UNIF", "DSTC", "OCNF", "CORI", "IORI", "CSCT", "CSCB")) {
            int count = sampleCount(signals.get(key));
            if (count > best) {
                best = count;
            }
        }
        return best > 0 ? best : null;
    }

    private static Double parseDoubleField(JsonNode node, String field) {
        if (node == null) {
            return null;
        }
        JsonNode child = node.get(field);
        if (child == null || child.isNull()) {
            return null;
        }
        if (child.isNumber()) {
            double value = child.asDouble();
            return Double.isFinite(value) ? value : null;
        }
        String text = child.asText();
        if (text == null || text.isBlank()) {
            return null;
        }
        try {
            double value = Double.parseDouble(text);
            return Double.isFinite(value) ? value : null;
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static List<Double> sanitizeFramePts(List<Double> framePts) {
        if (framePts == null || framePts.isEmpty()) {
            return List.of();
        }
        List<Double> out = new ArrayList<>(framePts.size());
        double prev = Double.NEGATIVE_INFINITY;
        for (Double pts : framePts) {
            if (pts == null || !Double.isFinite(pts)) {
                continue;
            }
            double value = pts;
            if (prev != Double.NEGATIVE_INFINITY && value < prev) {
                value = prev;
            }
            out.add(value);
            prev = value;
        }
        return out;
    }

    private static List<SignalSamplePoint> buildSignalSamplePoints(List<TelemetryEntry> entries,
                                                                   List<Double> framePts,
                                                                   double videoDuration,
                                                                   CaptureTimeline captureTimeline) {
        if (entries == null || entries.isEmpty()) {
            return List.of();
        }
        List<EntryGroup> groups = new ArrayList<>();
        for (TelemetryEntry entry : entries) {
            if (entry == null || entry.samples() == null || entry.samples().isEmpty()) {
                continue;
            }
            groups.add(new EntryGroup(entry.packetPts(), entry.samples()));
        }
        if (groups.isEmpty()) {
            return List.of();
        }

        List<Double> entryPts = new ArrayList<>();
        for (EntryGroup group : groups) {
            if (group.packetPts() != null && Double.isFinite(group.packetPts())) {
                entryPts.add(group.packetPts());
            }
        }
        Double medianPacketDelta = medianPositiveDelta(entryPts);
        if (medianPacketDelta == null || medianPacketDelta <= 0.0d) {
            medianPacketDelta = fallbackUniformSampleInterval(groups, framePts, videoDuration);
        }

        List<SignalSamplePoint> out = new ArrayList<>();
        int totalSampleCount = groups.stream().mapToInt(g -> g.samples().size()).sum();
        if ("capture_timeline".equals(captureTimeline.mode()) && !captureTimeline.relativeFrameTimes().isEmpty()) {
            if (captureTimeline.relativeFrameTimes().size() == totalSampleCount) {
                int globalIndex = 0;
                for (EntryGroup group : groups) {
                    for (double[] sample : group.samples()) {
                        out.add(new SignalSamplePoint(captureTimeline.relativeFrameTimes().get(globalIndex), sample, globalIndex));
                        globalIndex++;
                    }
                }
                return out;
            }
            return buildCaptureAlignedSignalSamplePoints(groups,
                    framePts,
                    videoDuration,
                    captureTimeline.relativeFrameTimes(),
                    medianPacketDelta);
        }

        int globalIndex = 0;
        for (int groupIndex = 0; groupIndex < groups.size(); groupIndex++) {
            EntryGroup group = groups.get(groupIndex);
            int sampleCount = group.samples().size();
            if (sampleCount <= 0) {
                continue;
            }
            Double startPts = group.packetPts();
            Double nextPts = nextPacketPts(groups, groupIndex + 1);
            if (startPts == null || !Double.isFinite(startPts)) {
                startPts = globalIndex == 0 ? 0.0d : out.get(out.size() - 1).pts();
            }
            double interval = medianPacketDelta;
            if (nextPts != null && nextPts > startPts) {
                interval = (nextPts - startPts) / Math.max(sampleCount, 1);
            }
            if (!Double.isFinite(interval) || interval <= 0.0d) {
                interval = fallbackUniformSampleInterval(groups, framePts, videoDuration);
            }
            for (int sampleIndex = 0; sampleIndex < sampleCount; sampleIndex++) {
                double samplePts = startPts + sampleIndex * interval;
                out.add(new SignalSamplePoint(samplePts, group.samples().get(sampleIndex), globalIndex));
                globalIndex++;
            }
        }
        out.sort(Comparator.comparingDouble(SignalSamplePoint::pts));
        return out;
    }

    private static CaptureTimeline deriveCaptureTimeline(DecodedTelemetry telemetry,
                                                         List<Double> normalizedFramePts,
                                                         List<String> warnings) {
        if (normalizedFramePts.isEmpty()) {
            return new CaptureTimeline("pts", List.of());
        }
        List<TelemetryEntry> gps9Entries = telemetry.signals().get("GPS9");
        int frameCount = normalizedFramePts.size();
        int gps9SampleCount = sampleCount(gps9Entries);

        Double framePtsSpacing = medianPositiveDelta(normalizedFramePts);
        Double gps9AbsoluteSpacing = medianGps9AbsoluteDelta(gps9Entries);
        if (framePtsSpacing == null || gps9AbsoluteSpacing == null) {
            return new CaptureTimeline("pts", normalizedFramePts);
        }
        if (!(gps9AbsoluteSpacing > framePtsSpacing * TIMELAPSE_CADENCE_RATIO_THRESHOLD)) {
            return new CaptureTimeline("pts", normalizedFramePts);
        }

        List<Double> relativeTimes = new ArrayList<>(frameCount);
        for (int i = 0; i < frameCount; i++) {
            relativeTimes.add(i * gps9AbsoluteSpacing);
        }
        if (gps9SampleCount != frameCount) {
            warnings.add(String.format(Locale.ROOT,
                    "Timelapse-like capture cadence detected with GPS9/frame mismatch (%d GPS9 samples vs %d frames): using capture timeline %.6fs instead of playback PTS spacing %.6fs.",
                    gps9SampleCount,
                    frameCount,
                    gps9AbsoluteSpacing,
                    framePtsSpacing));
        } else {
            warnings.add(String.format(Locale.ROOT,
                    "Timelapse-like capture cadence detected: using capture timeline %.6fs instead of playback PTS spacing %.6fs.",
                    gps9AbsoluteSpacing,
                    framePtsSpacing));
        }
        return new CaptureTimeline("capture_timeline", List.copyOf(relativeTimes));
    }

    private static int sampleCount(List<TelemetryEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return 0;
        }
        int count = 0;
        for (TelemetryEntry entry : entries) {
            if (entry != null && entry.samples() != null) {
                count += entry.samples().size();
            }
        }
        return count;
    }

    private static Double medianGps9AbsoluteDelta(List<TelemetryEntry> gps9Entries) {
        if (gps9Entries == null || gps9Entries.isEmpty()) {
            return null;
        }
        List<Double> absoluteTimesSeconds = new ArrayList<>();
        for (TelemetryEntry entry : gps9Entries) {
            if (entry == null || entry.samples() == null) {
                continue;
            }
            for (double[] sample : entry.samples()) {
                Double absoluteMillis = gps9AbsoluteMillis(sample);
                GnssFix fix = gnssFix(sample);
                if (absoluteMillis == null || !isPlausibleAbsoluteTime(absoluteMillis.longValue()) || !isValidGnssFix(fix)) {
                    continue;
                }
                absoluteTimesSeconds.add(absoluteMillis / 1000.0d);
            }
        }
        return medianPositiveDelta(absoluteTimesSeconds);
    }

    private static Double nextPacketPts(List<EntryGroup> groups, int startIndex) {
        for (int i = startIndex; i < groups.size(); i++) {
            Double pts = groups.get(i).packetPts();
            if (pts != null && Double.isFinite(pts)) {
                return pts;
            }
        }
        return null;
    }

    private static List<SignalSamplePoint> buildCaptureAlignedSignalSamplePoints(List<EntryGroup> groups,
                                                                                  List<Double> framePts,
                                                                                  double videoDuration,
                                                                                  List<Double> captureRelativeTimes,
                                                                                  Double medianPacketDelta) {
        List<SignalSamplePoint> out = new ArrayList<>();
        int globalIndex = 0;
        for (int groupIndex = 0; groupIndex < groups.size(); groupIndex++) {
            EntryGroup group = groups.get(groupIndex);
            int sampleCount = group.samples().size();
            if (sampleCount <= 0) {
                continue;
            }
            Double startPts = group.packetPts();
            Double nextPts = nextPacketPts(groups, groupIndex + 1);
            if (startPts == null || !Double.isFinite(startPts)) {
                startPts = globalIndex == 0 ? 0.0d : out.get(out.size() - 1).pts();
            }
            double interval = medianPacketDelta == null ? 0.0d : medianPacketDelta;
            if (nextPts != null && nextPts > startPts) {
                interval = (nextPts - startPts) / Math.max(sampleCount, 1);
            }
            if (!Double.isFinite(interval) || interval <= 0.0d) {
                interval = fallbackUniformSampleInterval(groups, framePts, videoDuration);
            }
            for (int sampleIndex = 0; sampleIndex < sampleCount; sampleIndex++) {
                double samplePlaybackPts = startPts + sampleIndex * interval;
                double sampleCaptureTime = mapPlaybackPtsToCaptureTime(framePts, captureRelativeTimes, samplePlaybackPts);
                out.add(new SignalSamplePoint(sampleCaptureTime, group.samples().get(sampleIndex), globalIndex));
                globalIndex++;
            }
        }
        out.sort(Comparator.comparingDouble(SignalSamplePoint::pts));
        return out;
    }

    private static double mapPlaybackPtsToCaptureTime(List<Double> framePts,
                                                      List<Double> captureRelativeTimes,
                                                      double samplePlaybackPts) {
        if (captureRelativeTimes.isEmpty()) {
            return samplePlaybackPts;
        }
        if (framePts.isEmpty() || framePts.size() != captureRelativeTimes.size()) {
            int frameIndex = Math.max(0, Math.min(captureRelativeTimes.size() - 1, (int) Math.round(samplePlaybackPts)));
            return captureRelativeTimes.get(frameIndex);
        }

        int lastFrameIndex = framePts.size() - 1;
        if (samplePlaybackPts <= framePts.get(0)) {
            return captureRelativeTimes.get(0);
        }
        if (samplePlaybackPts >= framePts.get(lastFrameIndex)) {
            return captureRelativeTimes.get(lastFrameIndex);
        }

        int hit = Collections.binarySearch(framePts, samplePlaybackPts);
        if (hit >= 0) {
            return captureRelativeTimes.get(hit);
        }

        int insertionPoint = -hit - 1;
        int lowerIndex = Math.max(0, insertionPoint - 1);
        int upperIndex = Math.min(lastFrameIndex, insertionPoint);
        double lowerPts = framePts.get(lowerIndex);
        double upperPts = framePts.get(upperIndex);
        double lowerTime = captureRelativeTimes.get(lowerIndex);
        double upperTime = captureRelativeTimes.get(upperIndex);
        if (!(upperPts > lowerPts)) {
            return lowerTime;
        }
        double fraction = (samplePlaybackPts - lowerPts) / (upperPts - lowerPts);
        if (!Double.isFinite(fraction)) {
            fraction = 0.0d;
        }
        fraction = Math.max(0.0d, Math.min(1.0d, fraction));
        return lowerTime + fraction * (upperTime - lowerTime);
    }

    private static double fallbackUniformSampleInterval(List<EntryGroup> groups,
                                                        List<Double> framePts,
                                                        double videoDuration) {
        int sampleCount = 0;
        for (EntryGroup group : groups) {
            sampleCount += group.samples().size();
        }
        if (sampleCount <= 1) {
            return 0.0d;
        }
        if (framePts.size() == sampleCount) {
            double total = framePts.get(framePts.size() - 1) - framePts.get(0);
            return total <= 0.0d ? 0.0d : total / (sampleCount - 1);
        }
        return videoDuration <= 0.0d ? 0.0d : videoDuration / (sampleCount - 1);
    }

    private static AnchorCluster selectTrustedGnssAnchor(List<SignalSamplePoint> gps9) {
        List<AnchorMember> candidates = new ArrayList<>();
        for (SignalSamplePoint point : gps9) {
            if (point.values() == null || point.values().length <= 8) {
                continue;
            }
            Double absoluteMillis = gps9AbsoluteMillis(point.values());
            if (absoluteMillis == null || !isPlausibleAbsoluteTime(absoluteMillis.longValue())) {
                continue;
            }
            GnssFix fix = gnssFix(point.values());
            if (fix == null || !isValidGnssFix(fix)) {
                continue;
            }
            long offsetMillis = Math.round(absoluteMillis - point.pts() * 1000.0d);
            candidates.add(new AnchorMember(point.pts(), absoluteMillis.longValue(), offsetMillis));
        }
        if (candidates.isEmpty()) {
            return null;
        }

        AnchorCluster best = null;
        for (AnchorMember seed : candidates) {
            List<AnchorMember> clusterMembers = new ArrayList<>();
            for (AnchorMember candidate : candidates) {
                if (Math.abs(candidate.offsetMillis() - seed.offsetMillis()) <= TRUSTED_OFFSET_CLUSTER_TOLERANCE_MS) {
                    clusterMembers.add(candidate);
                }
            }
            if (clusterMembers.size() < MIN_TRUSTED_ANCHOR_COUNT) {
                continue;
            }
            long clusterOffset = medianOffset(clusterMembers);
            double earliestPts = clusterMembers.stream().mapToDouble(AnchorMember::pts).min().orElse(0.0d);
            AnchorCluster cluster = new AnchorCluster(clusterOffset, earliestPts, List.copyOf(clusterMembers));
            if (best == null
                    || cluster.members().size() > best.members().size()
                    || (cluster.members().size() == best.members().size() && cluster.earliestPts() < best.earliestPts())) {
                best = cluster;
            }
        }
        return best;
    }

    private static long medianOffset(List<AnchorMember> members) {
        List<Long> offsets = new ArrayList<>(members.size());
        for (AnchorMember member : members) {
            offsets.add(member.offsetMillis());
        }
        Collections.sort(offsets);
        return offsets.get(offsets.size() / 2);
    }

    private static Integer firstTrustedFrameIndex(List<Double> framePts, double earliestTrustedPts) {
        for (int i = 0; i < framePts.size(); i++) {
            if (framePts.get(i) + 1e-9 >= earliestTrustedPts) {
                return i;
            }
        }
        return framePts.isEmpty() ? null : framePts.size() - 1;
    }

    private static String confidenceLabel(int anchorCount) {
        if (anchorCount >= 5) {
            return "high";
        }
        if (anchorCount >= 3) {
            return "medium";
        }
        if (anchorCount >= 1) {
            return "low";
        }
        return "none";
    }

    private static MedianSpacing medianSpacing(List<SignalSamplePoint> points) {
        List<Double> pts = new ArrayList<>(points.size());
        for (SignalSamplePoint point : points) {
            pts.add(point.pts());
        }
        Double spacing = medianPositiveDelta(pts);
        return new MedianSpacing(spacing == null ? null : spacing);
    }

    private static Double medianPositiveDelta(List<Double> values) {
        if (values == null || values.size() < 2) {
            return null;
        }
        List<Double> deltas = new ArrayList<>(values.size() - 1);
        for (int i = 1; i < values.size(); i++) {
            double delta = values.get(i) - values.get(i - 1);
            if (Double.isFinite(delta) && delta > 0.0d) {
                deltas.add(delta);
            }
        }
        if (deltas.isEmpty()) {
            return null;
        }
        Collections.sort(deltas);
        return deltas.get(deltas.size() / 2);
    }

    private static GnssFix nearestGnssFix(List<SignalSamplePoint> gps9, MedianSpacing spacing, double framePts) {
        double maxDistance = spacing.seconds() != null && spacing.seconds() > 0.0d
                ? spacing.seconds() * 0.5d
                : 0.5d;
        GnssFix bestFix = null;
        double bestDistance = Double.POSITIVE_INFINITY;
        for (SignalSamplePoint point : gps9) {
            GnssFix fix = gnssFix(point.values());
            if (!isValidGnssFix(fix)) {
                continue;
            }
            double distance = Math.abs(point.pts() - framePts);
            if (distance <= maxDistance && distance < bestDistance) {
                bestFix = fix;
                bestDistance = distance;
            }
        }
        return bestFix;
    }

    private static FrameVector3 nearestVector(List<SignalSamplePoint> points, double framePts) {
        SignalSamplePoint point = nearestPoint(points, framePts);
        if (point == null || point.values() == null || point.values().length < 3) {
            return null;
        }
        return new FrameVector3(point.values()[0], point.values()[1], point.values()[2]);
    }

    private static SignalSamplePoint nearestPoint(List<SignalSamplePoint> points, double framePts) {
        if (points == null || points.isEmpty()) {
            return null;
        }
        SignalSamplePoint best = null;
        double bestDistance = Double.POSITIVE_INFINITY;
        for (SignalSamplePoint point : points) {
            double distance = Math.abs(point.pts() - framePts);
            if (distance < bestDistance) {
                best = point;
                bestDistance = distance;
            }
        }
        return best;
    }

    private static GnssFix gnssFix(double[] sample) {
        if (sample == null || sample.length < 3) {
            return null;
        }
        Double quality = sample.length > 7 && Double.isFinite(sample[7]) ? sample[7] : null;
        Integer fixType = sample.length > 8 && Double.isFinite(sample[8]) ? (int) Math.round(sample[8]) : null;
        return new GnssFix(sample[0], sample[1], sample[2], quality, fixType);
    }

    private static boolean isValidGnssFix(GnssFix fix) {
        if (fix == null || fix.latitude() == null || fix.longitude() == null) {
            return false;
        }
        double latitude = fix.latitude();
        double longitude = fix.longitude();
        if (!Double.isFinite(latitude) || !Double.isFinite(longitude)) {
            return false;
        }
        if (Math.abs(latitude) > 90.0d || Math.abs(longitude) > 180.0d) {
            return false;
        }
        if (Math.abs(latitude) < 1e-9 && Math.abs(longitude) < 1e-9) {
            return false;
        }
        if (fix.fixType() != null && fix.fixType() < 2) {
            return false;
        }
        return fix.quality() == null || !Double.isFinite(fix.quality()) || fix.quality() < 99.0d;
    }

    private static Double gps9AbsoluteMillis(double[] sample) {
        if (sample == null || sample.length <= 6) {
            return null;
        }
        Double day = sample[5];
        Double secondOfDay = sample[6];
        if (day == null || secondOfDay == null || !Double.isFinite(day) || !Double.isFinite(secondOfDay)) {
            return null;
        }
        long dayIndex = Math.round(day);
        if (dayIndex < 0L || secondOfDay < 0.0d || secondOfDay >= 172800.0d) {
            return null;
        }
        // GPS9 absolute time tuple conversion follows the official GoPro telemetry convention:
        // day offset from 2000-01-01 plus seconds-of-day.
        return (double) (GPS_EPOCH_MILLIS + Math.round((dayIndex * 86400.0d + secondOfDay) * 1000.0d));
    }

    private static boolean isPlausibleAbsoluteTime(long millis) {
        try {
            ZonedDateTime utc = Instant.ofEpochMilli(millis).atZone(ZoneOffset.UTC);
            int year = utc.getYear();
            return year >= 2020 && year <= 2035;
        } catch (Exception ex) {
            return false;
        }
    }

    private static String stripExtension(String fileName) {
        int dot = fileName.lastIndexOf('.');
        return dot > 0 ? fileName.substring(0, dot) : fileName;
    }

    private record EntryGroup(Double packetPts, List<double[]> samples) {
    }

    private record SignalSamplePoint(double pts, double[] values, int sourceIndex) {
    }

    private record AnchorMember(double pts, long absoluteMillis, long offsetMillis) {
    }

    private record AnchorCluster(long offsetMillis, double earliestPts, List<AnchorMember> members) {
    }

    private record MedianSpacing(Double seconds) {
    }

    private record CaptureTimeline(String mode, List<Double> relativeFrameTimes) {
    }
}
