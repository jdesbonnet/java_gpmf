package ie.strix.gpmf.frame;

import ie.strix.gpmf.model.DecodedTelemetry;
import ie.strix.gpmf.model.ExtractionProvenance;
import ie.strix.gpmf.model.TelemetryEntry;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class FrameMetadataExtractorTest {

    @Test
    void buildUsesLaterTrustedGnssClusterAndRetrogradesEarlierFrames() {
        Map<String, List<TelemetryEntry>> signals = new LinkedHashMap<>();
        signals.put("GPS9", List.of(
                gps9Entry(0.0d, gps9Sample(53.0d, -8.0d, 50.0d, utc("2016-01-01T00:00:00Z"), 1.0d, 3.0d)),
                gps9Entry(2.0d, gps9Sample(53.1d, -8.1d, 51.0d, utc("2026-01-01T00:00:02Z"), 1.0d, 3.0d)),
                gps9Entry(3.0d, gps9Sample(53.2d, -8.2d, 52.0d, utc("2026-01-01T00:00:03Z"), 1.0d, 3.0d)),
                gps9Entry(4.0d, gps9Sample(53.3d, -8.3d, 53.0d, utc("2026-01-01T00:00:04Z"), 1.0d, 3.0d))));
        signals.put("MNOR", List.of(
                vectorEntry("MNOR", 0.0d, 0.0d, 0.1d, 0.2d),
                vectorEntry("MNOR", 1.0d, 1.0d, 1.1d, 1.2d),
                vectorEntry("MNOR", 2.0d, 2.0d, 2.1d, 2.2d),
                vectorEntry("MNOR", 3.0d, 3.0d, 3.1d, 3.2d),
                vectorEntry("MNOR", 4.0d, 4.0d, 4.1d, 4.2d)));
        signals.put("GRAV", List.of(
                vectorEntry("GRAV", 0.0d, 10.0d, 10.1d, 10.2d),
                vectorEntry("GRAV", 1.0d, 11.0d, 11.1d, 11.2d),
                vectorEntry("GRAV", 2.0d, 12.0d, 12.1d, 12.2d),
                vectorEntry("GRAV", 3.0d, 13.0d, 13.1d, 13.2d),
                vectorEntry("GRAV", 4.0d, 14.0d, 14.1d, 14.2d)));

        DecodedTelemetry telemetry = new DecodedTelemetry(
                signals,
                new ExtractionProvenance(
                        "auto",
                        "ffmpeg_packetized",
                        "GS999999",
                        2,
                        3,
                        true,
                        false,
                        1.0d,
                        5,
                        4.0d,
                        4000,
                        3000,
                        Map.of("GPS9", 4, "MNOR", 5, "GRAV", 5),
                        Map.of("GPS9", 4, "MNOR", 5, "GRAV", 5),
                        List.of()));

        FrameMetadataSeries series = new FrameMetadataExtractor().build(
                Path.of("GS999999.360"),
                telemetry,
                List.of(0.0d, 1.0d, 2.0d, 3.0d, 4.0d));

        assertEquals("gps9_pts_anchored", series.timeUtcMode());
        assertEquals("medium", series.timeUtcConfidence());
        assertEquals(3, series.trustedGnssAnchorCount());
        assertEquals(2, series.firstTrustedFrameIndex());
        assertEquals(5, series.frames().size());

        FrameMetadata frame0 = series.frames().get(0);
        assertEquals("retrograde_extrapolated", frame0.timeUtcMode());
        assertEquals("2026-01-01T00:00:00Z", frame0.timeUtc().toString());
        assertNotNull(frame0.gps9());
        assertEquals(53.0d, frame0.gps9().latitude());
        assertNotNull(frame0.mnor());
        assertNotNull(frame0.grav());

        FrameMetadata frame2 = series.frames().get(2);
        assertEquals("gnss_anchored", frame2.timeUtcMode());
        assertEquals("2026-01-01T00:00:02Z", frame2.timeUtc().toString());
        assertNotNull(frame2.gps9());
        assertEquals(53.1d, frame2.gps9().latitude());
    }

    @Test
    void buildUsesCaptureTimelineWhenGpsCadenceProvesTimelapse() {
        Map<String, List<TelemetryEntry>> signals = new LinkedHashMap<>();
        signals.put("GPS9", List.of(
                gps9Entry(0.0d, gps9Sample(53.0d, -8.0d, 50.0d, utc("2026-01-01T00:00:00Z"), 1.0d, 3.0d)),
                gps9Entry(0.033d, gps9Sample(53.0001d, -8.0001d, 50.1d, utc("2026-01-01T00:00:00.500Z"), 1.0d, 3.0d)),
                gps9Entry(0.067d, gps9Sample(53.0002d, -8.0002d, 50.2d, utc("2026-01-01T00:00:01Z"), 1.0d, 3.0d)),
                gps9Entry(0.100d, gps9Sample(53.0003d, -8.0003d, 50.3d, utc("2026-01-01T00:00:01.500Z"), 1.0d, 3.0d))));
        signals.put("MNOR", List.of(
                vectorEntry("MNOR", 0.0d, 0.0d, 0.1d, 0.2d),
                vectorEntry("MNOR", 0.033d, 1.0d, 1.1d, 1.2d),
                vectorEntry("MNOR", 0.067d, 2.0d, 2.1d, 2.2d),
                vectorEntry("MNOR", 0.100d, 3.0d, 3.1d, 3.2d)));
        signals.put("GRAV", List.of(
                vectorEntry("GRAV", 0.0d, 10.0d, 10.1d, 10.2d),
                vectorEntry("GRAV", 0.033d, 11.0d, 11.1d, 11.2d),
                vectorEntry("GRAV", 0.067d, 12.0d, 12.1d, 12.2d),
                vectorEntry("GRAV", 0.100d, 13.0d, 13.1d, 13.2d)));

        DecodedTelemetry telemetry = new DecodedTelemetry(
                signals,
                new ExtractionProvenance(
                        "auto",
                        "ffmpeg_packetized",
                        "GS_TIMELAPSE",
                        2,
                        3,
                        true,
                        false,
                        29.97d,
                        4,
                        0.100d,
                        5888,
                        1920,
                        Map.of("GPS9", 4, "MNOR", 4, "GRAV", 4),
                        Map.of("GPS9", 4, "MNOR", 4, "GRAV", 4),
                        List.of()));

        FrameMetadataSeries series = new FrameMetadataExtractor().build(
                Path.of("GS_TIMELAPSE.360"),
                telemetry,
                List.of(0.0d, 0.033d, 0.067d, 0.100d));

        assertEquals("gps9_capture_timeline_anchored", series.timeUtcMode());
        assertEquals(4, series.trustedGnssAnchorCount());
        assertEquals(0, series.firstTrustedFrameIndex());
        assertEquals(4, series.frames().size());

        FrameMetadata frame0 = series.frames().get(0);
        assertEquals(0.0d, frame0.pts());
        assertEquals(0.0d, frame0.relativeTime());
        assertEquals("2026-01-01T00:00:00Z", frame0.timeUtc().toString());

        FrameMetadata frame1 = series.frames().get(1);
        assertEquals(0.033d, frame1.pts());
        assertEquals(0.5d, frame1.relativeTime());
        assertEquals("2026-01-01T00:00:00.500Z", frame1.timeUtc().toString());

        FrameMetadata frame3 = series.frames().get(3);
        assertEquals(0.100d, frame3.pts());
        assertEquals(1.5d, frame3.relativeTime());
        assertEquals("2026-01-01T00:00:01.500Z", frame3.timeUtc().toString());
    }

    @Test
    void buildUsesCaptureTimelineWithOneOffGps9FrameMismatch() {
        Map<String, List<TelemetryEntry>> signals = new LinkedHashMap<>();
        signals.put("GPS9", List.of(
                gps9Entry(0.0d, gps9Sample(53.0d, -8.0d, 50.0d, utc("2026-01-01T00:00:00Z"), 1.0d, 3.0d)),
                gps9Entry(0.033d, gps9Sample(53.0001d, -8.0001d, 50.1d, utc("2026-01-01T00:00:00.500Z"), 1.0d, 3.0d)),
                gps9Entry(0.067d, gps9Sample(53.0002d, -8.0002d, 50.2d, utc("2026-01-01T00:00:01Z"), 1.0d, 3.0d)),
                gps9Entry(0.100d, gps9Sample(53.0003d, -8.0003d, 50.3d, utc("2026-01-01T00:00:01.500Z"), 1.0d, 3.0d)),
                gps9Entry(0.133d, gps9Sample(53.0004d, -8.0004d, 50.4d, utc("2026-01-01T00:00:02Z"), 1.0d, 3.0d))));
        signals.put("MNOR", List.of(
                vectorEntry("MNOR", 0.0d, 0.0d, 0.1d, 0.2d),
                vectorEntry("MNOR", 0.033d, 1.0d, 1.1d, 1.2d),
                vectorEntry("MNOR", 0.067d, 2.0d, 2.1d, 2.2d),
                vectorEntry("MNOR", 0.100d, 3.0d, 3.1d, 3.2d)));
        signals.put("GRAV", List.of(
                vectorEntry("GRAV", 0.0d, 10.0d, 10.1d, 10.2d),
                vectorEntry("GRAV", 0.033d, 11.0d, 11.1d, 11.2d),
                vectorEntry("GRAV", 0.067d, 12.0d, 12.1d, 12.2d),
                vectorEntry("GRAV", 0.100d, 13.0d, 13.1d, 13.2d)));

        DecodedTelemetry telemetry = new DecodedTelemetry(
                signals,
                new ExtractionProvenance(
                        "auto",
                        "ffmpeg_packetized",
                        "GS_TIMELAPSE_MISMATCH",
                        2,
                        3,
                        true,
                        false,
                        29.97d,
                        4,
                        0.100d,
                        5888,
                        1920,
                        Map.of("GPS9", 5, "MNOR", 4, "GRAV", 4),
                        Map.of("GPS9", 5, "MNOR", 4, "GRAV", 4),
                        List.of()));

        FrameMetadataSeries series = new FrameMetadataExtractor().build(
                Path.of("GS_TIMELAPSE_MISMATCH.360"),
                telemetry,
                List.of(0.0d, 0.033d, 0.067d, 0.100d));

        assertEquals("gps9_capture_timeline_anchored", series.timeUtcMode());
        assertEquals(4, series.frames().size());

        FrameMetadata frame0 = series.frames().get(0);
        assertEquals(0.0d, frame0.pts());
        assertEquals(0.0d, frame0.relativeTime());
        assertEquals("2026-01-01T00:00:00Z", frame0.timeUtc().toString());

        FrameMetadata frame1 = series.frames().get(1);
        assertEquals(0.033d, frame1.pts());
        assertEquals(0.5d, frame1.relativeTime());
        assertEquals("2026-01-01T00:00:00.500Z", frame1.timeUtc().toString());

        FrameMetadata frame3 = series.frames().get(3);
        assertEquals(0.100d, frame3.pts());
        assertEquals(1.5d, frame3.relativeTime());
        assertEquals("2026-01-01T00:00:01.500Z", frame3.timeUtc().toString());
    }


    @Test
    void approximateFramePtsUsesCheapStreamProvenance() {
        DecodedTelemetry telemetry = new DecodedTelemetry(
                Map.of(),
                new ExtractionProvenance(
                        "auto",
                        "ffmpeg_packetized",
                        "GS_FAST",
                        2,
                        3,
                        true,
                        false,
                        29.97d,
                        4,
                        0.100d,
                        5888,
                        1920,
                        Map.of(),
                        Map.of(),
                        List.of()));

        List<Double> framePts = FrameMetadataExtractor.approximateFramePts(telemetry);
        assertEquals(4, framePts.size());
        assertEquals(0.0d, framePts.get(0));
        assertEquals(1.0d / 29.97d, framePts.get(1), 1e-9);
        assertEquals(3.0d / 29.97d, framePts.get(3), 1e-9);
    }

    private static TelemetryEntry gps9Entry(double packetPts, double[] sample) {
        return new TelemetryEntry("GPS9", "GPS", null, null, null, List.of(sample), packetPts);
    }

    private static TelemetryEntry vectorEntry(String key, double packetPts, double x, double y, double z) {
        return new TelemetryEntry(key, key, null, null, null, List.of(new double[] {x, y, z}), packetPts);
    }

    private static double[] gps9Sample(double lat,
                                       double lon,
                                       double altitude,
                                       Instant utc,
                                       double quality,
                                       double fixType) {
        long gpsEpochMillis = Instant.parse("2000-01-01T00:00:00Z").toEpochMilli();
        long deltaMillis = utc.toEpochMilli() - gpsEpochMillis;
        long dayIndex = Math.floorDiv(deltaMillis, 86_400_000L);
        long secondOfDayMillis = Math.floorMod(deltaMillis, 86_400_000L);
        double secondOfDay = secondOfDayMillis / 1000.0d;
        return new double[] {
                lat,
                lon,
                altitude,
                0.0d,
                0.0d,
                dayIndex,
                secondOfDay,
                quality,
                fixType
        };
    }

    private static Instant utc(String value) {
        return Instant.parse(value);
    }
}
