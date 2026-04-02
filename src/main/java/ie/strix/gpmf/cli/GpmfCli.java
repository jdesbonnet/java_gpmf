package ie.strix.gpmf.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import ie.strix.gpmf.frame.FrameMetadata;
import ie.strix.gpmf.frame.FrameMetadataExtractor;
import ie.strix.gpmf.frame.FrameMetadataSeries;
import ie.strix.gpmf.frame.FrameVector3;
import ie.strix.gpmf.frame.GnssFix;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class GpmfCli {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private GpmfCli() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || hasFlag(args, "--help") || hasFlag(args, "-h")) {
            printUsage();
            return;
        }

        String command = args[0];
        if (!"frames-json".equals(command)) {
            throw new IllegalArgumentException("Unknown command: " + command);
        }

        Path input = requiredPathArg(args, "--input");
        Path output = requiredPathArg(args, "--output");

        FrameMetadataSeries series = new FrameMetadataExtractor().extract(input);
        List<FrameRecord> records = new ArrayList<>(series.frames().size());
        for (FrameMetadata frame : series.frames()) {
            records.add(FrameRecord.from(frame));
        }

        Path outputParent = output.toAbsolutePath().getParent();
        if (outputParent != null) {
            Files.createDirectories(outputParent);
        }
        MAPPER.writeValue(output.toFile(), records);

        System.out.println("frames_json_complete videoId=" + series.videoId()
                + " frames=" + records.size()
                + " output=" + output.toAbsolutePath());
    }

    private static boolean hasFlag(String[] args, String flag) {
        for (String arg : args) {
            if (flag.equals(arg)) {
                return true;
            }
        }
        return false;
    }

    private static Path requiredPathArg(String[] args, String name) {
        for (int i = 1; i < args.length - 1; i++) {
            if (name.equals(args[i])) {
                return Path.of(args[i + 1]);
            }
        }
        throw new IllegalArgumentException("Missing required argument: " + name);
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  gpmf frames-json --input <video> --output <frames.json>");
        System.out.println();
        System.out.println("Example:");
        System.out.println("  java -jar target/gopro-gpmf-0.1.0-SNAPSHOT-all.jar frames-json \\");
        System.out.println("    --input /path/to/GS010199.360 \\");
        System.out.println("    --output /tmp/GS010199_frames.json");
    }

    public record GnssRecord(
            Double latitude,
            Double longitude,
            Double altitude,
            Double quality,
            Integer fixType) {

        static GnssRecord from(GnssFix gps9) {
            if (gps9 == null) {
                return null;
            }
            return new GnssRecord(gps9.latitude(), gps9.longitude(), gps9.altitude(), gps9.quality(), gps9.fixType());
        }
    }

    public record VectorRecord(Double x, Double y, Double z) {

        static VectorRecord from(FrameVector3 vector) {
            if (vector == null) {
                return null;
            }
            return new VectorRecord(vector.x(), vector.y(), vector.z());
        }
    }

    public record FrameRecord(
            Integer frameIndex,
            Double pts,
            Double relativeTime,
            String timeUtc,
            String timeUtcMode,
            String timeUtcConfidence,
            GnssRecord gps9,
            VectorRecord mnor,
            VectorRecord grav,
            Double magneticHeading,
            Double magneticPitch,
            Double gravityRoll,
            Double gravityPitch) {

        static FrameRecord from(FrameMetadata frame) {
            FrameVector3 mnor = frame.mnor();
            FrameVector3 grav = frame.grav();
            return new FrameRecord(
                    frame.frameIndex(),
                    frame.pts(),
                    frame.relativeTime(),
                    frame.timeUtc() == null ? null : frame.timeUtc().toString(),
                    frame.timeUtcMode(),
                    frame.timeUtcConfidence(),
                    GnssRecord.from(frame.gps9()),
                    VectorRecord.from(mnor),
                    VectorRecord.from(grav),
                    deriveMagneticHeading(mnor),
                    deriveMagneticPitch(mnor),
                    deriveGravityRoll(grav),
                    deriveGravityPitch(grav));
        }
    }

    // Example angle convention used in Purlieu demo tooling. Other projects may choose
    // different axis conventions, so raw MNOR/GRAV vectors remain the primary outputs.
    private static Double deriveMagneticHeading(FrameVector3 mnor) {
        if (mnor == null) {
            return null;
        }
        return Math.atan2(mnor.z(), mnor.x()) * 180.0d / Math.PI - 180.0d;
    }

    private static Double deriveMagneticPitch(FrameVector3 mnor) {
        if (mnor == null) {
            return null;
        }
        return Math.atan(mnor.y() / mnor.z()) * 180.0d / Math.PI;
    }

    // Example GRAV convention assuming [x, -z, -y] when converting to view-level roll/pitch.
    private static Double deriveGravityRoll(FrameVector3 grav) {
        if (grav == null) {
            return null;
        }
        return Math.atan2(-grav.x(), grav.y()) * 180.0d / Math.PI;
    }

    private static Double deriveGravityPitch(FrameVector3 grav) {
        if (grav == null) {
            return null;
        }
        return Math.atan2(grav.z(), grav.y()) * 180.0d / Math.PI;
    }
}
