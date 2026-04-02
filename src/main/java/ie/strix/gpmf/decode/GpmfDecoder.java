package ie.strix.gpmf.decode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ie.strix.gpmf.klv.GpmfParser;
import ie.strix.gpmf.model.BackendMode;
import ie.strix.gpmf.model.DecodedTelemetry;
import ie.strix.gpmf.model.ExtractionProvenance;
import ie.strix.gpmf.model.TelemetryEntry;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class GpmfDecoder {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String KEY_DEVC = "DEVC";
    private static final String KEY_STRM = "STRM";
    private static final String KEY_STNM = "STNM";
    private static final String KEY_SCAL = "SCAL";
    private static final String KEY_TYPE = "TYPE";
    private static final String KEY_UNIT = "UNIT";
    private static final String KEY_SIUN = "SIUN";
    private static final String KEY_GPSU = "GPSU";

    public DecodedTelemetry decode(Path videoFile) throws IOException, InterruptedException {
        return decode(videoFile, DecodeOptions.defaults());
    }

    public DecodedTelemetry decode(Path videoFile, DecodeOptions options) throws IOException, InterruptedException {
        Objects.requireNonNull(videoFile, "videoFile");
        options = options == null ? DecodeOptions.defaults() : options;

        List<String> warnings = new ArrayList<>();
        ProbeMetadata probe = probeVideo(videoFile, warnings);
        BackendMode requestedMode = options.backendMode();
        Map<String, List<TelemetryEntry>> decoded = Map.of();
        String backendUsed = "none";
        boolean usedPacketSizes = false;
        boolean usedMp4BoxFallback = false;

        if (requestedMode != BackendMode.MP4BOX) {
            if (probe.gpmdStreamIndex() != null) {
                List<GpmdPacketInfo> packets = readGpmdPackets(videoFile, probe.gpmdStreamIndex(), warnings);
                if (!packets.isEmpty()) {
                    decoded = decodeUsingPacketSizes(videoFile, probe.gpmdStreamIndex(), packets, warnings);
                    if (!decoded.isEmpty()) {
                        backendUsed = "ffmpeg_packetized";
                        usedPacketSizes = true;
                    }
                }
                if (decoded.isEmpty()) {
                    decoded = decodeFromFullGpmdStream(videoFile, probe.gpmdStreamIndex(), warnings);
                    if (!decoded.isEmpty()) {
                        backendUsed = "ffmpeg_full_stream";
                    }
                }
            } else {
                warnings.add("No gpmd stream index could be identified by ffprobe.");
            }
        }

        if (decoded.isEmpty() && probe.gpmdTrackId() != null && requestedMode != BackendMode.FFMPEG) {
            decoded = decodeViaMp4BoxRawTrack(videoFile, probe.gpmdTrackId(), warnings);
            if (!decoded.isEmpty()) {
                backendUsed = "mp4box_raw_track";
                usedMp4BoxFallback = true;
            }
        }

        if (decoded.isEmpty()) {
            warnings.add("No supported telemetry signals were decoded.");
        }

        Map<String, Integer> signalEntryCounts = new LinkedHashMap<>();
        Map<String, Integer> signalSampleCounts = new LinkedHashMap<>();
        for (Map.Entry<String, List<TelemetryEntry>> signal : decoded.entrySet()) {
            signalEntryCounts.put(signal.getKey(), signal.getValue().size());
            int sampleCount = 0;
            for (TelemetryEntry entry : signal.getValue()) {
                if (entry != null && entry.samples() != null) {
                    sampleCount += entry.samples().size();
                }
            }
            signalSampleCounts.put(signal.getKey(), sampleCount);
        }

        ExtractionProvenance provenance = new ExtractionProvenance(
                requestedMode.name().toLowerCase(Locale.ROOT),
                backendUsed,
                stripExtension(videoFile.getFileName().toString()),
                probe.gpmdStreamIndex(),
                probe.gpmdTrackId(),
                usedPacketSizes,
                usedMp4BoxFallback,
                probe.frameRate(),
                probe.videoFrameCount(),
                probe.videoDurationSeconds(),
                probe.imageWidth(),
                probe.imageHeight(),
                Collections.unmodifiableMap(signalEntryCounts),
                Collections.unmodifiableMap(signalSampleCounts),
                List.copyOf(warnings));

        Map<String, List<TelemetryEntry>> immutableSignals = new LinkedHashMap<>();
        decoded.forEach((k, v) -> immutableSignals.put(k, List.copyOf(v)));
        return new DecodedTelemetry(Collections.unmodifiableMap(immutableSignals), provenance);
    }

    private static Map<String, List<TelemetryEntry>> decodeUsingPacketSizes(Path videoFile,
                                                                             int streamIndex,
                                                                             List<GpmdPacketInfo> packets,
                                                                             List<String> warnings)
            throws IOException, InterruptedException {
        Map<String, List<TelemetryEntry>> bySignal = new LinkedHashMap<>();
        GpmfParser.Options options = new GpmfParser.Options();
        options.capturePayload = true;
        options.parseNested = true;

        try (MetadataPipe pipe = openGpmdAsDataInputStream(videoFile, streamIndex)) {
            InputStream in = pipe.in();
            for (int packetIndex = 0; packetIndex < packets.size(); packetIndex++) {
                GpmdPacketInfo packet = packets.get(packetIndex);
                int packetSize = packet.size();
                if (packetSize <= 0) {
                    continue;
                }
                byte[] packetBytes = readExactly(in, packetSize);
                if (packetBytes.length != packetSize) {
                    warnings.add("Short GPMF packet read at index " + packetIndex
                            + " expected=" + packetSize + " actual=" + packetBytes.length);
                    break;
                }
                try {
                    List<GpmfParser.Entry> root = GpmfParser.parseToTree(new ByteArrayInputStream(packetBytes), options);
                    decodeRootEntries(root, bySignal, packet.pts());
                } catch (Exception ex) {
                    warnings.add("Unable to decode packet " + packetIndex + ": " + ex.getMessage());
                }
            }
        }

        return immutableSignalMap(bySignal);
    }

    private static Map<String, List<TelemetryEntry>> decodeFromFullGpmdStream(Path videoFile,
                                                                               int streamIndex,
                                                                               List<String> warnings)
            throws IOException, InterruptedException {
        Map<String, List<TelemetryEntry>> bySignal = new LinkedHashMap<>();
        GpmfParser.Options options = new GpmfParser.Options();
        options.capturePayload = true;
        options.parseNested = true;

        try (MetadataPipe pipe = openGpmdAsDataInputStream(videoFile, streamIndex)) {
            byte[] payload = pipe.in().readAllBytes();
            if (payload.length == 0) {
                return Map.of();
            }
            try {
                List<GpmfParser.Entry> root = GpmfParser.parseToTree(new ByteArrayInputStream(payload), options);
                decodeRootEntries(root, bySignal, null);
            } catch (Exception ex) {
                warnings.add("Unable to decode full gpmd stream: " + ex.getMessage());
            }
        }
        return immutableSignalMap(bySignal);
    }

    private static Map<String, List<TelemetryEntry>> decodeViaMp4BoxRawTrack(Path videoFile,
                                                                              int trackId,
                                                                              List<String> warnings) {
        if (!isExecutableOnPath("MP4Box")) {
            warnings.add("MP4Box not found on PATH; raw-track fallback unavailable.");
            return Map.of();
        }

        File outFile = null;
        try {
            outFile = Files.createTempFile("gpmd_track_", ".bin").toFile();
            List<String> cmd = List.of(
                    "MP4Box",
                    "-raw", String.valueOf(trackId),
                    videoFile.toAbsolutePath().toString(),
                    "-out", outFile.getAbsolutePath());
            ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
            pb.environment().put("HOME", "/tmp");
            Process p = pb.start();
            String output;
            try (InputStream is = p.getInputStream()) {
                output = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }
            int exit = p.waitFor();
            if (exit != 0) {
                warnings.add("MP4Box raw extraction failed exit=" + exit + " trackId=" + trackId + " output=" + output.trim());
                return Map.of();
            }
            byte[] payload = Files.readAllBytes(outFile.toPath());
            if (payload.length == 0) {
                return Map.of();
            }
            return decodeTelemetryPayload(payload, warnings);
        } catch (Exception ex) {
            warnings.add("MP4Box raw extraction exception: " + ex.getMessage());
            return Map.of();
        } finally {
            if (outFile != null && outFile.exists()) {
                //noinspection ResultOfMethodCallIgnored
                outFile.delete();
            }
        }
    }

    private static Map<String, List<TelemetryEntry>> decodeTelemetryPayload(byte[] payload, List<String> warnings) {
        Map<String, List<TelemetryEntry>> bySignal = new LinkedHashMap<>();
        GpmfParser.Options options = new GpmfParser.Options();
        options.capturePayload = true;
        options.parseNested = true;
        try {
            List<GpmfParser.Entry> root = GpmfParser.parseToTree(new ByteArrayInputStream(payload), options);
            decodeRootEntries(root, bySignal, null);
        } catch (Exception ex) {
            warnings.add("Unable to decode gpmd payload of size " + payload.length + ": " + ex.getMessage());
        }
        return immutableSignalMap(bySignal);
    }

    private static void decodeRootEntries(List<GpmfParser.Entry> root,
                                          Map<String, List<TelemetryEntry>> sink,
                                          Double packetPts) {
        for (GpmfParser.Entry entry : root) {
            if (KEY_DEVC.equals(entry.fourCC)) {
                decodeDevice(entry, sink, packetPts);
            }
        }
    }

    private static void decodeDevice(GpmfParser.Entry devc,
                                     Map<String, List<TelemetryEntry>> sink,
                                     Double packetPts) {
        if (devc.children == null) {
            return;
        }
        for (GpmfParser.Entry child : devc.children) {
            if (!KEY_STRM.equals(child.fourCC) || child.children == null) {
                continue;
            }
            decodeStream(child.children, sink, packetPts);
        }
    }

    private static void decodeStream(List<GpmfParser.Entry> entries,
                                     Map<String, List<TelemetryEntry>> sink,
                                     Double packetPts) {
        StreamContext ctx = StreamContext.empty();
        for (GpmfParser.Entry entry : entries) {
            if (entry.payload == null || entry.payload.length == 0) {
                continue;
            }
            switch (entry.fourCC) {
                case KEY_STNM -> ctx = ctx.withName(readAscii(entry.payload));
                case KEY_TYPE -> ctx = ctx.withType(readAscii(entry.payload));
                case KEY_SCAL -> ctx = ctx.withScales(decodeNumericEntry(entry, null));
                case KEY_UNIT, KEY_SIUN -> ctx = ctx.withUnits(readAscii(entry.payload));
                default -> {
                    if (!isLikelyMetadataDescriptor(entry.fourCC)) {
                        List<double[]> samples = KEY_GPSU.equals(entry.fourCC)
                                ? decodeGpsuSamples(entry)
                                : decodeSignalSamples(entry, ctx);
                        if (!samples.isEmpty()) {
                            sink.computeIfAbsent(entry.fourCC, key -> new ArrayList<>()).add(new TelemetryEntry(
                                    entry.fourCC,
                                    ctx.streamName(),
                                    ctx.typeDescriptor(),
                                    ctx.scales(),
                                    ctx.units(),
                                    samples,
                                    packetPts));
                        }
                    }
                }
            }
        }
    }

    private static boolean isLikelyMetadataDescriptor(String fourCC) {
        return KEY_DEVC.equals(fourCC)
                || KEY_STRM.equals(fourCC)
                || KEY_STNM.equals(fourCC)
                || KEY_TYPE.equals(fourCC)
                || KEY_SCAL.equals(fourCC)
                || KEY_UNIT.equals(fourCC)
                || KEY_SIUN.equals(fourCC);
    }

    private static List<double[]> decodeSignalSamples(GpmfParser.Entry entry, StreamContext ctx) {
        if (entry.payload == null || entry.payload.length == 0) {
            return List.of();
        }
        int type = entry.type;
        if (type == '?') {
            if (ctx.typeDescriptor() == null || ctx.typeDescriptor().isBlank()) {
                return List.of();
            }
            return decodeComplexSamples(entry.payload, ctx.typeDescriptor(), entry.repeat, ctx.scales());
        }

        double[] flattened = decodeNumericEntry(entry, ctx.scales());
        if (flattened.length == 0) {
            return List.of();
        }

        int elementSize = primitiveSize(type);
        int elementsPerSample = (entry.structSize > 0 && elementSize > 0) ? entry.structSize / elementSize : 0;
        if (elementsPerSample <= 0) {
            elementsPerSample = flattened.length;
        }

        List<double[]> out = new ArrayList<>();
        for (int i = 0; i < flattened.length; i += elementsPerSample) {
            int remaining = Math.min(elementsPerSample, flattened.length - i);
            double[] sample = new double[remaining];
            System.arraycopy(flattened, i, sample, 0, remaining);
            out.add(sample);
        }
        return out;
    }

    private static List<double[]> decodeGpsuSamples(GpmfParser.Entry entry) {
        if (entry.payload == null || entry.payload.length == 0) {
            return List.of();
        }
        int chunkSize = entry.structSize > 0 ? entry.structSize : 16;
        List<double[]> out = new ArrayList<>();
        for (int off = 0; off + chunkSize <= entry.payload.length; off += chunkSize) {
            byte[] chunk = Arrays.copyOfRange(entry.payload, off, off + chunkSize);
            String raw = readAscii(chunk);
            Long epochMillis = parseGpsuUtcMillis(raw);
            if (epochMillis != null) {
                out.add(new double[] { epochMillis.doubleValue() });
            }
        }
        return out;
    }

    private static Long parseGpsuUtcMillis(String raw) {
        if (raw == null) {
            return null;
        }
        String s = raw.trim();
        if (s.length() < 12) {
            return null;
        }
        try {
            int yy = Integer.parseInt(s.substring(0, 2));
            int year = (yy >= 80) ? (1900 + yy) : (2000 + yy);
            int month = Integer.parseInt(s.substring(2, 4));
            int day = Integer.parseInt(s.substring(4, 6));
            int hour = Integer.parseInt(s.substring(6, 8));
            int minute = Integer.parseInt(s.substring(8, 10));
            int second = Integer.parseInt(s.substring(10, 12));

            int millis = 0;
            int dot = s.indexOf('.');
            if (dot >= 0 && dot + 1 < s.length()) {
                String frac = s.substring(dot + 1).replaceAll("[^0-9]", "");
                if (!frac.isEmpty()) {
                    if (frac.length() > 3) {
                        frac = frac.substring(0, 3);
                    } else if (frac.length() == 1) {
                        frac = frac + "00";
                    } else if (frac.length() == 2) {
                        frac = frac + "0";
                    }
                    millis = Integer.parseInt(frac);
                }
            }

            LocalDateTime dt = LocalDateTime.of(year, month, day, hour, minute, second, millis * 1_000_000);
            return dt.toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (Exception ex) {
            return null;
        }
    }

    private static List<double[]> decodeComplexSamples(byte[] payload, String typeDescriptor, int repeat, double[] scales) {
        String types = expandTypeDescriptor(typeDescriptor);
        if (types.isBlank()) {
            return List.of();
        }

        int bytesPerSample = 0;
        for (int i = 0; i < types.length(); i++) {
            bytesPerSample += primitiveSize(types.charAt(i));
        }
        if (bytesPerSample <= 0 || payload.length < bytesPerSample) {
            return List.of();
        }

        int sampleCount = Math.max(1, Math.min(repeat, payload.length / bytesPerSample));
        List<double[]> out = new ArrayList<>(sampleCount);
        ByteBuffer bb = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        for (int sampleIndex = 0; sampleIndex < sampleCount; sampleIndex++) {
            double[] values = new double[types.length()];
            for (int valueIndex = 0; valueIndex < types.length(); valueIndex++) {
                values[valueIndex] = readPrimitive(bb, types.charAt(valueIndex));
            }
            applyScales(values, scales);
            out.add(values);
        }
        return out;
    }

    private static double[] decodeNumericEntry(GpmfParser.Entry entry, double[] scales) {
        if (entry.payload == null || entry.payload.length == 0) {
            return new double[0];
        }
        int size = primitiveSize(entry.type);
        if (size <= 0 || (entry.payload.length % size) != 0) {
            return new double[0];
        }
        ByteBuffer bb = ByteBuffer.wrap(entry.payload).order(ByteOrder.BIG_ENDIAN);
        int count = entry.payload.length / size;
        double[] values = new double[count];
        for (int i = 0; i < count; i++) {
            values[i] = readPrimitive(bb, (char) entry.type);
        }
        applyScales(values, scales);
        return values;
    }

    private static void applyScales(double[] values, double[] scales) {
        if (scales == null || scales.length == 0) {
            return;
        }
        for (int i = 0; i < values.length; i++) {
            double divisor = scales[Math.min(i, scales.length - 1)];
            if (divisor != 0.0d) {
                values[i] = values[i] / divisor;
            }
        }
    }

    private static int primitiveSize(int type) {
        return switch (type) {
            case 'b', 'B', 'c' -> 1;
            case 's', 'S' -> 2;
            case 'l', 'L', 'f', 'q' -> 4;
            case 'j', 'J', 'd', 'Q' -> 8;
            default -> -1;
        };
    }

    private static double readPrimitive(ByteBuffer bb, char type) {
        return switch (type) {
            case 'b' -> bb.get();
            case 'B' -> Byte.toUnsignedInt(bb.get());
            case 's' -> bb.getShort();
            case 'S' -> Short.toUnsignedInt(bb.getShort());
            case 'l', 'q' -> bb.getInt();
            case 'L' -> Integer.toUnsignedLong(bb.getInt());
            case 'j', 'Q' -> bb.getLong();
            case 'J' -> Long.parseUnsignedLong(Long.toUnsignedString(bb.getLong()));
            case 'f' -> bb.getFloat();
            case 'd' -> bb.getDouble();
            case 'c' -> bb.get();
            default -> Double.NaN;
        };
    }

    private static String expandTypeDescriptor(String raw) {
        String src = raw == null ? "" : raw.trim();
        if (src.isEmpty()) {
            return "";
        }
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < src.length(); i++) {
            char c = src.charAt(i);
            if (Character.isWhitespace(c)) {
                continue;
            }
            if (i + 1 < src.length() && src.charAt(i + 1) == '[') {
                int close = src.indexOf(']', i + 2);
                if (close > i + 2) {
                    int times = Integer.parseInt(src.substring(i + 2, close));
                    out.append(String.valueOf(c).repeat(Math.max(0, times)));
                    i = close;
                    continue;
                }
            }
            out.append(c);
        }
        return out.toString();
    }

    private static String readAscii(byte[] bytes) {
        int end = 0;
        while (end < bytes.length && bytes[end] != 0) {
            end++;
        }
        return new String(bytes, 0, end, StandardCharsets.US_ASCII).trim();
    }

    private static MetadataPipe openGpmdAsDataInputStream(Path videoFile, int dataStreamIndex)
            throws IOException {
        List<String> cmd = List.of(
                "ffmpeg", "-v", "error",
                "-i", videoFile.toAbsolutePath().toString(),
                "-map", "0:" + dataStreamIndex,
                "-c", "copy",
                "-f", "data", "pipe:1");
        Process process = new ProcessBuilder(cmd)
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start();
        DataInputStream dis = new DataInputStream(new BufferedInputStream(process.getInputStream(), 1 << 20));
        return new MetadataPipe(process, dis);
    }

    private static List<GpmdPacketInfo> readGpmdPackets(Path videoFile, int streamIndex, List<String> warnings)
            throws IOException, InterruptedException {
        List<String> cmd = List.of(
                "ffprobe", "-v", "error",
                "-select_streams", Integer.toString(streamIndex),
                "-show_packets",
                "-show_entries", "packet=size,pts_time",
                "-print_format", "json",
                videoFile.toAbsolutePath().toString());

        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String json;
        try (InputStream is = p.getInputStream()) {
            json = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
        int code = p.waitFor();
        if (code != 0) {
            warnings.add("ffprobe packet scan failed with exit code " + code);
            return List.of();
        }

        JsonNode root = MAPPER.readTree(json);
        JsonNode packets = root.get("packets");
        if (packets == null || !packets.isArray()) {
            return List.of();
        }

        List<GpmdPacketInfo> packetsOut = new ArrayList<>();
        for (JsonNode packet : packets) {
            int size = packet.path("size").asInt(0);
            if (size > 0) {
                Double pts = packet.path("pts_time").isMissingNode() ? null : packet.path("pts_time").asDouble();
                if (pts != null && !Double.isFinite(pts)) {
                    pts = null;
                }
                packetsOut.add(new GpmdPacketInfo(size, pts));
            }
        }
        return packetsOut;
    }

    private static ProbeMetadata probeVideo(Path videoFile, List<String> warnings) throws IOException, InterruptedException {
        List<String> cmd = List.of(
                "ffprobe", "-v", "error",
                "-print_format", "json",
                "-show_streams",
                videoFile.toAbsolutePath().toString());

        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String json;
        try (InputStream is = p.getInputStream()) {
            json = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
        int code = p.waitFor();
        if (code != 0) {
            throw new IOException("ffprobe failed with exit code " + code);
        }

        JsonNode root = MAPPER.readTree(json);
        JsonNode streams = root.get("streams");
        if (streams == null || !streams.isArray()) {
            return new ProbeMetadata(null, null, null, null, null, null, null);
        }

        Integer fallbackFirstData = null;
        Integer bestStreamIndex = null;
        Integer bestTrackId = null;
        int bestScore = Integer.MIN_VALUE;
        Double frameRate = null;
        Integer videoFrameCount = null;
        Double videoDurationSeconds = null;
        Integer width = null;
        Integer height = null;

        for (JsonNode s : streams) {
            int index = s.path("index").asInt(-1);
            String codecType = safeLower(text(s, "codec_type"));
            String codecTagString = safeLower(text(s, "codec_tag_string"));
            String codecName = safeLower(text(s, "codec_name"));
            String codecTagHex = safeLower(text(s, "codec_tag"));
            JsonNode tags = s.get("tags");
            String handlerName = tags != null ? safeLower(text(tags, "handler_name")) : "";

            if ("video".equals(codecType) && frameRate == null) {
                frameRate = parseFrameRate(text(s, "avg_frame_rate"));
                if (frameRate == null || frameRate <= 0.0d) {
                    frameRate = parseFrameRate(text(s, "r_frame_rate"));
                }
                videoFrameCount = parseInteger(text(s, "nb_frames"));
                videoDurationSeconds = parseDouble(text(s, "duration"));
                width = s.path("width").isMissingNode() ? null : s.path("width").asInt();
                height = s.path("height").isMissingNode() ? null : s.path("height").asInt();
            }

            boolean isData = "data".equals(codecType);
            if (isData && fallbackFirstData == null && index >= 0) {
                fallbackFirstData = index;
            }

            int score = 0;
            if (isData) score += 100;
            if ("gpmd".equals(codecTagString)) score += 1000;
            if ("gpmd".equals(codecName)) score += 900;
            if ("bin_data".equals(codecName)) score += 80;
            if ("0x646d7067".equals(codecTagHex)) score += 850;
            if (handlerName.contains("gopro met") || handlerName.contains("metadata")) score += 700;
            if (handlerName.contains("gopro tcd") || "tmcd".equals(codecTagString)) score -= 700;

            Integer trackId = parseIsoTrackId(text(s, "id"));
            if (score > bestScore) {
                bestScore = score;
                bestStreamIndex = index >= 0 ? index : null;
                bestTrackId = trackId;
            }
        }

        Integer gpmdStreamIndex = (bestScore >= 300) ? bestStreamIndex : fallbackFirstData;
        if (gpmdStreamIndex == null) {
            warnings.add("No data stream looked like gpmd; gpmdStreamIndex unresolved.");
        }
        return new ProbeMetadata(gpmdStreamIndex, bestTrackId, frameRate, videoFrameCount, videoDurationSeconds, width, height);
    }

    private static Double parseFrameRate(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.contains("/")) {
            String[] parts = trimmed.split("/");
            if (parts.length == 2) {
                try {
                    double numerator = Double.parseDouble(parts[0]);
                    double denominator = Double.parseDouble(parts[1]);
                    if (denominator != 0.0d) {
                        return numerator / denominator;
                    }
                } catch (NumberFormatException ignored) {
                }
            }
            return null;
        }
        try {
            return Double.parseDouble(trimmed);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static Integer parseIsoTrackId(String idText) {
        if (idText == null || idText.isBlank()) {
            return null;
        }
        String s = idText.trim().toLowerCase(Locale.ROOT);
        try {
            if (s.startsWith("0x")) {
                return Integer.parseInt(s.substring(2), 16);
            }
            return Integer.parseInt(s);
        } catch (Exception ex) {
            return null;
        }
    }

    private static Integer parseInteger(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static Double parseDouble(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            double value = Double.parseDouble(raw.trim());
            return Double.isFinite(value) ? value : null;
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static String safeLower(String value) {
        return value == null ? "" : value.toLowerCase(Locale.ROOT);
    }

    private static String text(JsonNode node, String field) {
        if (node == null || field == null) {
            return null;
        }
        JsonNode child = node.get(field);
        if (child == null || child.isNull()) {
            return null;
        }
        return child.asText();
    }

    private static byte[] readExactly(InputStream in, int size) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(size);
        byte[] buf = new byte[8192];
        int remaining = size;
        while (remaining > 0) {
            int n = in.read(buf, 0, Math.min(buf.length, remaining));
            if (n < 0) {
                break;
            }
            out.write(buf, 0, n);
            remaining -= n;
        }
        return out.toByteArray();
    }

    private static boolean isExecutableOnPath(String executable) {
        String path = System.getenv("PATH");
        if (path == null || path.isBlank()) {
            return false;
        }
        for (String dir : path.split(File.pathSeparator)) {
            if (dir == null || dir.isBlank()) {
                continue;
            }
            Path candidate = Path.of(dir, executable);
            if (Files.isRegularFile(candidate) && Files.isExecutable(candidate)) {
                return true;
            }
        }
        return false;
    }

    private static Map<String, List<TelemetryEntry>> immutableSignalMap(Map<String, List<TelemetryEntry>> signals) {
        Map<String, List<TelemetryEntry>> out = new LinkedHashMap<>();
        signals.forEach((k, v) -> out.put(k, List.copyOf(v)));
        return Collections.unmodifiableMap(out);
    }

    private static String stripExtension(String fileName) {
        int dot = fileName.lastIndexOf('.');
        return dot > 0 ? fileName.substring(0, dot) : fileName;
    }

    private record ProbeMetadata(Integer gpmdStreamIndex,
                                 Integer gpmdTrackId,
                                 Double frameRate,
                                 Integer videoFrameCount,
                                 Double videoDurationSeconds,
                                 Integer imageWidth,
                                 Integer imageHeight) {
    }

    private record StreamContext(String streamName,
                                 String typeDescriptor,
                                 double[] scales,
                                 String units) {
        static StreamContext empty() {
            return new StreamContext(null, null, null, null);
        }

        StreamContext withName(String value) {
            return new StreamContext(value, typeDescriptor, scales, units);
        }

        StreamContext withType(String value) {
            return new StreamContext(streamName, value, scales, units);
        }

        StreamContext withScales(double[] value) {
            return new StreamContext(streamName, typeDescriptor, value, units);
        }

        StreamContext withUnits(String value) {
            return new StreamContext(streamName, typeDescriptor, scales, value);
        }
    }

    private record MetadataPipe(Process process, DataInputStream in) implements Closeable {
        @Override
        public void close() throws IOException {
            try {
                in.close();
            } finally {
                process.destroy();
            }
        }
    }

    private record GpmdPacketInfo(int size, Double pts) {
    }
}
