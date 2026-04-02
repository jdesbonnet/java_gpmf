package ie.strix.gpmf.model;

public enum BackendMode {
    AUTO,
    FFMPEG,
    MP4BOX;

    public static BackendMode parse(String raw) {
        if (raw == null || raw.isBlank()) {
            return AUTO;
        }
        return BackendMode.valueOf(raw.trim().toUpperCase(java.util.Locale.ROOT));
    }
}
