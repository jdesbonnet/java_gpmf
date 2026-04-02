package ie.strix.gpmf.model;

import java.nio.file.Path;

public enum CameraModel {
    AUTO("auto"),
    HERO11_BLACK("HERO11 Black"),
    MAX2("MAX2");

    private final String legacyLabel;

    CameraModel(String legacyLabel) {
        this.legacyLabel = legacyLabel;
    }

    public String legacyLabel() {
        return legacyLabel;
    }

    public static CameraModel parse(String raw) {
        if (raw == null || raw.isBlank() || "auto".equalsIgnoreCase(raw.trim())) {
            return AUTO;
        }
        String normalized = raw.trim().replace('-', '_').replace(' ', '_').toUpperCase(java.util.Locale.ROOT);
        return CameraModel.valueOf(normalized);
    }

    public static CameraModel resolve(Path videoFile, CameraModel requested) {
        if (requested != null && requested != AUTO) {
            return requested;
        }
        String name = videoFile == null ? "" : videoFile.getFileName().toString().toLowerCase(java.util.Locale.ROOT);
        if (name.endsWith(".360")) {
            return MAX2;
        }
        return HERO11_BLACK;
    }
}
