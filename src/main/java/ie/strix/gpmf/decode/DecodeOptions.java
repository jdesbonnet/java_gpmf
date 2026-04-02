package ie.strix.gpmf.decode;

import ie.strix.gpmf.model.BackendMode;

public record DecodeOptions(BackendMode backendMode, boolean verbose) {

    public DecodeOptions {
        backendMode = backendMode == null ? BackendMode.AUTO : backendMode;
    }

    public static DecodeOptions defaults() {
        return new DecodeOptions(BackendMode.AUTO, false);
    }
}
