package ie.strix.gpmf.model;

import java.util.List;
import java.util.Map;

public record DecodedTelemetry(
        Map<String, List<TelemetryEntry>> signals,
        ExtractionProvenance provenance) {
}
