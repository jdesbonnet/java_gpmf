package ie.strix.gpmf.model;

import java.util.List;

public record TelemetryEntry(
        String key,
        String streamName,
        String typeDescriptor,
        double[] scales,
        String units,
        List<double[]> samples,
        Double packetPts) {

    public TelemetryEntry(
            String key,
            String streamName,
            String typeDescriptor,
            double[] scales,
            String units,
            List<double[]> samples) {
        this(key, streamName, typeDescriptor, scales, units, samples, null);
    }
}
