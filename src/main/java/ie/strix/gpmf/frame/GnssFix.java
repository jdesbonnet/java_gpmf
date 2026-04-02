package ie.strix.gpmf.frame;

public record GnssFix(
        Double latitude,
        Double longitude,
        Double altitude,
        Double quality,
        Integer fixType) {
}
