package edu.whu.structure;

import lombok.Data;

@Data
public class GeoPoint {
    public GeoPoint(double[] point) {
        if (point.length < 2) {
            throw new RuntimeException("Not a value point value.");
        }
        this.values = new double[]{point[0], point[1]};
        this.lat = point[0];
        this.lng = point[1];
    }

    private double[] values;

    private double lat;
    private double lng;
}
