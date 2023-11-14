package edu.whu.findroad;

import edu.whu.index.GeoEncoder;
import org.junit.Test;

public class PureEncodeTest {
    @Test
    public void encodeTest() {
        double lat_min = 0.0;
        double lat_max = 100.0;
        double lng_min = 0.0;
        double lng_max = 100.0;
        GeoEncoder encoder = new GeoEncoder(lat_min, lng_min, lat_max, lng_max, 2, 3);
        encoder.assertGeoGridIndex(new double[]{49.9, 33.3}, new int[]{0, 0});
        encoder.assertGeoGridIndex(new double[]{49.0, 50.0}, new int[]{0, 1});
        encoder.assertGeoGridIndex(new double[]{75.0, 50.0}, new int[]{1, 1});
        encoder.assertGeoGridIndex(new double[]{100.0, 100.0}, new int[]{1, 2});
    }
}
