package edu.whu.index;

import org.junit.Assert;


/**
 * 将一个地理空间范围编码为数字的index
 */
public class GeoEncoder {

    // 线性 encode & decode 的参数
    private double lat_a = 0.0;
    private double lat_b = 0.0;

    private double lng_a = 0.0;
    private double lng_b = 0.0;

    private double lat_min = 0.0;
    private double lat_max = 0.0;
    private double lng_min = 0.0;
    private double lng_max = 0.0;
    private int lat_count = 0;
    private int lng_count = 0;

    public GeoEncoder(double lat_min, double lng_min, double lat_max, double lng_max, int lat_count, int lng_count) {
        this.lat_min = lat_min;
        this.lat_max = lat_max;
        this.lng_min = lng_min;
        this.lng_max = lng_max;
        this.lat_count = lat_count;
        this.lng_count = lng_count;
        lat_a = (lat_max - lat_min) / lat_count;
        lat_b = lat_min;
        lng_a = (lng_max - lng_min) / lng_count;
        lng_b = lng_min;
    }

    public int encodeLat(double x) {
        if (x < lat_min || x > lat_max) {
            throw new RuntimeException("Lat out of bound");
        }
        if (x == lat_max) {
            return lat_count - 1;
        }
        return (int) Math.floor((x - lat_b) / lat_a);
    }

    public int encodeLng(double x) {
        if (x < lng_min || x > lng_max) {
            throw new RuntimeException("Lng out of bound");
        }
        if (x == lng_max) {
            return lng_count - 1;
        }
        return (int) Math.floor((x - lng_b) / lng_a);
    }

    public int[] encode(double[] point) {
        return new int[]{
                encodeLat(point[0]), encodeLng(point[1])
        };
    }

    public void assertGeoGridIndex(double[] point, int[] indexes) {
        int[] result = encode(point);
        Assert.assertEquals(result[0], indexes[0]);
        Assert.assertEquals(result[1], indexes[1]);
    }

}
