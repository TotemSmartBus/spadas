package edu.whu.similarity;

public class R2 {
    public static double distance(double[] p, double[] a, double[] b) {
        double A = p[0] - a[0];
        double B = p[1] - a[1];
        double C = b[0] - a[0];
        double D = b[1] - a[1];

        var dot = A * C + B * D;
        var len_sq = C * C + D * D;
        var param = -1.0;
        if (len_sq != 0) //in case of 0 length line
            param = dot / len_sq;

        double xx, yy;

        if (param < 0) {
            xx = a[0];
            yy = a[1];
        } else if (param > 1) {
            xx = b[0];
            yy = b[1];
        } else {
            xx = a[0] + param * C;
            yy = a[1] + param * D;
        }

        var dx = p[0] - xx;
        var dy = p[1] - yy;
        return Math.sqrt(dx * dx + dy * dy);
    }
}
