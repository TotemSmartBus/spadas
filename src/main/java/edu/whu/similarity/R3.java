package edu.whu.similarity;

public class R3 {
    public final double x;
    public final double y;
    public final double z;

    public final static R3 O = new R3(0.0, 0.0, 0.0);
    public final static R3 I = new R3(1.0, 0.0, 0.0);
    public final static R3 J = new R3(0.0, 1.0, 0.0);
    public final static R3 K = new R3(0.0, 0.0, 1.0);

    public R3(double x, double y, double z) {
        super();

        this.x = x;
        this.y = y;
        this.z = z;
    }

    public static R3 add(final R3 a, final R3 b) {
        return new R3(a.x + b.x, a.y + b.y, a.z + b.z);
    }

    public static R3 sub(final R3 a, final R3 b) {
        return new R3(a.x - b.x, a.y - b.y, a.z - b.z);
    }


    public static R3 scalar(final double k, final R3 v) {
        return new R3(k * v.x, k * v.y, k * v.z);
    }

    public static double dot(final R3 a, final R3 b) {
        return a.x * b.x + a.y * b.y + a.z * b.z;
    }

    public static R3 cross(final R3 a, final R3 b) {
        return new R3(a.y * b.z - a.z * b.y
                , a.z * b.x - a.x * b.z
                , a.x * b.y - a.y * b.x
        );
    }

    public static double modulus(final R3 v) {
        return Math.sqrt(dot(v, v));
    }

    public static R3 versor(final R3 v) {
        final double m = modulus(v);

        if (m == 0.0)
            return O;

        return scalar(1.0 / m, v);
    }

    /**
     * Calculates the euclidean distance between two points.
     *
     * @param a first point
     * @param b second point
     * @return the distance between a and b
     * @author Afonso Santos
     */
    public static double distance(final R3 a, final R3 b) {
        return b.sub(a).modulus();
    }

    /**
     * Calculates the shortest euclidean distance from a point to a line segment.
     *
     * @param v the point
     * @param a start of line segment
     * @param b end of line segment
     * @return the shortest distance from v to line segment [a,b]
     * @author Afonso Santos
     */
    public static double distanceToSegment(final R3 v, final R3 a, final R3 b) {
        final R3 ab = b.sub(a);

        {
            final R3 av = v.sub(a);

            if (av.dot(ab) <= 0.0)                // Point is lagging behind start of the segment, so perpendicular distance is not viable.
                return av.modulus();            // Use straight line distance to start of segment instead.
        }

        {
            final R3 bv = v.sub(b);

            if (bv.dot(ab) >= 1.0)                // Point is advanced past the end of the segment, so perpendicular distance is not viable.
                return bv.modulus();            // Use straight line distance to end of the segment instead.
        }

        // Point is within the segment's start/finish boundaries, so perpendicular distance is viable.
        return (ab.cross(a.sub(v))).modulus() / ab.modulus();        // Perpendicular distance of point to segment.
    }

    /**
     * Calculates the shortest distance from a point to a path.
     *
     * @param v    the point from with the distance is measured
     * @param path the array of points wich, when sequentialy joined by line segments, form a path
     * @return the shortest distance from v to any of the path forming line segments
     * @author Afonso Santos
     */
    public static double distanceToPath(final R3 v, final R3[] path) {
        double minDistance = Double.MAX_VALUE;

        for (int pathPointIdx = 1; pathPointIdx < path.length; ++pathPointIdx) {
            final double d = R3.distanceToSegment(v, path[pathPointIdx - 1], path[pathPointIdx]);

            if (d < minDistance)
                minDistance = d;
        }

        return minDistance;
    }

    public R3 add(final R3 v) {
        return add(this, v);
    }

    public R3 cross(final R3 v) {
        return cross(this, v);
    }

    /**
     * Calculates the euclidean distance to a point.
     *
     * @param v the point to witch the distance is calculated
     * @return the distance between this point and v
     * @author Afonso Santos
     */
    public double distance(final R3 v) {
        return distance(this, v);
    }

    /**
     * Calculates the shortest distance to a path.
     *
     * @param path the array of points wich, when sequentialy joined by line segments, form a path
     * @return the shortest distance from this point to any of the path forming line segments
     * @author Afonso Santos
     */
    public double distanceToPath(final R3[] path) {
        return distanceToPath(this, path);
    }

    /**
     * Calculates the shortest distance to a line segment.
     *
     * @param a start of line segment
     * @param b end of line segment
     * @return the shortest distance from this point to line segment [a,b]
     * @author Afonso Santos
     */
    public double distanceToSegment(final R3 a, final R3 b) {
        return distanceToSegment(this, a, b);
    }

    public double dot(final R3 v) {
        return dot(this, v);
    }


    public double modulus() {
        return modulus(this);
    }

    public R3 scalar(final double k) {
        return scalar(k, this);
    }

    public R3 sub(final R3 v) {
        return sub(this, v);
    }

    public R3 versor() {
        return versor(this);
    }
}
