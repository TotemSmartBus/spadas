package web.VO;

import lombok.Data;

import java.util.List;

@Data
public class RoadMatchPair {
    private int pointID;

    private double[] point;

    private int roadID;

    private List<double[]> roadPoints;

    private double distance;
}
