package edu.whu.index;

import lombok.Data;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;

/**
 * NOTICE: Only support single index now
 * An index for grid encoded range, currently we set the new york city roadmap for the trajectory map index
 */
@Component
@Data
public class GridTrajectoryIndex {
    /**
     * spatial split number for lat and lng
     */
    private int[] spatialSplit;
    /**
     * spatial range for lat and lng
     */
    private double[] spatialRange;
    /**
     * real index in this spatial region
     */
    private HashMap<Integer, List<Integer>>[][] value;

    /**
     * the origin encoder for the spatial dataset
     */
    private GeoEncoder getEncoder;

}
