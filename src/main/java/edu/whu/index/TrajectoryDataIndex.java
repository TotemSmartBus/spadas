package edu.whu.index;

import edu.whu.structure.Trajectory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * datasetID -> trajectory list
 */
@Component
public class TrajectoryDataIndex extends HashMap<Integer, ArrayList<Trajectory>> {
}


