package edu.whu.index;

import edu.whu.structure.DatasetID;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;

/**
 * 三重映射关系的索引结构
 * datasetID -> trajectory list
 * trajectoryID -> point list
 * point -> [lat, lng]
 */
@Component
public class TrajectoryGridIndex extends HashMap<DatasetID, HashMap<Integer, List<Integer>>[][]> {
}
