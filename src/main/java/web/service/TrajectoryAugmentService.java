package web.service;

import edu.nyu.dss.similarity.Framework;
import edu.whu.index.GridTrajectoryIndex;
import edu.whu.index.TrajectoryDataIndex;
import edu.whu.similarity.R2;
import edu.whu.structure.DatasetID;
import edu.whu.structure.Trajectory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import web.VO.RoadMatchPair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Service
@Slf4j
public class TrajectoryAugmentService {

    @Autowired
    private Framework framework;

    @Autowired
    private TrajectoryDataIndex trajectoryDataIndex;

    @Autowired
    private GridTrajectoryIndex gridTrajectoryIndex;

    @Autowired
    public TrajectoryAugmentOptions options;


    public List<RoadMatchPair> findNearestRoad(DatasetID datasetID) {
        double[][] pointDataset = framework.dataMapPorto.get(datasetID.get());
        if (pointDataset == null) {
            log.warn("query dataset is not exist");
            return new ArrayList<>();
        }
        List<RoadMatchPair> results = new ArrayList<>();
        for (int i = 0; i < pointDataset.length; i++) {
            double[] point = pointDataset[i];
            try {
                RoadMatchPair result = findNearestRoadWithGrid(point);
                result.setPoint(point);
                result.setPointID(i);
                results.add(result);
                log.debug("find nearest road for [{}, {}]: distance={}", point[0], point[1], result.getDistance());
            } catch (RuntimeException e) {
                RoadMatchPair result = new RoadMatchPair();
                result.setPoint(point);
                result.setPointID(i);
                result.setDistance(-1.0);
                result.setRoadPoints(new ArrayList<>());
                result.setRoadID(-1);
                results.add(result);
                log.warn(e.getMessage());
            }
        }
        return results;
    }

    public RoadMatchPair findNearestRoadWithGrid(double[] point) throws IndexOutOfBoundsException {
        int[] position;
        RoadMatchPair result = new RoadMatchPair();
        try {
            position = gridTrajectoryIndex.getGetEncoder().encode(point);
        } catch (Exception e) {
            log.warn("the target point [{}, {}] is out of boundary, skip.", point[0], point[1]);
            throw new IndexOutOfBoundsException("The point is out of the roadmap space range.");
        }
        ArrayList<Integer> candidateIDs = new ArrayList<>();
        for (int i = Math.max(0, position[0] - 1); i < Math.min(gridTrajectoryIndex.getSpatialSplit()[0], position[0] + 2); i++) {
            for (int j = Math.max(0, position[1] - 1); j < Math.min(gridTrajectoryIndex.getSpatialSplit()[1], position[1] + 2); j++) {
                HashMap<Integer, List<Integer>> grid = gridTrajectoryIndex.getValue()[i][j];
                if (grid != null) {
                    candidateIDs.addAll(grid.keySet());
                }
            }
        }
        // find trajectories by ids
        List<Trajectory> trajectoryList = trajectoryDataIndex.get(framework.defaultTrajectoryDataset());
        List<Trajectory> trajectoryCandidates = new ArrayList<>();
        for (int id : candidateIDs) {
            trajectoryCandidates.add(trajectoryList.get(id));
        }
        double distance = Double.MAX_VALUE;
        for (int i = 0; i < trajectoryCandidates.size(); i++) {
            double currentDistance = calculateDistanceWithSegmentMode(point, trajectoryCandidates.get(i));
            if (currentDistance < distance) {
                distance = currentDistance;
                result.setRoadID(i);
                result.setRoadPoints(trajectoryCandidates.get(i));
            }
        }
        result.setDistance(distance);
        log.info("Search nearest in {}/{} trajectories", trajectoryCandidates.size(), trajectoryList.size());
        return result;
    }

    private double calculateDistanceWithSegmentMode(double[] point, Trajectory road) {
        double currentDistance = Double.MAX_VALUE;
        for (int i = 0; i < road.size() - 1; i++) {
            double distance = calculateSegmentDistance(point, road.get(i), road.get(i + 1));
            if (distance < currentDistance) {
                currentDistance = distance;
            }
        }
        return currentDistance;
    }

    private double calculatePointDistance(double[] a, double[] b) {
        return Math.pow(a[0] - b[0], 2) + Math.pow(a[1] - b[1], 2);
    }

    private double calculateSegmentDistance(double[] p, double[] a, double[] b) {
        return R2.distance(p, a, b);
    }
}
