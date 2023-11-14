package edu.whu.findroad;

import edu.nyu.dss.similarity.Framework;
import edu.whu.index.GeoEncoder;
import edu.whu.index.GridTrajectoryIndex;
import edu.whu.index.TrajectoryDataIndex;
import edu.whu.similarity.R2;
import edu.whu.structure.Trajectory;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import web.SpadasWebApplication;
import web.consts.TrajectoryDistanceType;
import web.service.FrameworkService;
import web.service.TrajectoryAugmentOptions;
import web.service.TrajectoryAugmentService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = SpadasWebApplication.class)
public class PointFindRoadTest {

    private Long count = 0L;

    @Autowired
    private Framework framework;

    @Autowired
    private FrameworkService frameworkService;

    @Autowired
    private TrajectoryAugmentService trajectoryAugmentService;

    @Autowired
    private TrajectoryDataIndex trajectoryDataIndex;
    @Autowired
    private GridTrajectoryIndex gridTrajectoryIndex;

    @Autowired
    private TrajectoryAugmentOptions options;

    /**
     * read a dataset and the road dataset, find each point's nearest road
     */
    @Test
    public void findRoadForDataset() {
        int datasetID = framework.findNameContains("2019_-_2020_School_Locations");
        int roadDatasetID = framework.findNameContains("new-york-road");
        double[][] pointDataset = frameworkService.dataMapPorto.get(datasetID);
        ArrayList<Trajectory> roadDataset = frameworkService.trajectoryDataIndex.get(roadDatasetID);
        // brutal force
        log.info("All points search with point Distance");
        count = 0L;
        Long startTime1 = System.currentTimeMillis();
        for (double[] point : pointDataset) {
            int roadIndex = findNearestRoadBrutalForce(point, roadDataset, TrajectoryDistanceType.POINT);
//            log.info("find nearest road for [{}, {}]: {}", point[0], point[1], roadDataset.get(roadIndex));
        }
        Long endTime1 = System.currentTimeMillis();
        log.warn("Total calculate {} point, cost {} ms", pointDataset.length, (endTime1 - startTime1));
        log.warn("Total calculate {} times distance", count);
        // grid based
        log.info("Grid based search with point Distance");
        Long startTime2 = System.currentTimeMillis();
        count = 0L;
        for (double[] point : pointDataset) {
            double distance = findNearestRoadWithGrid(point);
            log.info("find nearest road for [{}, {}]: distance={}", point[0], point[1], distance);
        }
        Long endTime2 = System.currentTimeMillis();
        log.warn("Total calculate {} point, cost {} ms", pointDataset.length, (endTime2 - startTime2));
        log.warn("Total calculate {} times distance", count);
        // with road distance
        log.info("Brutal Force with segment Distance");
        count = 0L;
        Long startTime3 = System.currentTimeMillis();
        for (double[] point : pointDataset) {
            int roadIndex = findNearestRoadBrutalForce(point, roadDataset, TrajectoryDistanceType.SEGMENT);
            log.debug("find nearest road for [{}, {}]: {}", point[0], point[1], roadDataset.get(roadIndex));
        }
        Long endTime3 = System.currentTimeMillis();
        log.warn("Total calculate {} point, cost {} ms", pointDataset.length, (endTime3 - startTime3));
        log.warn("Total calculate {} times distance", count);

        // with road distance and grid base
        log.info("Grid based search with segment Distance");
    }


    private int findNearestRoadBrutalForce(double[] point, ArrayList<Trajectory> roads, TrajectoryDistanceType type) {
        double min = 999;
        int index = -1;
        for (int j = 0; j < roads.size(); j++) {
            double currentDistance = switch (type) {
                case SEGMENT -> calculateDistanceWithSegmentMode(point, roads.get(j));
                case POINT, default -> calculateDistanceWithPointMode(point, roads.get(j));
            };
            if (currentDistance < min) {
                min = currentDistance;
                index = j;
            }
        }
        return index;
    }


    private double findNearestRoadWithGrid(double[] point) {
        int[] position;
        try {
            position = gridTrajectoryIndex.getGetEncoder().encode(point);
        } catch (Exception e) {
            log.warn("the target point [{}, {}] is out of boundary, skip.", point[0], point[1]);
            return -1.0;
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
        for (Trajectory road : trajectoryCandidates) {
            double currentDistance = calculateDistanceWithSegmentMode(point, road);
            if (currentDistance < distance) {
                distance = currentDistance;
            }
        }
        log.info("Search nearest in {}/{} trajectories", trajectoryCandidates.size(), trajectoryList.size());
        return distance;
    }

    private double calculateDistanceWithPointMode(double[] point, Trajectory road) {
        double currentDistance = Double.MAX_VALUE;
        for (double[] seg : road) {
            double distance = calculatePointDistance(point, seg);
            if (distance < currentDistance) {
                currentDistance = distance;
            }
        }
        return currentDistance;
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
        count += 1;
        return Math.pow(a[0] - b[0], 2) + Math.pow(a[1] - b[1], 2);
    }

    private double calculateSegmentDistance(double[] p, double[] a, double[] b) {
        return R2.distance(p, a, b);
    }
}
