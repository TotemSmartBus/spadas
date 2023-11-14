package edu.nyu.dss.similarity.datasetReader;

import edu.nyu.dss.similarity.CityNode;
import edu.nyu.dss.similarity.config.SpadasConfig;
import edu.nyu.dss.similarity.index.DataMapPorto;
import edu.nyu.dss.similarity.index.DatasetIDMapping;
import edu.nyu.dss.similarity.index.FileIDMap;
import edu.nyu.dss.similarity.index.IndexBuilder;
import edu.nyu.dss.similarity.statistics.DatasetSizeCounter;
import edu.nyu.dss.similarity.statistics.PointCounter;
import edu.rmit.trajectory.clustering.kmeans.IndexNode;
import edu.whu.index.FilePathIndex;
import edu.whu.index.TrajectoryDataIndex;
import edu.whu.structure.Trajectory;
import lombok.extern.slf4j.Slf4j;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.locationtech.jts.geom.*;
import org.opengis.feature.simple.SimpleFeature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
public class ShapefileReader {
    @Autowired
    private SpadasConfig config;

    @Autowired
    private DatasetSizeCounter datasetSizeCounter;

    @Autowired
    private PointCounter pointCounter;

    @Autowired
    private FileIDMap fileIDMap;

    @Autowired
    private DataMapPorto dataMapPorto;

    @Autowired
    private DatasetIDMapping datasetIDMapping;

    @Autowired
    private IndexBuilder indexBuilder;

    @Autowired
    private FilePathIndex filePathIndex;


    @Autowired
    private TrajectoryDataIndex trajectoryDataIndex;

    private double[] latRange = new double[]{-74.214328, -73.726123};
    private double[] lonRange = new double[]{40.622748, 40.884902};

    public void read(File file, int id, CityNode cityNode) throws IOException {
        if (!file.getName().endsWith("shp")) {
            return;
        }
        file.setReadOnly();
        FileDataStore store = FileDataStoreFinder.getDataStore(file);
        SimpleFeatureSource featureSource = store.getFeatureSource();
        boolean hasTrajectory = false;
        try (SimpleFeatureIterator i = featureSource.getFeatures().features()) {
            SimpleFeature feature;
            List<double[]> locations = new ArrayList<>();
            int count = 0;
            while (i.hasNext()) {
                feature = i.next();
                Object defaultGeometry = feature.getDefaultGeometry();
                if (defaultGeometry instanceof MultiPolygon) {
                    List<double[]> points = getMultiPolygonPoints((MultiPolygon) defaultGeometry);
                    locations.addAll(points);
                    count += 1;
                } else if (defaultGeometry instanceof Point) {
                    double[] point = getPoint((Point) defaultGeometry);
                    locations.add(point);
                    count += 1;
                } else if (defaultGeometry instanceof MultiLineString) {
                    // TODO 实现路线轨迹的提取算法
                    ArrayList<Trajectory> trajectories = getMultiLineString((MultiLineString) defaultGeometry);
                    List<double[]> points = trajectories.stream().flatMap(ArrayList::stream).toList();
                    if (trajectoryDataIndex.containsKey(id)) {
                        trajectoryDataIndex.get(id).addAll(trajectories);
                    } else {
                        trajectoryDataIndex.put(id, trajectories);
                    }
                    hasTrajectory = true;
                    locations.addAll(points);
                    count += 1;
                } else {
                    log.warn("Unknown location type of shapefile: {}", defaultGeometry.getClass().getName());
                }
            }

            log.info("there are {} shapes, {} points in {}", count, locations.size(), file.getName());
            if (locations.size() > config.getFrontendLimitation()) {
                locations = locations.subList(0, config.getFrontendLimitation() - 1);
            }
            pointCounter.put(locations.size());
            datasetSizeCounter.put(locations.size());

            writeIndex(locations, id, file, cityNode, hasTrajectory);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            store.dispose();
        }
    }

    public void writeIndex(List<double[]> locations, int id, File file, CityNode cityNode, boolean hasTrajectory) {
        double[][] spatialData = locations.toArray(new double[locations.size()][]);
        if (config.isCacheDataset()) {
            dataMapPorto.put(id, spatialData);
        }
        if (config.isCacheIndex()) {
            IndexNode node;
            node = indexBuilder.createDatasetIndex(id, spatialData, hasTrajectory ? 0 : 1, cityNode);
            node.setFileName(file.getParent());
            indexBuilder.samplingDataByGrid(spatialData, id, node);

        }
        datasetIDMapping.put(id, file.getName());
        fileIDMap.put(id, file);
        filePathIndex.put(file.getAbsolutePath(), id);
    }

    private List<double[]> getMultiPolygonPoints(MultiPolygon polygon) {
        int size = polygon.getNumGeometries();
        List<double[]> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Polygon p = (Polygon) polygon.getGeometryN(i);
            Coordinate[] co = p.getCoordinates();
            for (int j = 0; j < co.length; j++) {
                double[] point = new double[2];
                point[0] = co[j].x;
                point[1] = co[j].y;
                result.add(point);
            }
        }
        return result;
    }

    private double[] getPoint(Point point) {
        double lat = point.getY();
        double lon = point.getX();
        return new double[]{lat, lon};
    }

    private ArrayList<Trajectory> getMultiLineString(MultiLineString lineString) {
        int size = lineString.getNumGeometries();
        boolean insideNyc = false;
        int count = 0;
        ArrayList<Trajectory> tras = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            LineString p = (LineString) lineString.getGeometryN(i);
            Trajectory tra = new Trajectory();
            Coordinate[] co = p.getCoordinates();
            for (int j = 0; j < co.length; j++) {
                double[] point = new double[2];
                point[1] = co[j].x;
                point[0] = co[j].y;
                if (point[1] >= latRange[0] && point[1] <= latRange[1] && point[0] >= lonRange[0] && point[0] <= lonRange[1]) {
                    insideNyc = true;
                    count += 1;
                }
                tra.add(point);
            }
            tras.add(tra);
        }
        return insideNyc ? tras : new ArrayList<>();
    }
}
