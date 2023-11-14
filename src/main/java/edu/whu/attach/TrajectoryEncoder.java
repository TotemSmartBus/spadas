package edu.whu.attach;

import edu.whu.index.TrajectoryDataIndex;
import edu.whu.index.TrajectoryGridIndex;
import edu.whu.structure.DatasetID;
import edu.whu.structure.Trajectory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TrajectoryEncoder {
    @Autowired
    private TrajectoryDataIndex dataIndex;

    @Autowired
    private TrajectoryGridIndex gridIndex;


    /**
     * find all the datasets contains trajectory need to be indexed
     *
     * @return all trajectory datasets
     */
    private List<DatasetID> filterIndexTrajectoryDatasets() {
        return dataIndex.keySet().stream().map(DatasetID::new).collect(Collectors.toList());
    }

    private void index(DatasetID datasetID) {
        ArrayList<Trajectory> roadDataset = dataIndex.get(datasetID.get());
        double[] range = findGeoRange(roadDataset);

    }

    private double[] findGeoRange(ArrayList<Trajectory> roads) {
        double lat_min = 180, lat_max = -180, lng_min = 180, lng_max = -180;
        Long beforeEncodeTime = System.currentTimeMillis();
        for (Trajectory road : roads) {
            for (double[] seg : road) {
                if (seg[0] < lat_min) {
                    lat_min = seg[0];
                }
                if (seg[0] > lat_max) {
                    lat_max = seg[0];
                }
                if (seg[1] < lng_min) {
                    lng_min = seg[1];
                }
                if (seg[1] > lng_max) {
                    lng_max = seg[1];
                }
            }
        }
        log.info("The roadmap lat range from {} to {}, lng range from {} to {}", lat_min, lat_max, lng_min, lng_max);
        return new double[]{lat_min, lng_min, lat_max, lng_max};
    }
}
