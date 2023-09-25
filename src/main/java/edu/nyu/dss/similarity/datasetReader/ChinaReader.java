package edu.nyu.dss.similarity.datasetReader;

import edu.nyu.dss.similarity.CityNode;
import edu.nyu.dss.similarity.config.SpadasConfig;
import edu.nyu.dss.similarity.index.*;
import edu.nyu.dss.similarity.statistics.DatasetSizeCounter;
import edu.nyu.dss.similarity.statistics.PointCounter;
import edu.rmit.trajectory.clustering.kmeans.indexNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class ChinaReader {
    @Autowired
    private SpadasConfig config;

    @Autowired
    private DatasetSizeCounter datasetSizeCounter;

    @Autowired
    private PointCounter pointCounter;

    @Autowired
    private DatasetPerDir datasetPerDir;

    @Autowired
    private DatasetIDMapping datasetIDMapping;

    @Autowired
    private FileIDMap fileIDMap;

    @Autowired
    private DataMapPorto dataMapPorto;

    @Autowired
    private IndexBuilder indexBuilder;

    public Map<Integer, double[][]> read(File file, int fileNo, CityNode cityNode, int datasetIDForOneDir) throws IOException {
        if (!file.getName().endsWith("csv")) {
            return null;
        }
        int i = 0;
        List<double[]> list = new ArrayList<>();
        double[][] data;
        log.info("Reading File {}", file.getName());
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String strLine;
            br.readLine();// skip first row
            int lineNumber = 0;
            while ((strLine = br.readLine()) != null) {
                lineNumber++;
                String[] splitString = strLine.split(",");
                if (splitString.length < 12) {
                    log.warn("file {},row {} has bad content.", file.getName(), lineNumber);
                    continue;
                }
                double[] b = new double[config.getDimension()];
                if (!splitString[12].isEmpty() && !splitString[11].isEmpty()) {
                    try {
//                        第一个值是纬度lat，第二个值是经度lng
                        b[0] = Double.parseDouble(splitString[12]);
                        b[1] = Double.parseDouble(splitString[11]);
                    } catch (Exception e) {
                        log.warn("Error reading lat and lng in {}, fields are {}, {}", file.getName(), splitString[12], splitString[11]);
                        continue;
                    }
                    // data point must inside china
                    if (b[0] < 3 || b[0] > 54 || b[1] < 73 || b[1] > 136) {
                        continue;
                    }
                    list.add(b);
                    i++;
                }
            }
            data = list.toArray(new double[i][]);
            log.info("File {} has {} lines", file.getName(), list.size());
            pointCounter.put(list.size());
        } catch (IOException e) {
            throw e;
        }
        if (i > 0) {
            datasetSizeCounter.put(i);
            if (config.isCacheDataset()) {
                dataMapPorto.put(fileNo, data);
                datasetPerDir.put(datasetIDForOneDir, data);
            }
            if (config.isCacheIndex()) {
//				createDatasetIndex(fileNo, xxx,1);
                indexNode node = indexBuilder.createDatasetIndex(fileNo, data, 1, cityNode);
//                对数据进行基于网格的取样，减小数据量
                indexBuilder.samplingDataByGrid(data, fileNo, node);
                node.setFileName(file.getName());
            }
            datasetIDMapping.put(fileNo, file.getName());
            fileIDMap.put(fileNo, file);
//          storeZcurve(xxx, fileNo, 5, 5, 30, 100);
////        EffectivenessStudy.SerializedZcurve(zcodemap);
        }
        return dataMapPorto;
    }
}
