package edu.nyu.dss.similarity.datasetReader;

import edu.nyu.dss.similarity.CityNode;
import edu.whu.config.SpadasConfig;
import edu.nyu.dss.similarity.index.*;
import edu.nyu.dss.similarity.statistics.DatasetSizeCounter;
import edu.nyu.dss.similarity.statistics.PointCounter;
import edu.rmit.trajectory.clustering.kmeans.IndexNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class PureLocationReader extends AbsReader {
    @Autowired
    private SpadasConfig config;

    @Autowired
    private DatasetSizeCounter datasetSizeCounter;

    @Autowired
    private PointCounter pointCounter;

    @Autowired
    private DatasetIDMapping datasetIDMapping;

    @Autowired
    private FileIDMap fileIDMap;

    @Autowired
    private DataMapPorto dataMapPorto;

    @Autowired
    private IndexBuilder indexBuilder;

    @Autowired
    private ZCodeMap zCodeMap;

    public Map<Integer, double[][]> read(File file, int fileNo, CityNode cityNode, int datasetIDForOneDir) throws IOException {
        if (!file.getName().endsWith("csv")) {
            return null;
        }
        IndexNode node = new IndexNode(2);
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
                if (splitString.length != 2) {
                    log.warn("file {},row {} has bad content.", file.getName(), lineNumber);
                    continue;
                }
                double[] b = new double[config.getDimension()];
                if (!splitString[0].isEmpty() && !splitString[1].isEmpty()) {
                    try {
//                        第一个值是纬度lat，第二个值是经度lng
                        b[0] = Double.parseDouble(splitString[0]);
                        b[1] = Double.parseDouble(splitString[1]);
                    } catch (Exception e) {
                        log.warn("Error reading lat and lng in {}, fields are {}, {}", file.getName(), splitString[0], splitString[1]);
                        continue;
                    }
                    list.add(b);
                    i++;
                }
            }
            data = list.toArray(new double[i][]);
            log.debug("File {} has {} lines", file.getName(), list.size());
            pointCounter.put(list.size());
        } catch (IOException e) {
            throw e;
        }
        if (i > 0) {
            String shortName = file.getParent() + "-" + FilenameUtils.removeExtension(file.getName());
            datasetSizeCounter.put(i);
            if (config.isCacheDataset()) {
                dataMapPorto.put(fileNo, data);
            }
            if (config.isCacheIndex()) {
//				createDatasetIndex(fileNo, xxx,1);
                node = indexBuilder.createDatasetIndex(fileNo, data, 1, cityNode);
//                对数据进行基于网格的取样，减小数据量
                indexBuilder.samplingDataByGrid(data, fileNo, node);
                node.setFileName(shortName);
            }
            datasetIDMapping.put(fileNo, shortName);
            fileIDMap.put(fileNo, file);
            indexBuilder.storeZcurve(data, fileNo);
            node.setSignautre(zCodeMap.get(fileNo).stream().mapToInt(Integer::intValue).toArray());
            indexBuilder.storeZCurveForEMD(data, fileNo, 180, 360, -90, -180);
//          storeZcurve(xxx, fileNo, 5, 5, 30, 100);
////        EffectivenessStudy.SerializedZcurve(zcodemap);
        }
        return dataMapPorto;
    }
}
