package edu.nyu.dss.similarity.datasetReader;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import edu.nyu.dss.similarity.CityNode;
import edu.nyu.dss.similarity.config.SpadasConfig;
import edu.nyu.dss.similarity.index.*;
import edu.nyu.dss.similarity.statistics.DatasetSizeCounter;
import edu.nyu.dss.similarity.statistics.PointCounter;
import edu.rmit.trajectory.clustering.kmeans.IndexNode;
import edu.whu.index.FilePathIndex;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@Slf4j
public class OpenNycReader {
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
    private DatasetProperties propertiesIndex;

    /**
     * 读取 Open NYC 的数据集
     * <p>
     * 有多少数据集有坐标的（195:325）
     *
     * @param file     文件对象
     * @param id       文件编号
     * @param cityNode 城市节点
     * @return 数据集ID
     * @throws IOException
     */
    public int read(File file, int id, CityNode cityNode) throws IOException {
        if (!file.getName().endsWith(".csv")) {
            return -1;
        }
        log.info("reading {}", file.getName());
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            CSVParser parser = CSVFormat.DEFAULT.withHeader().parse(reader);
            Map<String, Integer> headers = parser.getHeaderMap();
            String latKey = ReaderUtils.findLatKeyByHeader(headers);
            String lonKey = ReaderUtils.findLonKeyByHeader(headers);
            if (parser.getRecordNumber() >= Integer.MAX_VALUE) {
                throw new RuntimeException(String.format("Too many rows in file %s.", file.getName()));
            }
            if (latKey.isEmpty() || lonKey.isEmpty()) {
                log.info("[open-nyc]no position in file {}", file.getName());
                return -1;
            }
            List<CSVRecord> records = parser.getRecords();
            List<double[]> spatialData = new ArrayList<>();
            AtomicInteger size = new AtomicInteger();
            records.forEach(record -> {
                String lat = record.get(latKey);
                String lon = record.get(lonKey);
                if (lat.isEmpty() || lon.isEmpty()) {
                    return;
                }
                try {
                    double[] position = new double[2];
                    position[0] = Double.parseDouble(lat);
                    position[1] = Double.parseDouble(lon);
                    if (emptyPosition(position[0], position[1])) {
                        return;
                    }
                    spatialData.add(position);
                    size.addAndGet(1);
                } catch (NumberFormatException e) {
                    log.warn("Error format position column in file {} line {}", file.getName(), size.intValue());
                }
            });
            if (spatialData.isEmpty()) {
                log.warn("The dataset {} contains no valid position!", file.getAbsolutePath());
                return -1;
            }
            pointCounter.put(spatialData.size());
            datasetSizeCounter.put(spatialData.size());
            // try to read meta
            HashMap<String, Object> properties = readMeta(file);
            propertiesIndex.put(id, properties);
            loadSpatialIndex(spatialData.toArray(new double[size.intValue()][]), id, file, cityNode);
            return id;
        } catch (IOException e) {
            log.error(e.getMessage());
            throw e;
        }
    }


    private HashMap<String, Object> readMeta(File file) {
        boolean containMeta = false;
        File meta = null;
        for (File f : file.getParentFile().listFiles()) {
            if (f.isFile() && f.getName().contains("meta") && f.getName().endsWith("json")) {
                containMeta = true;
                meta = f;
            }
        }
        if (!containMeta || meta == null) {
            log.debug("No meta info for {}", file.getName());
        }
        HashMap<String, Object> result = new HashMap<>();
        try {
            InputStream in = new FileInputStream(meta);
            String text = IOUtils.toString(in, "utf8");
            JSONObject json = JSON.parseObject(text);
            if (json.containsKey("view")) {
                Object name = ((JSONObject) json.get("view")).get("name");
                result.put("name", name);
            }
        } catch (IOException e) {
            log.warn("The meta file {} contains format error", meta.getName());
        }
        return result;
    }

    private void loadSpatialIndex(double[][] spatialData, int id, File file, CityNode cityNode) {
        if (config.isCacheDataset()) {
            dataMapPorto.put(id, spatialData);
        }
        if (config.isCacheIndex()) {
            IndexNode node = indexBuilder.createDatasetIndex(id, spatialData, 1, cityNode);
            indexBuilder.samplingDataByGrid(spatialData, id, node);
            node.setFileName(file.getParentFile().getName());
        }
        datasetIDMapping.put(id, file.getName());
        fileIDMap.put(id, file);
        filePathIndex.put(file.getAbsolutePath(), id);
    }

    private boolean emptyPosition(double lat, double lng) {
        if (lat == 0.0 && lng == 0.0) {
            return true;
        }
        if (lat == 1.0 && lng == 1.0) {
            return true;
        }
        return false;
    }
}
