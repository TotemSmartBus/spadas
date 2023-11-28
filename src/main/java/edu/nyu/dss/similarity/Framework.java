package edu.nyu.dss.similarity;

import edu.nyu.dss.similarity.config.SpadasConfig;
import edu.nyu.dss.similarity.consts.DataLakeType;
import edu.nyu.dss.similarity.datasetReader.*;
import edu.nyu.dss.similarity.index.DataMapPorto;
import edu.nyu.dss.similarity.index.DatasetIDMapping;
import edu.nyu.dss.similarity.index.IndexMap;
import edu.nyu.dss.similarity.index.IndexNodes;
import edu.nyu.dss.similarity.statistics.DatasetSizeCounter;
import edu.nyu.dss.similarity.statistics.PointCounter;
import edu.nyu.dss.similarity.utils.FileUtil;
import edu.rmit.trajectory.clustering.kmeans.IndexAlgorithm;
import edu.rmit.trajectory.clustering.kmeans.IndexNode;
import edu.whu.index.FilePathIndex;
import edu.whu.index.GeoEncoder;
import edu.whu.index.GridTrajectoryIndex;
import edu.whu.index.TrajectoryDataIndex;
import edu.whu.structure.Trajectory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Component
@Slf4j
public class Framework {

    @Autowired
    private SpadasConfig config;

    @Autowired
    private DatasetSizeCounter datasetSizeCounter;

    @Autowired
    private PointCounter pointCounter;

    @Autowired
    private PoiReader poiReader;

    @Autowired
    private UsaReader usaReader;

    @Autowired
    private ChinaReader chinaReader;

    @Autowired
    private ArgoReader argoReader;

    @Autowired
    private FilePathIndex filePathIndex;

    @Autowired
    private PureLocationReader pureLocationReader;

    @Autowired
    private OpenNycReader openNycReader;

    @Autowired
    private ShapefileReader shapefileReader;

    @Autowired
    private IndexAlgorithm indexDSS;

    private String indexString = "";

    @Autowired
    public DatasetIDMapping datasetIdMapping;

    // 每遍历一个目录文件（如城市）生成一个新的map，value总共组成了dataMapPorto，用于计算emd
    @Autowired
    public DataMapPorto dataMapPorto;

    @Autowired
    private TrajectoryDataIndex trajectoryDataIndex;

    @Autowired
    private GridTrajectoryIndex gridTrajectoryIndex;
    private int fileNo = 0;
    //	store the data of every point of the whole data lake
    public static List<double[]> dataPoint = new ArrayList<>();
    public static List<CityNode> cityNodeList = new ArrayList<>();

    public static Map<String, List<IndexNode>> cityIndexNodeMap = new HashMap<>();

    private GeoEncoder geoEncoder;
    /*
     * z-curve for grid-based overlap
     */
    private final double minx = -90;
    private final double miny = -180;
    //    初试设为了4600，为什么？
//    说到底这个spaceRange到底有什么用
    private final double spaceRange = 100;


    /**
     * 每个维度划分成的网格份数
     */
    private final int SLIDE_PER_SIDE = 100;

    @Autowired
    public IndexMap indexMap;// root node of dataset's index
    static Map<Integer, IndexNode> datalakeIndex = null;// the global datalake index
    @Autowired
    public IndexNodes indexNodes;// store the root nodes of all datasets in the lake
    public IndexNode datasetRoot = null; // the root node of datalake index in memory mode

    static double[] weight = null; // the weight in all dimensions

    /*
     * read a folder and extract the corresponding column of each file inside
     * 读目录下的所有数据集文件，构建索引
     */
    public void readFolder(File folder, int limit, CityNode cityNode, int datasetIDForOneDir, HashMap<Integer, String> datasetIdMappingItem, DataLakeType type) throws IOException {
        File[] fileNames = folder.listFiles();
//        遍历每个数据集文件
        if (fileNames == null) {
            return;
        }
        for (File file : fileNames) {
            if (file.isDirectory()) {
                readFolder(file, limit, cityNode, datasetIDForOneDir++, datasetIdMappingItem, type);
            } else {
//				if (!isfolderVisited) {
//					dataLakeMapping.put(index, new ArrayList<>());
//					index++;
//					isfolderVisited = true;
//				}
//                String parentDir = file.getParent();
//            		if(a.length()<15){//argo
//            			//linux ????/
//            			datasetIdMapping.put(fileNo, parentDir.substring(parentDir.lastIndexOf(File.separator)+1)+File.separator+a);
//            			readContentCustom(file, fileNo++);
//            		}
//            		else {//poi
//            			datasetIdMapping.put(fileNo, parentDir.substring(parentDir.lastIndexOf(File.separator)+1)+File.separator+a);
//            			readContentPoi(file, fileNo++, a);
//            		}
//					datasetIdMapping.put(fileNo, parentDir.substring(parentDir.lastIndexOf(File.separator)+1)+File.separator+a);
//                fileNo考虑修改一下，目前表示的是数据集文件数，或许应该换成数据集目录数
//                fileName = parentDir.substring(parentDir.lastIndexOf(File.separator) + 1) + File.separator + a;
                datasetIdMappingItem.put(datasetIDForOneDir, file.getName());
//                datasetIdMapping.put(fileNo, fileName);
//                选择使用哪种读取方法

                switch (type) {
                    case PURE_LOCATION -> pureLocationReader.read(file, fileNo++, cityNode, datasetIDForOneDir);
                    case BAIDU_POI -> chinaReader.read(file, fileNo++, cityNode, datasetIDForOneDir);
                    case BUS_LINE, MOVE_BANK, USA -> usaReader.read(file, fileNo++, cityNode, datasetIDForOneDir);
                    case POI -> poiReader.read(file, fileNo++, cityNode);
                    case OPEN_NYC -> openNycReader.read(file, fileNo++, cityNode);
                    case SHAPE_FILE -> shapefileReader.read(file, fileNo++, cityNode);
                    case ARGOVERSE -> argoReader.read(file, fileNo++, cityNode);
                    // impossible to be here, default type is BAIDU_POI
                    default -> throw new RuntimeException("Unknown dataset type:" + type.name());
                }
//                一个数据集集中的索引
                datasetIDForOneDir++;
            }
            if (fileNo > limit) // 一般不会出现这种情况
                break;
        }
    }

    /*
     * storing the z-curve
     */
    @SuppressWarnings("unused")
    public void storeZcurveForEMD(double[][] dataset, int datasetId, double xRange, double yRange, double minx, double miny, HashMap<Integer, HashMap<Long, Double>> zcodeMapTmp) {
        int pointCnt = dataset.length;
        int numberCells = (int) Math.pow(2, config.getResolution());
        double xUnit = xRange / numberCells;
        double yUnit = yRange / numberCells;
        double weightUnit = 1.0 / pointCnt;
        HashMap<Long, Double> zcodeItemMap = new HashMap<>();
        for (double[] doubles : dataset) {
            int x = (int) ((doubles[0] - minx) / xUnit);
            int y = (int) ((doubles[1] - miny) / yUnit);
            long zcode = EffectivenessStudy.combine(x, y, config.getResolution());
            if (zcodeItemMap.containsKey(zcode)) {
                double val = zcodeItemMap.get(zcode);
                zcodeItemMap.put(zcode, val + weightUnit);
            } else {
                zcodeItemMap.put(zcode, weightUnit);
            }
        }
        zcodeMapTmp.put(datasetId, zcodeItemMap);
    }

    public int[] generateZcurveForRange(double[] minRange, double[] maxRange) {
        int numberCells = (int) Math.pow(2, config.getResolution());
        double unit = spaceRange / numberCells;
        List<Integer> list = new ArrayList<>();
        int minX = (int) ((minRange[0] - minx) / unit);
        int minY = (int) ((minRange[1] - miny) / unit);
        int maxX = (int) ((maxRange[0] - minx) / unit);
        int maxY = (int) ((maxRange[1] - miny) / unit);
        for (int i = minX; i < maxX; i++) {
            for (int j = minY; j < maxY; j++) {
                int zcode = (int) EffectivenessStudy.combine(i, j, config.getResolution() + 1);
                if (!list.contains(zcode)) {
                    list.add(zcode);
                }
            }
        }
        Collections.sort(list);
        return list.stream().mapToInt(Integer::intValue).toArray();
    }

    /**
     * setting parameters and reading the whole datalake
     * 需要对每个目录（argoverse、poi、beijing、shanghai等）建立z-order签名文件
     * 这样的话样例查询就只能在同一个数据集目录下进行，但这也合理，毕竟同一个目录下数据集之间距离是最近的
     *
     * @param limit 文件夹数据集数量上限
     * @throws IOException 读取文件异常
     */
    private void readDatalake(int limit) throws IOException {
        File folder = new File(config.getFile().getBaseUri());
        String[] files = folder.list();
        for (File subFolder : Objects.requireNonNull(folder.listFiles())) {
            if (subFolder.isFile()) {
                continue;
            }
            String pre_str = "./index/dss/index/";
            indexString = pre_str + subFolder.getName() + "/";

            HashMap<Integer, String> datasetIdMappingItem = new HashMap<>();
            DataLakeType type = DataLakeType.matchType(subFolder);
            File myFolder = new File(config.getFile().getBaseUri() + "/" + subFolder.getName());
            CityNode cityNode = new CityNode(subFolder.getName(), config.getDimension());

//            核心代码，这里的myFolder目录下就全部是数据集文件了
            readFolder(myFolder, limit, cityNode, 0, datasetIdMappingItem, type);

            cityNode.calAttrs(config.getDimension());
            cityNodeList.add(cityNode);
            cityIndexNodeMap.put(cityNode.cityName, cityNode.nodeList);
        }
        log.info("Totally {} files/folders and {} lines", files.length, pointCounter.get());
        // can modify weight by `normalizationWeight` method
        TreeMap<Integer, Integer> map = datasetSizeCounter.get();
        for (int key : map.keySet()) {
            FileUtil.write(indexString + "countHistogram.txt", key + "," + map.get(key) + "\n");
        }
    }


    // create datalake index based on the root nodes of all datasets, too slow
    void createDatalake(int N) {
        indexDSS.setGloabalid();
        if (config.isSaveIndex()) {
            datasetRoot = indexDSS.indexDatasetKD(indexNodes, config.getDimension(), config.getLeafCapacity(), weight);
            log.info("index created");
        } else {
            // FIXME exception when there's no directory.
            datalakeIndex = indexDSS.restoreDatalakeIndex(indexString + "datalake" + N + "-" + config.getLeafCapacity() + "-" + config.getDimension() + ".txt", config.getDimension());//the datalake index
        }

        if (datalakeIndex != null) {
            datalakeIndex.get(1).setMaxCovertpoint(datalakeIndex); // for the query that measures how many points in the intersected range
        } else {
            datasetRoot.setMaxCovertpoint(datalakeIndex);
        }
    }

    private void clearAll() {
        if (fileNo > 0) {
            fileNo = 0;
        }
        if (!datasetIdMapping.isEmpty()) {
            datasetIdMapping.clear();
        }
        if (!dataMapPorto.isEmpty()) {
            dataMapPorto.clear();
        }
        if (indexMap != null) {
            indexMap.clear();
        }
        if (!indexNodes.isEmpty()) {
            indexNodes.clear();
        }
        if (datasetRoot != null) {
            datasetRoot = null;
        }
        if (dataPoint != null) {
            dataPoint.clear();
        }
        if (!cityNodeList.isEmpty()) {
            cityNodeList.clear();
        }
    }

    /**
     * load the trajectory dataset's indexes
     */
    private void loadTrajectoryIndex(int datasetID) {
        Long beforeEncodeTime = System.currentTimeMillis();
        ArrayList<Trajectory> roadDataset = trajectoryDataIndex.get(datasetID);
        double[] range = findGeoRange(roadDataset);
        geoEncoder = new GeoEncoder(range[0], range[1], range[2], range[3], SLIDE_PER_SIDE, SLIDE_PER_SIDE);
        HashMap<Integer, List<Integer>>[][] index = encodeRoadmap(roadDataset, geoEncoder);
        gridTrajectoryIndex.setValue(index);
        gridTrajectoryIndex.setGetEncoder(geoEncoder);
        gridTrajectoryIndex.setSpatialRange(range);
        gridTrajectoryIndex.setSpatialSplit(new int[]{SLIDE_PER_SIDE, SLIDE_PER_SIDE});
        log.info("Loaded a grid based trajectory index with {} trajectories.", roadDataset.size());
        Long afterEncodeTime = System.currentTimeMillis();
        log.info("Total encode cost {} ms", (afterEncodeTime - beforeEncodeTime));
    }

    public HashMap<Integer, List<Integer>>[][] getGridIndex() {
        return gridTrajectoryIndex.getValue();
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

    private HashMap<Integer, List<Integer>>[][] encodeRoadmap(ArrayList<Trajectory> roads, GeoEncoder encoder) {
        // map key=road index, map value = segment number
        HashMap<Integer, List<Integer>>[][] grids = new HashMap[SLIDE_PER_SIDE][SLIDE_PER_SIDE];
        for (int i = 0; i < roads.size(); i++) {
            HashSet<Integer> gridRecord = new HashSet<>();
            for (int j = 0; j < roads.get(i).size(); j++) {
                double[] seg = roads.get(i).get(j);
                int[] indexes = encoder.encode(seg);
                gridRecord.add(indexes[0] * 100 + indexes[1]);
                if (grids[indexes[0]] == null) {
                    grids[indexes[0]] = new HashMap[SLIDE_PER_SIDE];
                }
                HashMap<Integer, List<Integer>> grid = grids[indexes[0]][indexes[1]];
                if (grid == null) {
                    grids[indexes[0]][indexes[1]] = new HashMap<>();
                    grid = grids[indexes[0]][indexes[1]];
                }
                if (grid.containsKey(i)) {
                    grid.get(i).add(j);
                } else {
                    ArrayList<Integer> list = new ArrayList<>();
                    list.add(j);
                    grid.put(i, list);
                }
            }
            log.debug("encode road {} with {} points in {} grids", i, roads.get(i).size(), gridRecord.size());
        }
        return grids;
    }

    public int defaultTrajectoryDataset() {
        return findNameContains("new-york-road");
    }

    public int findNameContains(String seg) {
        Optional<Map.Entry<String, Integer>> e = filePathIndex.entrySet().stream().filter(entry -> entry.getKey().contains(seg)).findFirst();
        if (e.isPresent()) {
            return e.get().getValue();
        }
        throw new RuntimeException(seg + " is not present.");
    }

    /**
     * init all datasets
     * 1. read the directory
     * 2. load the datasets index with points
     * 3. load the datasets index with trajectories
     */
    public void init() throws IOException {
        clearAll();
        readDatalake(config.getFrontendLimitation());
        createDatalake(config.getFrontendLimitation());
        loadTrajectoryIndex(defaultTrajectoryDataset());
        initTestRoadmap(10000);
        log.info("All data loaded.");
    }

    private void initTestRoadmap(int limit) {
        if (trajectoryDataIndex.isEmpty()) {
            log.warn("There's no trajectory data. skip for create test dataset.");
            return;
        }
        trajectoryDataIndex.put(0, (ArrayList<Trajectory>) trajectoryDataIndex.get(trajectoryDataIndex.keySet().stream().findFirst().get()).stream().limit(limit).collect(Collectors.toList()));
    }
}
