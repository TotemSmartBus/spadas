package web.service;

import edu.nyu.dss.similarity.*;
import edu.whu.config.SpadasConfig;
import edu.nyu.dss.similarity.datasetReader.SingleFileReader;
import edu.nyu.dss.similarity.datasetReader.UploadReader;
import edu.nyu.dss.similarity.index.*;
import edu.rmit.trajectory.clustering.kmeans.IndexAlgorithm;
import edu.rmit.trajectory.clustering.kmeans.IndexNode;
import edu.whu.index.FilePathIndex;
import edu.whu.index.TrajectoryDataIndex;
import edu.whu.structure.Trajectory;
import edu.whu.tmeans.augment.BrutalForceAugment;
import edu.whu.tmeans.model.GeoLocation;
import emd.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import web.Utils.ListUtil;
import web.VO.DatasetVo;
import web.VO.JoinPair;
import web.VO.JoinPoint;
import web.VO.JoinResultVO;
import web.param.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class FrameworkService {

    @Autowired
    private SpadasConfig config;

    @Autowired
    private UploadReader uploadReader;

    @Autowired
    private SingleFileReader singleFileReader;

    @Autowired
    private Search search;

    @Autowired
    private IndexAlgorithm indexDSS;

    private final String indexString = "";
    @Autowired
    public DatasetIDMapping datasetIdMapping;
    // 存储数据集文件id到数据集文件名的映射，每个目录独立映射
    @Autowired
    public DataSamplingMap dataSamplingMap;

    // 每遍历一个目录文件（如城市）生成一个新的map，value总共组成了dataMapPorto，用于计算emd
    @Autowired
    public DataMapPorto dataMapPorto;

    @Autowired
    public TrajectoryDataIndex trajectoryDataIndex;

    @Autowired
    public FileIDMap fileIDMap;

    @Autowired
    public FilePathIndex filePathIndex;

    @Autowired
    private Framework framework;

    @Autowired
    public IndexMap indexMap;// root node of dataset's index
    static Map<Integer, IndexNode> datalakeIndex = null;// the global datalake index
    @Autowired
    public IndexNodes indexNodes;// store the root nodes of all datasets in the lake// the root node of datalake index in memory mode
    static Map<Integer, Map<Integer, IndexNode>> datasetIndex; // restored dataset index

    @Autowired
    private ZCodeMap zCodeMap;

    @Autowired
    private ZCodeMapEmd zCodeMapEmd;

    @Autowired
    private EffectivenessStudy effectivenessStudy;

    @Autowired
    private DatasetProperties datasetProperties;

    public List<DatasetVo> rangeQuery(RangeQueryParams qo) {
        HashMap<Integer, Double> result = new HashMap<>();
        IndexNode root = framework.datasetRoot;
//        为什么需要根节点参与？
        if (datalakeIndex != null)
            root = datalakeIndex.get(1);
//        IA
        if (qo.getMode() == 1) {
            //base on intersecting area
            search.setScanning(!qo.isUseIndex());
            search.rangeQueryRankingArea(root, result, qo.getQuerymax(), qo.getQuerymin(), Double.MAX_VALUE, qo.getK(), null, qo.getDim(),
                    datalakeIndex);
        } else {
//		GBO
            //base on grid-base overlap
            int queryid = 1;
            if (qo.isUseIndex()) {
//				int[] queryzcurve = new int[zcodemap.get(queryid).size()];
//				for (int i = 0; i < zcodemap.get(queryid).size(); i++)
//					queryzcurve[i] = zcodemap.get(queryid).get(i);
                int[] queryzcurve = framework.generateZcurveForRange(qo.getQuerymin(), qo.getQuerymax());
                result = search.gridOverlap(root, result, queryzcurve, 1, qo.getK(), null, datalakeIndex);
            } else {
                result = effectivenessStudy.topkAlgorithmZcurveHashMap(zCodeMap, zCodeMap.get(queryid), qo.getK());
            }
        }//base on intersecting area
//        干嘛用的？
//        默认传值false
//        更新静态变量scanning，在下面的核心算法中会用到

//        这是IA度量方法
//        Search.setScanning(!qo.isUseIndex());
//        Search.setScanning(qo.isUseIndex());
//        核心算法
//        result = Search.rangeQueryRankingArea(root, result, qo.getQuerymax(), qo.getQuerymin(), Double.MAX_VALUE, qo.getK(), null, qo.getDim(),
//                datalakeIndex, dimNonSelected, dimensionAll);

        return result.entrySet().stream()
                .sorted((o1, o2) -> (int) (o1.getValue() - o2.getValue()))
                .map(item -> new DatasetVo(
                        indexMap.get(item.getKey()),
                        datasetIdMapping.get(item.getKey()),
                        item.getKey(),
                        null
//                        dataMapPorto.get(item.getKey())
                ))
                .collect(Collectors.toList());
    }

    public List<DatasetVo> keywordsQuery(KeywordsParams qo) {
        List<DatasetVo> res = new ArrayList<>();
        String kwd = qo.getKws().toLowerCase();
        for (int i = 0; i < datasetIdMapping.size(); i++) {
            if (datasetIdMapping.get(i) != null && datasetIdMapping.get(i).toLowerCase().contains(kwd)) {
                res.add(new DatasetVo(i, indexMap.get(i), datasetIdMapping.get(i), dataSamplingMap.get(i), indexMap.get(i).getTotalCoveredPoints() < config.getFrontendLimitation() ? dataMapPorto.get(i) : null));
                if (res.size() == qo.getLimit()) {
                    break;
                }
            }
        }
        return res;
    }

    public List<DatasetVo> datasetQuery(IndexNode queryNode, double[][] data, int k) throws IOException {
        HashMap<Integer, Double> result = search.pruneByIndex(dataMapPorto, framework.datasetRoot, queryNode,
                config.getDimension(), indexMap, datasetIndex, datalakeIndex, datasetIdMapping, k,
                indexString, null, true, 0, config.getLeafCapacity(), null, config.isSaveIndex(), data);
        List<DatasetVo> finalResult = new ArrayList<>(result.entrySet().stream().sorted((o1, o2) -> (int) (o1.getValue() - o2.getValue())).
                map(i -> new DatasetVo(i.getKey(), indexMap.get(i.getKey()), datasetIdMapping.get(i.getKey()), dataSamplingMap.get(i.getKey()), indexMap.get(i.getKey()).getTotalCoveredPoints() < config.getFrontendLimitation() ? dataMapPorto.get(i.getKey()) : null)).toList());
        return finalResult.subList(0, k);
    }

    public List<DatasetVo> datasetQuery(DatasetQueryParams qo) throws IOException, CloneNotSupportedException {
//        indexNode queryNode;
        IndexNode queryNode = indexMap.get(qo.getDatasetId());
        double[][] data = dataMapPorto.get(qo.getDatasetId());
        HashMap<Integer, Double> result = new HashMap<>();

        switch (qo.getMode()) {
            case Haus -> // HausDist
                    result = search.pruneByIndex(dataMapPorto, framework.datasetRoot, queryNode,
                            qo.getDim(), indexMap, datasetIndex, datalakeIndex, datasetIdMapping, qo.getK(),
                            indexString, null, true, qo.getError(), config.getLeafCapacity(), null, config.isSaveIndex(), data);
            case IA -> { // Intersecting Area
                search.setScanning(false);
                result = search.rangeQueryRankingArea(framework.datasetRoot, result, queryNode.getMBRmax(), queryNode.getMBRmin(), Double.MAX_VALUE, qo.getK(), null, qo.getDim(),
                        datalakeIndex);
            }
            case GBO -> { // GridOverlap using index
                IndexNode root = framework.datasetRoot;//datalakeIndex.get(1);//use store
                int[] queryzcurve = zCodeMap.get(qo.getDatasetId()).stream().mapToInt(Integer::intValue).toArray();
                result = search.gridOverlap(root, result, queryzcurve, Double.MAX_VALUE, qo.getK(), null, datalakeIndex);
            }
            case EMD -> {
//            emd
//                int[] queryID = convertID(qo.getDatasetId());
                PriorityQueue<relaxIndexNode> resQueue = EMD(qo.getDatasetId(), qo.getK());
                while (!resQueue.isEmpty()) {
                    relaxIndexNode rin = resQueue.poll();
                    result.put(rin.getResultId(), rin.getLb());
                }
            }
            default -> {
            }
        }

        List<DatasetVo> finalResult = new ArrayList<>(result.entrySet().
                stream()
                .sorted(Comparator.comparingDouble(Map.Entry::getValue))
                .map(item -> new DatasetVo(indexMap.get(item.getKey()), datasetIdMapping.get(item.getKey()), item.getKey(), dataMapPorto.get(item.getKey()))).
                toList());
//        防止找不到k个候选结果
        return finalResult.size() > qo.getK() ? finalResult.subList(0, qo.getK()) : finalResult;
    }

    public List<List<double[]>> union(UnionParams dto) {
        double[][] queryDataAll = dataMapPorto.get(dto.getQueryId());
        int rows = dto.getPreRows();
        List<double[]> queryDataList = new ArrayList<>(Arrays.asList(queryDataAll));

        List<double[]> queryData = ListUtil.sampleList(queryDataList, rows);
        List<List<double[]>> bodies = new ArrayList<>();
        bodies.add(queryData);

        double[][] unionDataAll = dataMapPorto.get(dto.getUnionId());
        List<double[]> unionDataList = new ArrayList<>(Arrays.asList(unionDataAll));
        bodies.add(ListUtil.sampleList(unionDataList, rows));
        return bodies;
    }

    public List<List<double[]>> unionRangeQuery(UnionRangeQueryParams dto) {
        double[][] queryDataAll = dataMapPorto.get(dto.getQueryId());
        int rows = dto.getPreRows();
        List<double[]> queryDataList = new ArrayList<>(Arrays.asList(queryDataAll));

        List<double[]> queryData = ListUtil.sampleList(queryDataList, rows);
        List<List<double[]>> bodies = new ArrayList<>();
        bodies.add(queryData);

        int id = dto.getUnionId();
        List<double[]> dataAll = new ArrayList<>();
        search.UnionRangeQueryForPoints(dto.getRangeMax(), dto.getRangeMin(), id, indexMap.get(id), dataAll, config.getDimension(), null, true);
        List<double[]> dataSample = ListUtil.sampleList(dataAll, rows);
        bodies.add(dataSample);

        return bodies;
    }

    public Pair<Double[], Map<Integer, Integer>> pairwiseJoin(int queryID, int datasetID, int rowLimit) throws IOException {
        double[][] queryDataset = queryDataset(queryID);
        double[][] targetDataset = queryDataset(datasetID);

        IndexNode queryNode = queryNode(queryID, queryDataset, null);
        IndexNode targetNode = queryNode(datasetID, targetDataset, null);
        // Hausdorff Pair-wise distance measure
        AdvancedHausdorff.setBoundChoice(0);
//		Pair<Double, PriorityQueue<queueMain>> aPair = AdvancedHausdorff.IncrementalDistance(querydata, dataset, dimension, queryNode, datanode, 1, 1, 0, false, 0, false,queryindexmap, dataindexMap, null, true);
//        splitOption: 1 -> 0;
        Pair<Double, PriorityQueue<queueMain>> aPair = AdvancedHausdorff.IncrementalDistance(queryDataset, targetDataset, config.getDimension(),
                queryNode, targetNode, 0, 1, 0, true, 0, true,
                null, null, null, true);
//        fastMode: 0 -> 1
        return Join.IncrementalJoinCustom(rowLimit, dataMapPorto.get(queryID), dataMapPorto.get(datasetID),
                config.getDimension(), indexMap.get(config.getFrontendLimitation() + 1), indexMap.get(datasetID), 1, 0, 0, false,
                0, true, aPair.getLeft(), aPair.getRight(), null, null, "haus",
                null, true);
    }


    public JoinResultVO join(int queryId, int datasetId, int rowLimit) throws IOException {
        Pair<Double[], Map<Integer, Integer>> pair = pairwiseJoin(queryId, datasetId, rowLimit);
        double[][] queryDataset = dataMapPorto.get(queryId);
        double[][] targetDataset = dataMapPorto.get(datasetId);
        List<JoinPair> list = new ArrayList<>();
        Double[] distances = pair.getLeft();
        Map<Integer, Integer> combine = pair.getRight();
        for (Map.Entry<Integer, Integer> entry : combine.entrySet()) {
            JoinPoint queryPoint = new JoinPoint(entry.getKey(), queryDataset[entry.getKey()]);
            JoinPoint targetPoint = new JoinPoint(entry.getValue(), targetDataset[entry.getValue()]);
            JoinPair pairResult = new JoinPair(queryPoint, targetPoint, distances[entry.getKey()]);
            list.add(pairResult);
        }
        JoinResultVO result = new JoinResultVO();
        result.setList(list);
        result.setQueryDatasetID(queryId);
        result.setTargetDatasetID(datasetId);
        return result;
    }

    public Pair<IndexNode, double[][]> readNewFile(MultipartFile file, String fileName) throws IOException {
        return uploadReader.read(file, fileName);
    }

    public DatasetVo getDatasetVO(int i) {
        return new DatasetVo(i, indexMap.get(i), datasetIdMapping.get(i), dataSamplingMap.get(i), indexMap.get(i).getTotalCoveredPoints() < config.getFrontendLimitation() ? dataMapPorto.get(i) : null);
    }

    private IndexNode queryNode(int datasetID, double[][] dataset, Map<Integer, IndexNode> indexMap) {
        if (indexMap != null) {
            return indexMap.get(datasetID);
        } else if (datasetIndex != null) {
            indexMap = datasetIndex.get(datasetID);
            return datasetIndex.get(datasetID).get(1);
        } else {
            return indexDSS.buildBalltree2(dataset, config.getDimension(), config.getLeafCapacity(), null, null, null);

        }
    }

    private double[][] queryDataset(int datasetID) throws IOException {
        if (dataMapPorto == null || dataMapPorto.isEmpty()) {

            return singleFileReader.readSingleFile(datasetIdMapping.get(datasetID));
        } else {
            return dataMapPorto.get(datasetID);
        }
    }

    /**
     * get a dataset with its augment columns
     *
     * @param datasetID query dataset id
     * @param k         top k
     * @return result dataset columns
     */
    public DatasetVo datasetAugment(int datasetID, int k, int n, DatasetQueryParams options) {
//        List<Column> dataset = datasetIndex.get(datasetID);
//        List<?> topRelatedDatasets =datasetQuery(new DatasetQueryParams(10,));
//        for(? related: topRelatedDatasets) {
//            Column col = queryRelatedData(dataset, related, k);
//            dataset.add(col);
//        }
//        return dataset;
        return brutalForceSearchAugment(datasetID, k, n, options);
    }


    private DatasetVo brutalForceSearchAugment(int datasetID, int k, int n, DatasetQueryParams options) {
        // read target dataset
        DatasetVo targetDataset = getDatasetVO(datasetID);

        // search related dataset
        options.setQuerydata(targetDataset.getMatrix());
        options.setDatasetId(datasetID);
        options.setK(k);
        List<DatasetVo> relatedDatasets = new ArrayList<>();
        // find related datasets
        try {
            relatedDatasets = datasetQuery(options);
        } catch (IOException | CloneNotSupportedException e) {
            log.warn("Error finding related datasets for {}", datasetID);
        }
        // prepare the data
        HashMap<String, List<Object>> results = new HashMap<>();
        List<GeoLocation> target = Arrays.stream(targetDataset.getMatrix()).map(p -> new GeoLocation(p[0], p[1])).toList();
        for (DatasetVo candidate : relatedDatasets) {
            List<GeoLocation> can = Arrays.stream(candidate.getMatrix()).map(p -> new GeoLocation(p[0], p[1])).toList();

            List<GeoLocation[]> augmentColumn = BrutalForceAugment.getNearestPoints(target, can, n);
            List<Object> eraseColumn = Arrays.asList(augmentColumn.toArray());
            String name = candidate.getFilename();
            if (datasetProperties.containsKey(candidate.getId())) {
                name = (String) (datasetProperties.get(candidate.getId()).get("name"));
            }
            name = "Nearest " + (n > 1 ? (n + " ") : "") + name;
            results.put(name, eraseColumn);
        }
        targetDataset.getColumns().putAll(results);
        return targetDataset;
    }

    public ArrayList<Trajectory> getTrajectory(int datasetId) {
        return trajectoryDataIndex.get(datasetId);
    }

    public PriorityQueue<relaxIndexNode> EMD(int queryId, int topk) throws CloneNotSupportedException {
//        HashMap<Integer, HashMap<Long, Double>> mapForDir =
//        HashMap<Long, Double> mapQuery = mapForDir.get(datasetQueryID);
        HashMap<Integer, ArrayList<double[]>> allHistogram = new HashMap<>();
//        iterMatrix中的每个元素代表一个数据集池化后的坐标
        HashMap<Integer, double[]> iterMatrix = new HashMap<>();
        HashMap<Integer, Double> ubMove = new HashMap<>();
        HashMap<Integer, Integer> histogramName = new HashMap<>();
//        double[][] iterMatrix = new double[zCodeMapEmd.size()][config.getDimension()];
//        double[] ubMove = new double[zCodeMapEmd.size()];
//        int[] histogram_name = new int[zCodeMapEmd.size()];
//        signature_t querySignature = new signature_t();
        double[] query = new double[config.getDimension()];
        double radiusQuery = 0;
        SignatureT querySignature = getAllData(queryId, allHistogram, iterMatrix, ubMove, histogramName, query, radiusQuery);
        int leafThreshold = 100;
        int maxDepth = 10;
//        看能不能替换成已经建立好的索引结构，可以在原有的结构上增加属性如radiusEMD
//        这里建立的ballTree到底是整个根节点还是单个数据集的根节点
//        这里是根据iterMatrix（池化后的坐标数据）来建立节点的，而不是原始数据
        IndexNode ballTree = ball_tree.create(ubMove, iterMatrix, leafThreshold, maxDepth, config.getDimension());
//        我的理解是，第一次过滤根本不会深入到数据集节点内部，更不会到叶子节点和点级别
        ArrayList<Integer> firstFlterResult = getBranchAndBoundResultID(ballTree, query);
        PriorityQueue<relaxIndexNode> resultApprox = new PriorityQueue<>(new ComparatorByRelaxIndexNode());
        relax_EMD re = new relax_EMD();

        int filterCount = 0;
        int refineCount = 0;

        for (int id : firstFlterResult) {
//            id - 1 -> id
            SignatureT data = getSignature(id, allHistogram);
            double emdLB = re.tighter_ICT(data, querySignature);
            if (resultApprox.size() < topk) {
                relaxIndexNode in = new relaxIndexNode(id, emdLB);
                resultApprox.add(in);
                refineCount++;
            } else {
                double best = resultApprox.peek().lb;
                double lowerbound = emdLB;
                if (lowerbound >= best) {
                    filterCount++;
                    continue;
                }//被过滤
                else {
                    relaxIndexNode in = new relaxIndexNode(id, emdLB);
                    resultApprox.poll();
                    resultApprox.add(in);
                    refineCount++;
                }
            }
        }
        return resultApprox;
    }

    public SignatureT getAllData(int datasetQueryId, HashMap<Integer, ArrayList<double[]>> allHistogram,
                                 HashMap<Integer, double[]> iterMatrix, HashMap<Integer, Double> ubMove, HashMap<Integer, Integer> histogramName, double[] query, double queryRadius) {
        ArrayList<double[]> l = new ArrayList<>();
        DoubleArrayList ub = new DoubleArrayList();
        Map<Integer, String[]> datasetList_after_pooling = getPooling(zCodeMapEmd);
        ArrayList<Integer> his = new ArrayList(); //his coresponding to the all histogram_name
        ArrayList<Integer> datasetID = new ArrayList<>();
        int numberOfLine = 0;
        SignatureT querySignature = null;
        for (Map.Entry<Integer, HashMap<Long, Double>> entry : zCodeMapEmd.entrySet()) {
            ArrayList<double[]> allPoints = new ArrayList<>();
            int id = entry.getKey();
            HashMap<Long, Double> map = entry.getValue();
            for (long key : map.keySet()) {
                long[] coord = resolve(key);
                double[] d = new double[3];
                d[0] = Double.parseDouble(String.valueOf(coord[0]));
                d[1] = Double.parseDouble(String.valueOf(coord[1]));
                d[2] = map.get(key);
                allPoints.add(d);
            }
            //sampleData
            String[] buf = datasetList_after_pooling.get(id);
            allHistogram.put(id, allPoints);
//            datasetID.add(numberOfLine);
            if (id == datasetQueryId) {
                //sampleData
//                query = new double[dimension];
                query[0] = Double.parseDouble(buf[1]);
                query[1] = Double.parseDouble(buf[2]);
                queryRadius = Double.parseDouble(buf[3]);

                int n = allPoints.size();
                FeatureT[] features = new FeatureT[n];
                double[] weights = new double[n];
                for (int i = 0; i < n; i++) {
                    features[i] = new FeatureT(allPoints.get(i)[0], allPoints.get(i)[1]);
                    weights[i] = allPoints.get(i)[2];
                }
                querySignature = new SignatureT(n, features, weights);
            }
            //sampleData
//            double[] corrd = new double[config.getDimension()];
//            corrd[0] = Double.parseDouble(buf[1]);
//            corrd[1] = Double.parseDouble(buf[2]);
            iterMatrix.put(id, new double[]{Double.parseDouble(buf[1]), Double.parseDouble(buf[2])});
            ubMove.put(id, Double.parseDouble(buf[3]));
            histogramName.put(id, Integer.parseInt(buf[0]));
//            l.add(corrd);
//            his.add(Integer.parseInt(buf[0]));
//            ub.add(Double.parseDouble(buf[3]));
//            numberOfLine++;
        };
        /*while (numberOfLine < zCodeMapEmd.size()) {
            ArrayList<double[]> allPoints = new ArrayList<>();
            HashMap<Long, Double> map1 = zCodeMapEmd.get(numberOfLine);
            for (long key : map1.keySet()) {
                long[] coord = resolve(key);
                double[] d = new double[3];
                d[0] = Double.parseDouble(String.valueOf(coord[0]));
                d[1] = Double.parseDouble(String.valueOf(coord[1]));
                d[2] = map1.get(key);
                allPoints.add(d);
            }
            //sampleData
            String[] buf = datasetList_after_pooling.get(numberOfLine);
            allHistogram.put(numberOfLine, allPoints);
//            datasetID.add(numberOfLine);
            if (numberOfLine == datasetQueryId) {
                //sampleData
//                query = new double[dimension];
                query[0] = Double.parseDouble(buf[1]);
                query[1] = Double.parseDouble(buf[2]);
                queryRadius = Double.parseDouble(buf[3]);

                int n = allPoints.size();
                FeatureT[] features = new FeatureT[n];
                double[] weights = new double[n];
                for (int i = 0; i < n; i++) {
                    features[i] = new FeatureT(allPoints.get(i)[0], allPoints.get(i)[1]);
                    weights[i] = allPoints.get(i)[2];
                }
                querySignature = new SignatureT(n, features, weights);
            }
            //sampleData
            double[] corrd = new double[config.getDimension()];
            corrd[0] = Double.parseDouble(buf[1]);
            corrd[1] = Double.parseDouble(buf[2]);
            l.add(corrd);
            his.add(Integer.parseInt(buf[0]));
            ub.add(Double.parseDouble(buf[3]));
            numberOfLine++;
        }*/
        //sampleData
//        int countOfRow = numberOfLine;
//        iterMatrix = new double[countOfRow][dimension];
//        ubMove = new double[countOfRow];
//        histogram_name = new int[countOfRow];
//        ubMove = ub.toDoubleArray();
//        for (int i = 0; i < countOfRow; i++) {
//            iterMatrix[i][0] = l.get(i)[0];
//            iterMatrix[i][1] = l.get(i)[1];
//            histogram_name[i] = his.get(i);
//        }
//        return datasetID;
        return querySignature;
    }

    public Map<Integer, String[]> getPooling(HashMap<Integer, HashMap<Long, Double>> mapForDir) {
        Map<Integer, String[]> datasetMap_after_pooling = new HashMap<>();
        pooling p = new pooling();
        mapForDir.forEach((k, v) -> {
            HashMap<Long, Double> map1 = v;
            SignatureT s1 = getSignature(map1);
            SignatureT s1_pooling = null;
            try {
                s1_pooling = p.poolingOperature(s1, 1);
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
            double ub_move = p.getUb();
            String[] strings = new String[4];
            strings[0] = String.valueOf(k);
            strings[1] = String.valueOf(s1_pooling.Features[0].X);
            strings[2] = String.valueOf(s1_pooling.Features[0].Y);
            strings[3] = String.valueOf(ub_move);
            datasetMap_after_pooling.put(k, strings);
        });
        return datasetMap_after_pooling;
    }

    public SignatureT getSignature(HashMap<Long, Double> map) {
        int n = map.size();
        FeatureT[] Features = new FeatureT[n];
        double[] Weights = new double[n];
        long[] Coordinates;
        double unit = 1.0;
        int i = 0;
        for (long key : map.keySet()) {
            Coordinates = resolve(key);
            Features[i] = new FeatureT(Coordinates[0] * unit, Coordinates[1] * unit);
            Weights[i] = map.get(key);
            i++;
        }
        SignatureT s = new SignatureT(n, Features, Weights);
        return s;
    }

    public static SignatureT getSignature(int id, HashMap<Integer, ArrayList<double[]>> allHistogram) {
        ArrayList<double[]> a = allHistogram.get(id);
        int n = a.size();
        FeatureT[] features = new FeatureT[n];
        double[] weights = new double[n];
        for (int i = 0; i < n; i++) {
            features[i] = new FeatureT(a.get(i)[0], a.get(i)[1]);
            weights[i] = a.get(i)[2];
        }
        SignatureT dataSignature = new SignatureT(n, features, weights);
        return dataSignature;
    }

    public long[] resolve(long code) {
        long[] Coordinates = new long[2];
        String str = Long.toBinaryString(code);

        while (str.length() < 2 * config.getResolution()) {
            str = "0" + str;
        }

        StringBuilder c = new StringBuilder();
        StringBuilder d = new StringBuilder();

        for (int i = 0; i < str.length(); i++) {
            if (i % 2 == 0)
                c.append(str.charAt(i));
            else
                d.append(str.charAt(i));
        }

        Coordinates[0] = Long.parseLong(c.toString(), 2);
        Coordinates[1] = Long.parseLong(d.toString(), 2);
        return Coordinates;
    }

    public ArrayList<Integer> getBranchAndBoundResultID(IndexNode root, double[] query) {
        ArrayList<Integer> resultID = new ArrayList<>();
        BranchAndBound(root, query);
        while (!PQ_Branch.isEmpty()) {
            IndexNodeExpand in = PQ_Branch.poll();
            resultID.addAll(in.getIn().getpointIdList());
//            System.out.println("LB = "+ in.lb+"  UB==  "+in.ub);
        }
        return resultID;
    }

    public double distance(double[] x, double[] y) {
        double d = 0.0;
        for (int i = 0; i < x.length; i++) {
            d += (x[i] - y[i]) * (x[i] - y[i]);
        }
        return Math.sqrt(d);
    }

    public static PriorityQueue<IndexNodeExpand> PQ_Branch = new PriorityQueue<>(new ComparatorByIndexNodeExpand());
    public static double LB_Branch = 1000000000;
    public static double UB_Branch = 1000000000;

    public void BranchAndBound(IndexNode root, double[] query) {
        if (root.isLeaf()) {//leaf node
//            System.out.println("distance(root.getPivot(), query)======"+distance(root.getPivot(), query));
//            System.out.println("root.getEMDRadius()====="+root.getEMDRadius());
            double LowerBound = distance(root.getPivot(), query) - root.getEMDRadius();
            double UpperBound = distance(root.getPivot(), query) + root.getEMDRadius();
//            System.out.print(LowerBound < 0);

            IndexNodeExpand in = new IndexNodeExpand(root, LowerBound, UpperBound);
            if (PQ_Branch.isEmpty()) {
                PQ_Branch.add(in);
            } else if (in.getLb() > UB_Branch) {
                //Pruning
            } else if (in.getUb() < LB_Branch) {
//                PQ_Branch.poll();
//                PQ_Branch.add(in);
                while (in.getUb() < LB_Branch) {
                    PQ_Branch.poll();
                    if (!PQ_Branch.isEmpty()) {
                        LB_Branch = PQ_Branch.peek().lb;
                        UB_Branch = PQ_Branch.peek().ub;
                    } else {
                        break;
                    }
                }
                PQ_Branch.add(in);
            } else {
                PQ_Branch.add(in);
            }
            LB_Branch = PQ_Branch.peek().lb;
            UB_Branch = PQ_Branch.peek().ub;
        } else {//internal node
            Set<IndexNode> listnode = root.getNodelist();
            for (IndexNode aListNode : listnode) {
                double LowerBound = distance(aListNode.getPivot(), query) - aListNode.getEMDRadius();
                double UpperBound = distance(aListNode.getPivot(), query) + aListNode.getEMDRadius();
                if (LowerBound > UB_Branch) {
                }//no sense looking here
                else {// if (LowerBound <= UB_Branch )
                    BranchAndBound(aListNode, query);
                }
            }

        }
    }

}