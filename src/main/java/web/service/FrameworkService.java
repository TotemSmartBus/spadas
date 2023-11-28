package web.service;

import edu.nyu.dss.similarity.*;
import edu.nyu.dss.similarity.config.SpadasConfig;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import web.Utils.ListUtil;
import web.VO.DatasetVo;
import web.VO.JoinVO;
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
    private EffectivenessStudy effectivenessStudy;

    @Autowired
    private DatasetProperties datasetProperties;

    public List<DatasetVo> rangequery(RangeQueryParams qo) {
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
        for (int i = 0; i < datasetIdMapping.size(); i++) {
            if (datasetIdMapping.get(i) != null && datasetIdMapping.get(i).contains(qo.getKws())) {
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
                config.getDimension(), indexMap, datasetIndex, null, datalakeIndex, datasetIdMapping, k,
                indexString, null, true, 0, config.getLeafCapacity(), null, config.isSaveIndex(), data);
        List<DatasetVo> finalResult = new ArrayList<>();
        finalResult.addAll(result.entrySet().stream().sorted((o1, o2) -> (int) (o1.getValue() - o2.getValue())).
                map(i -> new DatasetVo(i.getKey(), indexMap.get(i.getKey()), datasetIdMapping.get(i.getKey()), dataSamplingMap.get(i.getKey()), indexMap.get(i.getKey()).getTotalCoveredPoints() < config.getFrontendLimitation() ? dataMapPorto.get(i.getKey()) : null)).toList());
        return finalResult.subList(0, k);
    }

    public List<DatasetVo> datasetQuery(DatasetQueryParams qo) throws IOException {
//        indexNode queryNode;
        IndexNode queryNode = indexMap.get(qo.getDatasetId());
        double[][] data = dataMapPorto.get(qo.getDatasetId());
        HashMap<Integer, Double> result = new HashMap<>();

        switch (qo.getMode()) {
            case 0 -> // HausDist
                    result = search.pruneByIndex(dataMapPorto, framework.datasetRoot, queryNode,
                            qo.getDim(), indexMap, datasetIndex, null, datalakeIndex, datasetIdMapping, qo.getK(),
                            indexString, null, true, qo.getError(), config.getLeafCapacity(), null, config.isSaveIndex(), data);
            case 1 -> { // Intersecting Area
                search.setScanning(false);
                search.rangeQueryRankingArea(framework.datasetRoot, result, queryNode.getMBRmax(), queryNode.getMBRmin(), Double.MAX_VALUE, qo.getK(), null, qo.getDim(),
                        datalakeIndex);
            }
            case 2 -> { // GridOverlap using index
                IndexNode root = framework.datasetRoot;//datalakeIndex.get(1);//use store

//            if (datalakeIndex != null)
//                root = datalakeIndex.get(1);
//            int[] queryzcurve = new int[zcodemap.get(queryid).size()];
                int[] queryzcurve = zCodeMap.get(qo.getDatasetId()).stream().mapToInt(Integer::intValue).toArray();
//            for (int i = 0; i < zcodemap.get(queryid).size(); i++)
//                queryzcurve[i] = zcodemap.get(queryid).get(i);
                result = search.gridOverlap(root, result, queryzcurve, Double.MAX_VALUE, qo.getK(), null, datalakeIndex);
            }
            default -> {
            }
        }

        List<DatasetVo> finalResult = new ArrayList<>();
        finalResult.addAll(result.entrySet().
                stream()
                .sorted((o1, o2) -> (int) (o1.getValue() - o2.getValue()))
                .map(item -> new DatasetVo(indexMap.get(item.getKey()), datasetIdMapping.get(item.getKey()), item.getKey(), dataMapPorto.get(item.getKey()))).
                toList());
        return finalResult.subList(0, qo.getK());
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


    public JoinVO join(int queryId, int datasetId, int rowLimit) throws IOException {
        Pair<Double[], Map<Integer, Integer>> pair = pairwiseJoin(queryId, datasetId, rowLimit);

//        Map<Integer, Integer> map = pair.getRight();
//        double[][] datasetData = dataMapPorto.get(datasetId);
////        Pair<String[], String[][]> querydata = FileU.readPreviewDataset(fileIDMap.get(queryId), rowLimit, queryData);
////        Pair<String[], String[][]> basedata = FileU.readPreviewDataset(fileIDMap.get(datasetId), Integer.MAX_VALUE, datasetData);
//
//        double[][] queryData = Arrays.copyOfRange(dataMapPorto.get(queryId), 0, 100);
//        double[][] targetData = Arrays.copyOfRange(dataMapPorto.get(datasetId), 0, 100);
//        //int len = querydata.getRight()[0].length+basedata.getRight()[0].length;
//        String[] distHeader = {"distance(km)"};
//        String[] joinHeaderTemp = ArrayUtils.addAll(querydata.getLeft(), distHeader);
//        String[] joinHeader = ArrayUtils.addAll(joinHeaderTemp, basedata.getLeft());
////        List<String[]> joindata = pair.getRight().entrySet().stream()
////                .map(idPair -> ArrayUtils.addAll(querydata.getRight()[idPair.getValue()], basedata.getRight()[idPair.getValue()])).collect(Collectors.toList());
//        List<List<String>> joinData = new ArrayList<>();
//        String[] queryEntry;
//        String[] baseEntry;
//        Double[] distEntry = pair.getLeft();
//        String[] distEntryTemp;
//        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
//            int queryIndex = entry.getKey();
//            int datasetIndex = entry.getValue();
//            queryEntry = querydata.getRight()[queryIndex];
//            baseEntry = basedata.getRight()[datasetIndex];
//            double tmp = Math.round(distEntry[queryIndex] * 1000) / 1000.000;
//
//            String tmpStr = tmp < 5 ? String.valueOf(tmp) : "INVALID";
//            distEntryTemp = new String[]{tmpStr};
//            joinData.add(Arrays.stream(ArrayUtils.addAll(ArrayUtils.addAll(queryEntry, distEntryTemp), baseEntry)).toList());
//        }
        JoinVO result = new JoinVO();
//        result.setHeader(Arrays.stream(joinHeader).toList());
//        result.setBody(joinData);
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
        } catch (IOException e) {
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
}
