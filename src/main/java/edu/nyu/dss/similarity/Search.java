package edu.nyu.dss.similarity;

import edu.nyu.dss.similarity.datasetReader.SingleFileReader;
import edu.nyu.dss.similarity.index.DataMapPorto;
import edu.rmit.trajectory.clustering.kmeans.IndexAlgorithm;
import edu.rmit.trajectory.clustering.kmeans.IndexNode;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/*
 * this is for top-k dataset search based on similarity search, for evaluation paper
 *
 * 3d object dataset: http://yulanguo.me/dataset.html
 * 2d dataset: porto, tdrive,
 */
@Component
public class Search {

    @Autowired
    private SingleFileReader singleFileReader;

    @Autowired
    private DataMapPorto dataMapPorto;

    //	static boolean scanning = false; // set as true to scan every dataset indexed by our datalake, to simulate the bruteforce baseline
    static boolean scanning = true;

    void kNN() {
        /*
         * we evaluate multiple queries and matching
         */
    }

    /*
     * select random queries to test all the functions
     */
    void generateQueries() {
        //randomly select a set of datasets, or selecting subset of each dataset to compose new
    }

    /*
     * MBR bound from index
     */
    void vldb17() {

    }

    /*
     * expansion based method with grid index
     */
    void sigir18() {

    }

    /*
     * index-nodes
     */
    void sigspatial11() {

    }

    /*
     * incremental search using bounding box, we can
     */
    void vldb11() {

    }

    public void setScanning(boolean scan) {
        scanning = scan;
    }

    /*
     * lower bounds one by one, integrating a bound into the algorithm, as we know the gap between lower and upper, a+2b, then we
     * we can prune by infering the lower bound, need to debug this function.
     */
    public void hausdorffEarylyAbandonging(Map<Integer, double[][]> dataMap, int querySet, Map<Integer, IndexNode> indexMap,
                                           int dimension, int limit, boolean topkEarlyBreaking, boolean saveDatasetIndex,
                                           boolean nonselectedDimension[], boolean dimensionAll, double querydata[][],
                                           Map<Integer, String> argoDataMap, int capacity, double weight[], Map<Integer, IndexNode> queryindexmap, IndexNode querynode, String indexString) throws FileNotFoundException, IOException {
        double min_dis = Double.MAX_VALUE;
        int datasetid = 0;
        double error = 0;
        IndexAlgorithm indexDSS = new IndexAlgorithm();
        //TODO changed
        //when argoDataMap's size bigger than limit would throw npe
        int maxnum = Math.min(limit, argoDataMap.size());
        for (int a = 1; a <=/*limit*/maxnum; a++) {// access the dataset by files or in memory directly, find the datasetMapping to read
            Pair<Double, PriorityQueue<queueMain>> aPair;
            double[][] dataset = null;
            if (dataMap != null) {
                dataset = dataMap.get(a);
            } else {
                dataset = singleFileReader.readSingleFile(argoDataMap.get(a));
                //	System.out.println("aaa"+argoDataMap.get(a));
            }
            IndexNode datanode;
            Map<Integer, IndexNode> datasetNodes = null;
            if (indexMap != null) {
                querynode = indexMap.get(querySet);
                datanode = indexMap.get(a);
            } else {
                if (saveDatasetIndex) {// load from disk
                    datasetNodes = indexDSS.restoreSingleIndex(indexString, datasetid, dimension);
                    datanode = datasetNodes.get(1);
                } else {
                    datanode = indexDSS.buildBalltree2(dataset, dimension, capacity, null, null, weight);
                }
            }
            if (topkEarlyBreaking)//teminate when index is not
                aPair = AdvancedHausdorff.IncrementalDistance(querydata, dataset, dimension, querynode,
                        datanode, 1, 2, error, false, min_dis, topkEarlyBreaking, queryindexmap, datasetNodes, nonselectedDimension, dimensionAll);
            else
                aPair = AdvancedHausdorff.IncrementalDistance(querydata, dataset, dimension, querynode,
                        datanode, 0, 0, 0.01, false, min_dis, topkEarlyBreaking, queryindexmap, datasetNodes, nonselectedDimension, dimensionAll);
            double distance = aPair.getLeft();
            if (min_dis > distance) {
                min_dis = distance;
                datasetid = a;
            }
        }
        //	System.out.println(min_dis+","+datasetid);
    }

    /*
     * lower bounds one by one, compute the lower bound first, rank them, then scan one by one
     */
    public int HausdorffEarylyAbandongingRanking(Map<Integer, double[][]> dataMap, int querySet, Map<Integer, IndexNode> indexMap,
                                                 int dimension, int limit, Map<Integer, Pair<Double, PriorityQueue<queueMain>>> resultPair,
                                                 Map<Integer, IndexNode> nodelist, boolean saveDatasetIndex, boolean nonselectedDimension[],
                                                 boolean dimensionAll, double error, double[][] query, Map<Integer, String> argoDataMap,
                                                 IndexNode querynode, int capacity, double[] weight, String indexString) throws FileNotFoundException, IOException {
        Map<Integer, Double> setBound = new HashMap<Integer, Double>();
        double[][] dataset;
        IndexAlgorithm indexDSS = new IndexAlgorithm();

        //TODO changed
        //when argoDataMap's size bigger than limit would throw npe
        int maxnum = Math.min(limit, argoDataMap.size());
        for (int a = 1; a <=/*limit*/maxnum; a++) {
            if (dataMap != null) {
                dataset = dataMap.get(a);
            } else {
                dataset = singleFileReader.readSingleFile(argoDataMap.get(a));
            }
            IndexNode datanode;
            Map<Integer, IndexNode> datasetNodes = null;
            if (indexMap != null) {
                querynode = indexMap.get(querySet);
                datanode = indexMap.get(a);
            } else {
                if (saveDatasetIndex) {// load from disk
                    datasetNodes = indexDSS.restoreSingleIndex(indexString, a, dimension);
                    datanode = datasetNodes.get(1);
                } else {
                    datanode = indexDSS.buildBalltree2(dataset, dimension, capacity, null, null, weight);
                }
            }
            Pair<Double, PriorityQueue<queueMain>> aPair = AdvancedHausdorff.IncrementalDistance(query, dataset,
                    dimension, querynode, datanode, 1, 2, error, false, 0, false, nodelist, datasetNodes, nonselectedDimension, dimensionAll);
            setBound.put(a, aPair.getLeft() - error);
        }
        LinkedHashMap<Integer, Double> sortedMap = new LinkedHashMap<>();
        setBound.entrySet().stream().sorted(Map.Entry.comparingByValue()).forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));
        double min_dis = Double.MAX_VALUE;
        int datasetid = 0;
        Pair<Double, PriorityQueue<queueMain>> resPair = null;
        for (int a : sortedMap.keySet()) {
            if (min_dis <= setBound.get(a))//the bound
                break;
            if (dataMap != null) {
                dataset = dataMap.get(a);
            } else {
                dataset = singleFileReader.readSingleFile(argoDataMap.get(a));
            }
            IndexNode datanode;
            Map<Integer, IndexNode> datasetNodes = null;
            if (saveDatasetIndex) {// load from disk
                datasetNodes = indexDSS.restoreSingleIndex(indexString, a, dimension);
                datanode = datasetNodes.get(1);
            } else {
                if (indexMap != null) {
                    datanode = indexMap.get(a);
                } else {
                    datanode = indexDSS.buildBalltree2(dataset, dimension, capacity, null, null, weight);
                }
            }
            Pair<Double, PriorityQueue<queueMain>> aPair = AdvancedHausdorff.IncrementalDistance(query, dataset,
                    dimension, querynode, datanode, 0, 1, 0, false, min_dis, false, nodelist, datasetNodes, nonselectedDimension, dimensionAll);
            double distance = aPair.getLeft();
            if (min_dis > distance) {
                min_dis = distance;
                datasetid = a;
                resPair = aPair;
            }
        }
        //	System.out.println(min_dis+","+counter+","+datasetid);
        resultPair.put(datasetid, resPair);
        return datasetid;
    }

    void scanning() {

    }

    /*
     * scanning with index, prune if no intersection at all.
     */
    ArrayList<Integer> rangeQuery(IndexNode datalakeRoot, ArrayList<Integer> result, double querymax[], double querymin[], int dim,
                                  Map<Integer, IndexNode> datalakeIndex) {
        //give a range, find all the dataset that intersect, we just need the data lake tree
        if (datalakeRoot.isRootLeaf()) {
            if (datalakeRoot.intersected(querymax, querymin, dim)) {
                result.add(datalakeRoot.getDatasetID());// add the results into, and ranked by intersected area.
            }
        } else {
            for (IndexNode childNode : datalakeRoot.getNodelist(datalakeIndex)) {
                if (scanning || childNode.intersected(querymax, querymin, dim)) {
                    result = rangeQuery(childNode, result, querymax, querymin, dim, datalakeIndex);
                }
            }
        }
        return result;
    }

    /*
     * scanning with index, compute the intersection, and rank by overlapped range, or number of points.
     */
//	看前人的注释感觉是用的IA度量
//    增加一个条件：结果数据集必须存在点在range当中，而不仅仅是两者范围有交集，这是为了union range query功能做准备
//    增加该条件的坏处是：如果range画得太偏，将不会搜索到结果，因为没有点落在该range里面
    public HashMap<Integer, Double> rangeQueryRankingArea(IndexNode datalakeRoot, HashMap<Integer, Double> result, double querymax[], double querymin[],
                                                          double min_dis, int k, HashMap<Integer, PriorityQueue<queueMain>> queues, int dim, Map<Integer, IndexNode> datalakeIndex) {
        //give a range, find all the dataset that intersect, we just need the data lake tree and ranked by intersected areas
//		只需要data lake tree?
        if (datalakeRoot.isRootLeaf()) {
            double distance = datalakeRoot.intersectedArea(querymax, querymin, dim);
            if (distance < min_dis) {
//                增加一个判断，若存在点在range中，则加入结果集
                if (isDatasetPointInRange(datalakeRoot, querymax, querymin, dim)) {
                    result = holdingTopK(result, datalakeRoot.getDatasetID(), distance, k, queues, null);
                }
                if (result.size() == k)
                    min_dis = result.entrySet().iterator().next().getValue();
                else
                    min_dis = Double.MAX_VALUE;
            }
        } else {
            for (IndexNode childNode : datalakeRoot.getNodelist(datalakeIndex)) {
                double bound = 0;
                if (!scanning) {
                    bound = childNode.intersectedArea(querymax, querymin, dim);
                }
                if (bound < min_dis && bound > 0) {
                    result = rangeQueryRankingArea(childNode, result, querymax, querymin, min_dis, k, queues, dim, datalakeIndex);
                }
            }
        }
        return result;
    }

    /*
     * scanning with index, compute the intersection, and rank by overlapped range, or number of points.
     */
    public HashMap<Integer, Double> rangeQueryRankingNumberPoints(IndexNode datalakeRoot, HashMap<Integer, Double> result,
                                                                  double querymax[], double querymin[],
                                                                  double min_dis, int k, HashMap<Integer, PriorityQueue<queueMain>> queues, int dim,
                                                                  Map<Integer, double[][]> dataMap, Map<Integer, IndexNode> datalakeIndex,
                                                                  Map<Integer, Map<Integer, IndexNode>> datasetindex, Map<Integer, String> DatasetMapping,
                                                                  String indexString, IndexAlgorithm indexDSS, boolean dimSelected[], boolean dimensionAll,
                                                                  ArrayList<double[]> points, Map<Integer, IndexNode> indexMap, int capacity, double weight[], boolean saveDatasetIndex) throws FileNotFoundException, IOException {
        if (datalakeRoot.isRootLeaf()) {
            int datasetid = datalakeRoot.getDatasetID();
            double[][] dataset;
            if (dataMap == null) {
                dataset = singleFileReader.readSingleFile(DatasetMapping.get(datasetid));
            } else {
                dataset = dataMap.get(datasetid);
            }
            Map<Integer, IndexNode> dataindex = null;
            IndexNode dataseNode;
            if (indexMap != null) {// memory mode, all have been created
                dataseNode = indexMap.get(datasetid);
                dataindex = null;
            } else { // disk mode
                if (saveDatasetIndex) {//restore index
                    dataindex = indexDSS.restoreSingleIndex(indexString, datasetid, dim);
                    dataseNode = dataindex.get(1);
                } else {
                    dataseNode = indexDSS.buildBalltree2(dataset, dim, capacity, null, null, weight);
                }
            }
            double distance = -dataseNode.coveredPOints(querymax, querymin, min_dis, dim, dataset, dataindex, dimSelected, dimensionAll, points);
            if (distance < min_dis) {
                result = holdingTopK(result, datasetid, distance, k, queues, null);
                if (result.size() == k)
                    min_dis = result.entrySet().iterator().next().getValue();
                else
                    min_dis = Double.MAX_VALUE;
            }
        } else {
            for (IndexNode childNode : datalakeRoot.getNodelist(datalakeIndex)) {
                int threshold = 0;
                if (!scanning) {
                    threshold = childNode.coveredPOints(querymax, querymin, dim, dimSelected, dimensionAll);
                }
                if (scanning || -threshold < min_dis && threshold > 0) {
                    result = rangeQueryRankingNumberPoints(childNode, result, querymax, querymin, min_dis, k,
                            queues, dim, dataMap, datalakeIndex, datasetindex, DatasetMapping, indexString, indexDSS, dimSelected, dimensionAll, points, indexMap, capacity, weight, saveDatasetIndex);
                }
            }
        }
        return result;
    }


    /*
     * posting list, similar to term and document, but not used
     */
    void createPostinglists(HashMap<Integer, ArrayList<Integer>> dataset) {
        HashMap<Integer, ArrayList<Integer>> postlisitngHashMap = new HashMap<Integer, ArrayList<Integer>>();
        for (int datasetid : dataset.keySet()) {
            ArrayList<Integer> signatureArrayList = dataset.get(datasetid);
            for (int code : signatureArrayList) {
                ArrayList<Integer> datasetList;
                if (postlisitngHashMap.containsKey(code))
                    datasetList = postlisitngHashMap.get(code);
                else
                    datasetList = new ArrayList<Integer>();
                if (!datasetList.contains(datasetid))
                    datasetList.add(datasetid);
                postlisitngHashMap.put(code, datasetList);
            }
        }
    }

    /*
     * the similarity is based on the intersection of z-curve code of each datasets: grid-based overlap, GBO
     * we design an inverted index to accelerate the pair-wise and top-k, for node,
     * we can get a upper bound, and it is more efficient then the inverted index only
     */
    public HashMap<Integer, Double> gridOverlap(IndexNode datalakeRoot, HashMap<Integer, Double> result, int[] query,
                                                double min_dis, int k, HashMap<Integer, PriorityQueue<queueMain>> queues, Map<Integer, IndexNode> datalakeIndex) {
        // give a query signature, find all the dataset that has a ratio,
        if (datalakeRoot.isRootLeaf()) {
            int datasetid = datalakeRoot.getDatasetID();
            double distance = datalakeRoot.GridOverlap(query);
            if (distance < min_dis) {
                result = holdingTopK(result, datasetid, distance, k, queues, null);
                if (result.size() == k)
//                    min_dis = result.entrySet().iterator().next().getValue();
                    result.entrySet().iterator().next().getValue();
//                else
//                    min_dis = Double.MAX_VALUE;
            }
        } else {
            for (IndexNode childNode : datalakeRoot.getNodelist(datalakeIndex)) {
                if (scanning || childNode.GridOverlap(query) < min_dis) {
                    result = gridOverlap(childNode, result, query, min_dis, k, queues, datalakeIndex);
                }
            }
        }
        return result;
    }


    /*
     * access the posting list to filter other datasets, get a candidate list, then call the baseline
     */
    HashMap<Integer, Double> gridOverlapPosstingList(ArrayList<Integer> query, HashMap<Integer, ArrayList<Integer>> postlisitngHashMap,
                                                     Map<Integer, ArrayList<Integer>> bitMap, int k) {
        Set<Integer> candidateDatasetArrayList = new HashSet<Integer>();
        HashMap<Integer, Double> result = new HashMap<Integer, Double>();
        HashMap<Integer, PriorityQueue<queueMain>> queues = new HashMap<Integer, PriorityQueue<queueMain>>();
        for (int code : query) {
            if (postlisitngHashMap.containsKey(code))
                candidateDatasetArrayList.addAll(postlisitngHashMap.get(code));
        }
        for (int datasetid : candidateDatasetArrayList) {
            ArrayList<Integer> arrayList = bitMap.get(datasetid);
            int counter = 0;
            for (int queryz : query) {
                if (arrayList.contains(queryz)) {
                    counter++;
                }
            }
            double reverseoverlapRatio = 1 - counter / (double) query.size();
            result = holdingTopK(result, datasetid, reverseoverlapRatio, k, queues, null);
        }
        return result;
    }

    /*
     * we use hashmap to hold top-k
     */
    HashMap<Integer, Double> holdingTopK(HashMap<Integer, Double> result, int datasetid, double score, int k,
                                                HashMap<Integer, PriorityQueue<queueMain>> queues, PriorityQueue<queueMain> usedQueue) {
        if (result.size() < k) {
            result.put(datasetid, score);
            if (queues != null)
                queues.put(datasetid, usedQueue);
            if (result.size() == k) {
                HashMap<Integer, Double> sortedMap = new LinkedHashMap<>();
                result.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                        .forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));
                result = sortedMap;
            }
        } else {
            Entry<Integer, Double> a = result.entrySet().iterator().next();
            if (score < a.getValue()) {
                result.remove(a.getKey());
                if (queues != null)
                    queues.remove(a.getKey());
                result.put(datasetid, score);
                if (queues != null)
                    queues.put(datasetid, usedQueue);
                HashMap<Integer, Double> sortedMap = new LinkedHashMap<>();
                result.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                        .forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));
                result = sortedMap;
            }
        }
        return result;
    }

    /*
     * Hausdorff, we prune those index and return candidates for further purning using sequential methods
     * 样例查询的算法
     */
    public HashMap<Integer, Double> pruneByIndex(Map<Integer, double[][]> dataMap, IndexNode datalakeRoot, IndexNode query,
                                                 int dimension, Map<Integer, IndexNode> indexMap,
                                                 Map<Integer, Map<Integer, IndexNode>> nodelist1, Map<Integer, IndexNode> datalakeIndex,
                                                 Map<Integer, String> argoDataMap, int k, String indexString, boolean nonselectedDimension[], boolean dimensionAll,
                                                 double error, int capacity, double weight[], boolean saveDatasetIndex, double[][] querydata) throws FileNotFoundException, IOException {
//        为了提高查询准确度，不管前端提供的k为多少，统一查询前100个近邻数据集，然后再取前k个
        int constantK = 100;
        PriorityQueue<queueForNode> aForNodes = new PriorityQueue<queueForNode>();
        queueForNode qNodes;
        if (datalakeIndex != null)
            qNodes = new queueForNode(query, datalakeIndex.get(1));// root node
        else {
            qNodes = new queueForNode(query, datalakeRoot);
        }
        aForNodes.add(qNodes);
        double min_dis = Double.MAX_VALUE;
        int counter = 0;
        IndexAlgorithm indexDSS = new IndexAlgorithm();
        HashMap<Integer, Double> result = new HashMap<>();
        HashMap<Integer, PriorityQueue<queueMain>> queues = new HashMap<>();
        while (!aForNodes.isEmpty()) {
            queueForNode aForNodes2 = aForNodes.poll();
            IndexNode aIndexNode = aForNodes2.getNode();
            double lowerbound = aForNodes2.getbound();
            if (lowerbound > min_dis) {
                break;
            }
            if (aIndexNode.isRootLeaf()) {//we get to the dataset level
                int datasetid = aIndexNode.getDatasetID();
                double[][] dataset;
                if (dataMap != null) {
                    dataset = dataMap.get(datasetid);
                } else {
                    dataset = singleFileReader.readSingleFile(argoDataMap.get(datasetid));
                }
                Pair<Double, PriorityQueue<queueMain>> aPair;
                if (nodelist1 != null) // index loaded from disk
                    aPair = AdvancedHausdorff.IncrementalDistance(querydata, dataset, dimension, query,
                            nodelist1.get(datasetid).get(1), 0, 1, error, false, min_dis, false, null, nodelist1.get(datasetid), nonselectedDimension, dimensionAll);
                else {// index not loaded in disk
                    if (indexMap != null)// index in memory
                        aPair = AdvancedHausdorff.IncrementalDistance(querydata, dataset, dimension, query,
                                indexMap.get(datasetid), 0, 1, error, true, min_dis, true, null, null, nonselectedDimension, dimensionAll);
                    else {// index not in memory
                        if (saveDatasetIndex) {// load from disk
                            Map<Integer, IndexNode> dataindexMap = indexDSS.restoreSingleIndex(indexString, datasetid, dimension);
                            aPair = AdvancedHausdorff.IncrementalDistance(querydata, dataset, dimension, query,
                                    dataindexMap.get(1), 0, 1, error, false, min_dis, false, null, dataindexMap, nonselectedDimension, dimensionAll);
                        } else {    // create index if both memory and disk version are not available
                            IndexNode rootBall = indexDSS.buildBalltree2(dataset, dimension, capacity, null, null, weight);
                            aPair = AdvancedHausdorff.IncrementalDistance(querydata, dataset, dimension, query,
                                    rootBall, 0, 1, error, false, min_dis, false, null, null, nonselectedDimension, dimensionAll);
                        }
                    }
                }
                double distance = aPair.getLeft();
                result = holdingTopK(result, datasetid, distance, constantK, queues, aPair.getRight());
                if (result.size() == constantK)
                    min_dis = result.entrySet().iterator().next().getValue();
                else
                    min_dis = Double.MAX_VALUE;
                counter++;
            } else {
                for (IndexNode childIndexNode : aIndexNode.getNodelist(datalakeIndex)) {
                    //	System.out.println(childIndexNode.getRadius());
                    qNodes = new queueForNode(query, childIndexNode);
                    aForNodes.add(qNodes);
                }
            }
        }
	/*	for(int datastid: result.keySet()) {
			System.out.println(datastid+", "+result.get(datastid));
		}
		System.out.println("\n"+min_dis+", "+counter);*/
//        再取前k个
//        转化为列表
        List<Map.Entry<Integer, Double>> entryList = new ArrayList<>(result.entrySet());
//        获取最后k个元素
        List<Map.Entry<Integer, Double>> lastKEntries = entryList.stream()
                .skip(entryList.size() - k)
                .toList();
//        存储到新的HashMap
        HashMap<Integer, Double> resultK = new HashMap<>();
        lastKEntries.forEach(entry -> resultK.put(entry.getKey(), entry.getValue()));
        return resultK;
    }

    // My custom method
//    Union算法，不知道还有没有别的功能在里面
    public List<double[]> UnionRangeQueryForPoints(double[] querymax, double[] querymin, int unionDatasetId, IndexNode unionDatasetNode, List<double[]> result, int dim,
                                                   boolean nonselectedDimension[], boolean dimensionAll) {
        // not leaf node
        if (unionDatasetNode.getpointIdList() == null || unionDatasetNode.getpointIdList().size() == 0) {
            for (IndexNode childNode : unionDatasetNode.getNodelist()) {
                if (childNode.intersected(querymax, querymin, dim, nonselectedDimension, dimensionAll))
                    result = UnionRangeQueryForPoints(querymax, querymin, unionDatasetId, childNode, result, dim, nonselectedDimension, dimensionAll);
            }
        } else {
            // leaf node
            for (Integer rowId : unionDatasetNode.getpointIdList()) {
                if (isPointInRange(dataMapPorto.get(unionDatasetId)[rowId], querymax, querymin, dim, nonselectedDimension, dimensionAll))
                    result.add(dataMapPorto.get(unionDatasetId)[rowId]);
            }
        }
//        返回所有落在query bound中的数据集中的点
        return result;
    }

    private static boolean isPointInRange(double[] point, double[] mbrmax, double[] mbrmin, int dim, boolean selectedDimension[], boolean dimensionAll) {
//        暂时不考虑选择dimension的情况
        if (selectedDimension != null && !dimensionAll) {
            for (int i = 0; i < dim; i++)
                if (mbrmax[i] < mbrmin[i]) {
                    return false;
                }
            return true;
        } else {
            return isPointInRange(point, mbrmax, mbrmin, dim);
        }
    }

    private static boolean isPointInRange(double[] point, double[] mbrmax, double[] mbrmin, int dim) {
        for (int i = 0; i < dim; i++)
            if (point[i] > mbrmax[i] || point[i] < mbrmin[i]) {
                return false;
            }
        return true;
    }

    //    判断一个数据集中有没有点在range中
    private boolean isDatasetPointInRange(IndexNode root, double[] mbrmax, double[] mbrmin, int dim) {
        double[][] data = dataMapPorto.get(root.getDatasetID());
        List<double[]> dataList = Arrays.asList(data);
        Collections.shuffle(dataList);
        for (double[] d : dataList) {
            if (isPointInRange(d, mbrmax, mbrmin, dim)) {
                return true;
            }
        }
        return false;
    }

}
