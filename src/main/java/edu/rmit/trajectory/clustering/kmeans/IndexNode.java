package edu.rmit.trajectory.clustering.kmeans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.rmit.trajectory.clustering.kpaths.Util;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@JsonIgnoreProperties({"assignedCluster", "bounds", "prunedCounter"})
@Data
// this class will build the index based on the
public class IndexNode {
    int type; // 0 for line , 1 for point
    //    有些属性不能传
    @JsonIgnore
    protected Set<Integer> pointIdList;
    @JsonIgnore// the leaf node, the index node is a leaf node when this is not empty, we can
    protected Set<IndexNode> nodeList; // the internal node
    protected Set<Integer> nodeIDList;//check whether the root is empty before use, for seralization
    protected double[] pivot;// the mean value
    protected double radius;// the radius from the pivot to the furthest point
    protected double distanceToFarther;//this is the distance to father for bound estimation
    protected double[] sum;// the sum of all the points inside this node.
    double[] bounds;//the lower bound distance to the non nearest neighbor;
    private int totalCoveredPoints;

    //    用来计算EMD
//    EMDRadius = radius + minUbMove
    double EMDRadius;
    //    一个节点内部所有点的UbMove的最小值？
    double minUbMove;
    //    只有数据集文件的根节点会有的文件名属性
    String fileName;


    //  used for pick-means
    short assignedClusterID = 0;// it is set as 0 when not assigned, >0 is assigned in current iteration, <0 show the pervious assigned cluster
    short newassign = 0; // used to store nearest
    double lowerbound = 0;// set as the bound to second nearest neighbor
    double upperbound = Double.MAX_VALUE;// set as the bound to nearest centroid, assigned if upperbound < lowerbound
    short counterPruned = 0;// initialized as zero


    // used for dataset search
    public int rootToDataset = 0;// indicate the dataset id, for dataset search use only
    double mindis = Double.MAX_VALUE;
    int counter = 0; //number of nodes in the queue
    int nodeid = 0;//for node identification
    double[] mbrmax;//MBR bounding box
    double[] mbrmin;

    public void setSignautre(int[] signautre) {
        this.signautre = signautre;
    }

    int[] signautre = new int[2]; // store the signature
    int maxCoverpoints;// store the maximum number of point under it, for data lake index only, this for pruning

    // used for fair clustering
    private double totalCoveredPointsFair = 0; //calculate the normalized

    //    EMD相关
    public void setEMDRadius(double radius, double ubMove) {
        this.EMDRadius = radius + ubMove;
    }

    public double getEMDRadius() {
        return EMDRadius;
    }


    long getMemory(int dimension) {
        long all = 0;
        all += pointIdList.size() * 4 + dimension * 8 + 8 + 8 + dimension * 8;
        if (bounds != null)
            all += bounds.length * 8;
        all += 4;
        return all;
    }


    /*
     * get the space of lightweight index, just consider the pivot and radius, number of points
     */
    long getMemoryPick(int dimension, Boolean centroid) {
        long all = 0;
        all += pointIdList.size() * 4 + dimension * 8 + 8 + 4;
        if (centroid)
            all -= 4;
        if (bounds != null)
            all += bounds.length * 8;
        return all;
    }

    /*
     * calculate index size for dataset search engine
     */
    public long getMemorySpadas(int dimension, Boolean mbr) {
        long all = 0;
        if (pointIdList != null)
            all += pointIdList.size() * 4;
        if (nodeIDList != null)
            all += nodeIDList.size() * 4;
        if (signautre != null)
            all += signautre.length * 4;
        all += dimension * 8 * 3 + 8 + 4 + 8;
        return all;
    }


    public IndexNode(int dimension) {
        pointIdList = new HashSet<>();
        nodeList = new HashSet<>();
        pivot = new double[dimension];
        mbrmax = new double[dimension];// only for dataset search
        mbrmin = new double[dimension]; // only for dataset search
        radius = Double.MAX_VALUE; // this is for the root node
        sum = new double[dimension];
    }


    /*
     * for loading index of each dataset
     */
    public void addEveryThing(String[] pivotpoint, String[] basic, String[] list, String[] mbr, int dimension) {
        nodeIDList = new HashSet<>();
        //	nodeList = null;
        nodeid = Integer.valueOf(basic[0]);
        totalCoveredPoints = Integer.valueOf(basic[1]);
        distanceToFarther = Double.valueOf(basic[2]);
        radius = Double.valueOf(basic[3]);
        if (basic[4].equals("0")) {// leaf node
            for (String aString : list) {
                if (!aString.equals("")) {
                    pointIdList.add(Integer.valueOf(aString));
                }
            }
        } else {//internal node
            for (String aString : list) {
                if (!aString.equals(""))
                    nodeIDList.add(Integer.valueOf(aString));
            }
        }
        for (int i = 0; i < pivot.length; i++) {
            pivot[i] = Double.valueOf(pivotpoint[i]);
        }
        mbrmax = new double[dimension];
        mbrmin = new double[dimension];
        for (int i = 0; i < dimension; i++) {//add the mbr information
            mbrmax[i] = Double.valueOf(mbr[i]);
            mbrmin[i] = Double.valueOf(mbr[i + dimension]);
        }
    }

    /*
     * rebuild datalate index node from file
     */
    public void addEveryThingDatalake(String[] pivotpoint, String[] basic, String[] list, String[] mbr, int dimension) {
        nodeIDList = new HashSet<>();
        nodeid = Integer.valueOf(basic[0]);
        totalCoveredPoints = Integer.valueOf(basic[1]);
        distanceToFarther = Double.valueOf(basic[2]);
        radius = Double.valueOf(basic[3]);
        if (basic[4].equals("0")) {// leaf node
            for (String aString : list) {
                if (!aString.equals(""))
                    rootToDataset = Integer.valueOf(aString);
            }
        } else {//internal node
            for (String aString : list) {
                if (!aString.equals(""))
                    nodeIDList.add(Integer.valueOf(aString));
            }
            if (basic[4].equals("1"))
                rootToDataset = -1;
            else
                rootToDataset = -2;
        }
        for (int i = 0; i < pivot.length; i++) {
            pivot[i] = Double.valueOf(pivotpoint[i]);
        }
        mbrmax = new double[dimension];
        mbrmin = new double[dimension];
        for (int i = 0; i < dimension; i++) {//add the mbr information
            mbrmax[i] = Double.valueOf(mbr[i]);
            mbrmin[i] = Double.valueOf(mbr[i + dimension]);
        }
    }


    public void addNodeIds(int nodeid) {
        if (nodeIDList == null)
            nodeIDList = new HashSet<>();
        nodeIDList.add(nodeid);
    }

    public void addNodes(IndexNode newNode) {
        nodeList.add(newNode);
    }

    public void emptyNodes() {
        nodeList = null;
    }

    public void clearNodes() {
        nodeList = new HashSet<IndexNode>();
    }

    public void addPoint(Set<Integer> newPoint) {
        pointIdList.addAll(newPoint);
    }

    public void addPoint(int point) {
        pointIdList.add(point);
    }

    public void addSinglePoint(int newPoint) {
        pointIdList.add(newPoint);
    }

    public void setRadius(double radius) {
        this.radius = radius;
    }

    public void setroot(int datasetID) {
        this.rootToDataset = datasetID;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return this.fileName;
    }

    public boolean isRootLeaf() {
        if (rootToDataset > 0)
            return true;
        else {
            return false;
        }
    }

    public int getDatasetID() {
        return rootToDataset;
    }

    public void setDatasetID(int id) {
        this.rootToDataset = id;
    }

    public void setdistanceToFarther(double distanceToFarther) {
        this.distanceToFarther = distanceToFarther;
    }

    public void setSum(double[] sum) {
        for (int i = 0; i < sum.length; i++)
            this.sum[i] = sum[i];
    }

    public void addSum(double[] sum) {
        for (int i = 0; i < sum.length; i++)
            this.sum[i] += sum[i];
    }


    public void setPivot(double[] pivot) {
        System.arraycopy(pivot, 0, this.pivot, 0, pivot.length);
    }

    public void setTotalCoveredPoints(int totalCoveredPoints) {
        this.totalCoveredPoints = totalCoveredPoints;
    }

    public void setBounds(double bounds[], int groupNumber) {
        this.bounds = new double[groupNumber + 2];
        this.bounds[0] = bounds[0] + distanceToFarther;
        for (int i = -1; i < groupNumber; i++)
            this.bounds[i + 2] = bounds[i + 2] - distanceToFarther;//update the bound with the distance from the farther to child
    }

    public void updateBoundPick(double bound) {
        this.upperbound = bound;
    }


    public void setBoundsEmpty() {
        this.bounds = null;
    }

    public void setLowerBoundForRegroup(int dimension) {
        if (this.bounds != null)
            for (int i = 0; i < dimension; i++)
                this.bounds[i + 2] = 0;
    }

    public void setUpperbound(double Upperbound) {
        this.bounds[0] = Upperbound;
    }

    public void setLowerbound(double lowerbound) {
        this.bounds[1] = lowerbound;
    }

    protected void updateSingleLowerBound(int traid, int group_i, double newbound, int groupNumber) {
        if (bounds == null) {
            bounds = new double[groupNumber + 2];
            bounds[0] = Double.MAX_VALUE;
            for (int i = 1; i < groupNumber + 2; i++) {
                bounds[i] = 0;//initialize as the maximum value
            }
            if (groupNumber > 0)
                bounds[group_i + 2] = newbound;
        } else {
            if (group_i + 2 >= bounds.length) {//update the size from last round
                bounds = new double[groupNumber + 2];
                bounds[0] = Double.MAX_VALUE;
                for (int i = 1; i < groupNumber + 2; i++) {
                    bounds[i] = 0;//initialize as the maximum value
                }
                if (groupNumber > 0)
                    bounds[group_i + 2] = newbound;
            } else {
                if (groupNumber > 0 && bounds[group_i + 2] > newbound)
                    bounds[group_i + 2] = newbound;
            }
        }
    }

    public boolean isLeaf() {
        if (pointIdList.isEmpty())
            return false;
        else
            return true;
    }

    public Set<IndexNode> getNodelist() {
        return nodeList;
    }

    public Set<IndexNode> getNodelist(Map<Integer, IndexNode> nodelists) {
        if (nodelists == null)
            return nodeList;
        if (nodeIDList == null)
            return new HashSet<>();
        Set<IndexNode> nodelistIndexNodes = new HashSet<>();
        for (int nodeid : nodeIDList) {
            nodelistIndexNodes.add(nodelists.get(nodeid));
        }
        return nodelistIndexNodes;
    }

    public Set<Integer> getpointIdList() {
        return pointIdList;
    }

    /*
     * full dimension search
     */
    public double getRadius() {
        return radius;
    }

    /*
     * get the radius according to the selected dimension to achieve pruning, used for Hausdorff
     */
    public double getRadius(boolean selectedDimension[], int dimension, boolean dimensionAll, double weight[]) {
        if (selectedDimension != null && dimensionAll == false) {
            float sum = 0;
            //	System.out.println(weight.length);
            for (int i = 0; i < dimension; i++)
                if (selectedDimension[i] == false)//selected dimension, and also need to consider the weights for computing radius
                    sum += Math.pow((mbrmax[i] - mbrmin[i]) / 2 * weight[i], 2);
            return Math.sqrt(sum);//the single dimension
        } else { // the full radius
            return radius;
        }
    }


    public double[] getSum() {
        return sum;
    }

    public double[] getPivot() {
        return pivot;
    }

    public double[] getBounds() {
        return bounds;
    }

    public int getTotalCoveredPoints() {
        return totalCoveredPoints;
    }

    public void getAllCoveredPoints(ArrayList<Integer> arrayList) {
        if (isLeaf()) {
            arrayList.addAll(pointIdList);
        } else {
            for (IndexNode child : nodeList) {
                child.getAllCoveredPoints(arrayList);
            }
        }
    }

    public void getAllCoveredNodes(ArrayList<IndexNode> arrayList) {
        if (!nodeList.isEmpty())
            arrayList.addAll(nodeList);
        for (IndexNode child : nodeList) {
            child.getAllCoveredNodes(arrayList);
        }
    }

    public void increaseCounter() {
        counter++;
        for (IndexNode child : nodeList) {
            child.increaseCounter();
        }
    }

    public void decreaseCounter() {
        counter++;
        for (IndexNode child : nodeList) {
            child.decreaseCounter();
        }
    }

    public int getcounter() {
        return counter;
    }

    public double getDisFather() {
        return distanceToFarther;
    }

    public void updateBound(double newdis) {
        if (newdis < mindis)
            mindis = newdis;
    }

    public double getMinDis() {
        return mindis;
    }

    public double getPivotdistance(double[] pivota) {
        return Util.EuclideanDis(pivota, pivot, pivota.length);
    }

    public void setNodeid(int nodeid) {
        this.nodeid = nodeid;
    }

    public int setNodeid() {
        return nodeid;
    }

    public void setMBRmin(double[] mbrmin) {
        //	this.mbrmin = new double[mbrmin.length];
        for (int i = 0; i < mbrmin.length; i++)
            this.mbrmin[i] = mbrmin[i];
    }

    public void setMBRmax(double[] mbrmax) {
        //	this.mbrmax = new double[mbrmax.length];
        for (int i = 0; i < mbrmax.length; i++) {
            this.mbrmax[i] = mbrmax[i];
        }
    }


    public double[] getMBRmin() {
        return mbrmin;
    }

    public double[] getMBRmax() {
        return mbrmax;
    }

    public void removeNode(IndexNode tempIndexNode) {
        nodeList.remove(tempIndexNode);
    }

    /*
     * selected dimensions
     */
    public boolean intersected(double querymax[], double querymin[], int dim, boolean selectedDimension[], boolean dimensionAll) {
        if (selectedDimension != null && !dimensionAll) {
            for (int i = 0; i < dim; i++)
                if (!selectedDimension[i] && mbrmin[i] > querymax[i] || mbrmax[i] < querymin[i]) {
                    return false;
                }
            return true;
        } else {
            return intersected(querymax, querymin, dim);
        }
    }

    public boolean intersected(double querymax[], double querymin[], int dim) {
        for (int i = 0; i < dim; i++)
            //	if(mbrmin[i] > 0 || mbrmax[i] < 0) {
            if (mbrmin[i] > querymax[i] || mbrmax[i] < querymin[i]) {
                return false;
            }
        return true;
    }

    /*
     * compute the range of intersection
     * TODO range query ????????????????
     */
    public double intersectedArea(double querymax[], double querymin[], int dim) {
        if (!intersected(querymax, querymin, dim)) {
            return 0;
        } else {
            /*boolean isQueryBigEnough = false;
            double area = 1;
            for (int i = 0; i < dim; i++) {
                if (querymin[i] < mbrmin[i] || querymax[i] > mbrmax[i]) {
                    isQueryBigEnough = true;
                }
                if (querymin[i] < mbrmin[i]) {
                    area *= Math.min(querymax[i], mbrmax[i]) - mbrmin[i];
                } else {
                    area *= Math.min(querymax[i], mbrmax[i]) - querymin[i];
                }
            }
            if (!isQueryBigEnough) {
                area = 1;
                for (int i = 0; i < dim; i++) {
                    area *= mbrmax[i] - mbrmin[i];
                }
            }
            return Math.abs(area);*/
            double area = 1, areaQ = 1, areaD = 1;
            for (int i = 0; i < dim; i++) {
                if (querymin[i] < mbrmin[i])
                    area *= Math.min(querymax[i], mbrmax[i]) - mbrmin[i];
                else
                    area *= Math.min(querymax[i], mbrmax[i]) - querymin[i];
                areaD *= mbrmax[i] - mbrmin[i];
                areaQ *= querymax[i] - querymin[i];
            }
            double similarity = 0;
            if (area == areaD) {
                similarity = Math.abs(area / areaQ);
            } else if (area == areaQ) {
                similarity = Math.abs(area / areaD);
            } else {
                similarity = Math.abs(Math.max(area / areaD, area / areaQ));
            }
            return 1 / similarity;
        }
    }

    /*
     * compute the range of intersection
     */
//    需要考虑 查询范围远大于节点范围 和 节点范围远大于查询范围 的情况
//    当前的思路是：结果是较小的mbr
//    这就会存在区分度弱的问题
//    建议改成Min(O/Q, O/D)
//    灵感来自李禛的毕业论文第10页
    public double intersectedArea(double querymax[], double querymin[], int dim, boolean nonselectedDimension[], boolean dimensionAll) {
        return intersectedArea(querymax, querymin, dim);
    }

    /*
     * covered by the query absolutely.
     */
    public boolean coverByQuery(double querymax[], double querymin[], int dim) {
        for (int i = 0; i < dim; i++)
            if (mbrmax[i] > querymax[i] || mbrmin[i] < querymin[i])
                return false;
        return true;
    }

    /*
     * covered by the query absolutely.
     */
    public boolean coverByQuery(double querymax[], double querymin[], int dim, boolean nonselectedDimension[], boolean dimensionAll) {
        if (nonselectedDimension != null && dimensionAll == false) {
            for (int i = 0; i < dim; i++)
                if (nonselectedDimension[i] == false && mbrmax[i] > querymax[i] || mbrmin[i] < querymin[i])
                    return false;
            return true;
        } else {
            return coverByQuery(querymax, querymin, dim);
        }
    }


    /*
     * return the bound of a datalake node for pruning only
     */
    public int coveredPOints(double querymax[], double querymin[], int dim) {
        if (intersected(querymax, querymin, dim))
            return maxCoverpoints;
        else {
            return 0;
        }
    }

    /*
     * return the bound of a datalake node for pruning only, for selected dimension only
     */
    public int coveredPOints(double querymax[], double querymin[], int dim, boolean nonselectedDimension[], boolean dimensionAll) {
        if (intersected(querymax, querymin, dim, nonselectedDimension, dimensionAll)) {
            return maxCoverpoints;
        } else {
            return 0;
        }
    }


    /*
     * get the number of covered points exactly
     */
    public int coveredPOints(double querymax[], double querymin[], double dis, int dim, double[][] dataset, Map<Integer, IndexNode> nodelist) {
        if (!intersected(querymax, querymin, dim)) {
            return 0;
        } else {
            if (coverByQuery(querymax, querymin, dim))
                return totalCoveredPoints;
            int cover = 0;
            if (pointIdList.size() > 0) {//leaf node
                for (int pointid : pointIdList) {
                    //	System.out.println(dataset.length);
                    double[] point = dataset[pointid - 1];
                    boolean locateinsied = true;
                    for (int i = 0; i < dim; i++) {
                        if (point[i] > querymax[i] || point[i] > querymin[i]) {
                            locateinsied = false;
                        }
                    }
                    if (locateinsied)
                        cover++;
                }
            } else {
                for (IndexNode child : getNodelist(nodelist)) {
                    cover += child.coveredPOints(querymax, querymin, dis, dim, dataset, nodelist);
                }
            }
            return cover;
        }
    }

    /*
     * get the number of covered points exactly
     */
    public int coveredPOints(double querymax[], double querymin[], double dis, int dim, double[][] dataset, Map<Integer, IndexNode> nodelist,
                             boolean nonselectedDimension[], boolean dimensionAll, ArrayList<double[]> points) {
        if (nonselectedDimension != null && dimensionAll == false) {
            if (!intersected(querymax, querymin, dim, nonselectedDimension, dimensionAll)) {
                return 0;
            } else {
                if (coverByQuery(querymax, querymin, dim, nonselectedDimension, dimensionAll)) {
                    // add all the points into the list for results
                    if (points != null)
                        for (int pointid : pointIdList) {
                            points.add(dataset[pointid - 1]);
                        }
                    return totalCoveredPoints;
                }
                int cover = 0;
                if (pointIdList.size() > 0) {// leaf node
                    for (int pointid : pointIdList) {
                        double[] point = dataset[pointid - 1];
                        boolean locateinsied = true;
                        for (int i = 0; i < dim; i++) {
                            if (nonselectedDimension[i] == false && point[i] > querymax[i] || point[i] < querymin[i]) {
                                locateinsied = false;
                                break;
                            }
                        }
                        if (locateinsied) {
                            if (points != null)
                                points.add(point);// add the points into the list for results
                            cover++;
                        }
                    }
                } else {
                    for (IndexNode child : getNodelist(nodelist)) {
                        cover += child.coveredPOints(querymax, querymin, dis, dim, dataset, nodelist, nonselectedDimension, dimensionAll, points);
                    }
                }
                return cover;
            }
        } else {
            return coveredPOints(querymax, querymin, dis, dim, dataset, nodelist);
        }
    }

    /*
     * get all the points, not a number, for selected dimensions
     */
    public ArrayList<Integer> coveredPOintsAll(double querymax[], double querymin[], double dis, int dim, double[][] dataset,
                                               Map<Integer, IndexNode> nodelist, boolean nonselectedDimension[], boolean dimensionAll) {
        if (nonselectedDimension != null && dimensionAll == false) {
            ArrayList<Integer> cover = new ArrayList<Integer>();
            if (!intersected(querymax, querymin, dim, nonselectedDimension, dimensionAll)) {
                return null;
            } else {
                if (coverByQuery(querymax, querymin, dim, nonselectedDimension, dimensionAll)) {// locate inside the query rectangle
                    getAllCoveredPoints(cover);
                    return cover;
                }
                if (pointIdList.size() > 0) {// leaf node
                    for (int pointid : pointIdList) {
                        double[] point = dataset[pointid - 1];
                        boolean locateinsied = true;
                        for (int i = 0; i < dim; i++) {
                            if (nonselectedDimension[i] == false && point[i] > querymax[i] || point[i] < querymin[i]) {
                                locateinsied = false;
                            }
                        }
                        if (locateinsied)
                            cover.add(pointid);
                    }
                } else {
                    for (IndexNode child : getNodelist(nodelist)) {
                        cover.addAll(child.coveredPOintsAll(querymax, querymin, dis, dim, dataset, nodelist, nonselectedDimension, dimensionAll));
                    }
                }
                return cover;
            }
        } else {
            return coveredPOintsAll(querymax, querymin, dis, dim, dataset, nodelist);
        }
    }

    /*
     * get all the points, not a number
     */
    public ArrayList<Integer> coveredPOintsAll(double querymax[], double querymin[], double dis, int dim,
                                               double[][] dataset, Map<Integer, IndexNode> nodelist) {
        ArrayList<Integer> cover = new ArrayList<Integer>();
        if (!intersected(querymax, querymin, dim)) {
            return null;
        } else {
            if (coverByQuery(querymax, querymin, dim)) {//locate inside the query rectangle
                getAllCoveredPoints(cover);
                return cover;
            }
            if (pointIdList.size() > 0) {//leaf node
                for (int pointid : pointIdList) {
                    double[] point = dataset[pointid - 1];
                    boolean locateinsied = true;
                    for (int i = 0; i < dim; i++) {
                        if (point[i] > querymax[i] || point[i] < querymin[i]) {
                            locateinsied = false;
                        }
                    }
                    if (locateinsied)
                        cover.add(pointid);
                }
            } else {
                for (IndexNode child : getNodelist(nodelist)) {
                    cover.addAll(child.coveredPOintsAll(querymax, querymin, dis, dim, dataset, nodelist));
                }
            }
            return cover;
        }
    }

    /*
     * get the z-curve overlaps, only for two dimension
     */
    public double GridOverlap(int[] query) {
//        得到结果的范围是：0~1
//        值越小，距离越近，相似度越高
        double intersection = computeIntersection(signautre, query, signautre.length, query.length);
        double res = (query.length - intersection) / query.length;
        return res;
    }

    /*
     * compute the intersection of two sorted lists,
     */
//    居然要求list有序，但它明明不有序，只能在创建时先进行排序了
//    存在的问题：
//    只是有网格重叠就判断相似不合理，更应该进行整体的考察
//    但是论文用的就是这个算法
//    那只能尝试增大网格密度了
    int computeIntersection(int arr1[], int arr2[], int m, int n) {
        int i = 0, j = 0;
        int dist = 0;
        while (i < m && j < n) {
            if (arr1[i] < arr2[j])
                i++;
            else if (arr2[j] < arr1[i])
                j++;
            else {
                dist++;
                i++;
                j++;
            }
        }
        return dist;
    }

    // how to select a right unit based on index
    public double[] setUpperBound(double ub, double unit, double quantilizedUpperBound[]) {
        upperbound = ub;
        if (nodeList != null) {
            for (IndexNode child : nodeList) {
                quantilizedUpperBound = child.setUpperBound(ub, unit, quantilizedUpperBound);
            }
        } else {
            for (int pointid : pointIdList)
                quantilizedUpperBound[pointid - 1] = ub;
        }
        return quantilizedUpperBound;
    }

    public void setUpperBoundPick(double ub) {
        upperbound = ub;
    }

    public double getUpperBoundPick() {
        return upperbound;
    }

    public void setLowerBound(double lb, double unit) {
        lowerbound = lb;
    }

    public double getLowerBound(double unit) {
        return lowerbound;
    }

    public double getUpperBound(double unit) {
        return upperbound;
    }

    public void setNearestCluster(short assignedID) {
        newassign = assignedID;
    }

    public short getAssignedCluster() {
        return assignedClusterID;
    }

    public short getnearestID() {
        return newassign;
    }

    public void setPrunedCounter(short prundNum, short pointcounterPruned[]) {
        counterPruned += prundNum;
        if (nodeList != null) {
            for (IndexNode child : nodeList) {
                child.setPrunedCounter(prundNum, pointcounterPruned);
            }
        } else {
            for (int pointid : pointIdList)
                pointcounterPruned[pointid - 1] += prundNum;
        }
    }

    public void setAssign(short nearest, short newassign[]) {
        this.newassign = nearest;
        if (nodeList != null) {
            for (IndexNode child : nodeList) {
                child.setAssign(nearest, newassign);
            }
        } else {
            for (int pointid : pointIdList)
                newassign[pointid - 1] = nearest;
        }
    }

    public short[] setAssignPICK(short nearest, short newassign[]) {
        this.assignedClusterID = nearest;
        if (nodeList != null) {
            for (IndexNode child : nodeList) {
                child.setAssignPICK(nearest, newassign);
            }
        } else {
            if (newassign != null)
                for (int pointid : pointIdList)
                    newassign[pointid - 1] = nearest;
        }
        return newassign;
    }

    public void unAssignPICK() {
        this.assignedClusterID = 0;
    }

    public void setPrunedCounterSingle(short prundNum) {
        counterPruned = prundNum;
    }

    public void setAllPrune(short prundNum, short pointcounterPruned[]) {
        counterPruned = prundNum;
        if (nodeList != null) {
            for (IndexNode child : nodeList) {
                child.setAllPrune(prundNum, pointcounterPruned);
            }
        } else {
            for (int pointid : pointIdList)
                pointcounterPruned[pointid - 1] = prundNum;
        }
    }

    public short getPrunedCounter() {
        return counterPruned;
    }

    // when to split the centroid index, whether we should split all, update the bound of each node
    void cleanIndex(int k, double group_drift[], double maxdrift[], double unitQuantization) {
        if (counterPruned == k && newassign > 0) {// has been assigned
            //	upperbound = (byte)((double)upperbound*unitQuantization + group_drift[newassign-1]/unitQuantization+1);
        }
        upperbound += group_drift[newassign - 1];
        assignedClusterID = newassign;
        counterPruned = 0;
        if (!nodeList.isEmpty()) {
            for (IndexNode childNode : nodeList) {
                childNode.cleanIndex(k, group_drift, maxdrift, unitQuantization);
            }
        }
    }


    /*
     * set the signature
     */
    void setSignature(ArrayList<Integer> signauture) {
        this.signautre = signauture.stream().mapToInt(i -> i).toArray();
    }

    /*
     * get the signature
     */
    int[] getSignature() {
        return signautre;
    }


    /*
     * build the signature for the whole data lake
     */
    public int[] buildsignarture(HashMap<Integer, ArrayList<Integer>> dataset, Map<Integer, IndexNode> nodelists) {
        ArrayList<Integer> signauture = new ArrayList<Integer>();
        if (isRootLeaf()) {
            int datasetid = rootToDataset;
            ArrayList<Integer> arrayList = dataset.get(datasetid);
            if (arrayList != null) {
                for (int sig : arrayList) {
                    if (!signauture.contains(sig))
                        signauture.add(sig);
                }
            }
        } else {
            for (IndexNode dataseNode : getNodelist(nodelists)) {
                int[] signa = dataseNode.buildsignarture(dataset, nodelists);
                for (int sig : signa) {
                    if (!signauture.contains(sig))
                        signauture.add(sig);
                }
            }
        }
        setSignature(signauture);
        return signautre;
    }

    /*
     * set the maximum covered points
     */
    public void setMaxCovertpoint(Map<Integer, IndexNode> nodelists) {
        if (isRootLeaf()) {
            maxCoverpoints = totalCoveredPoints;
        } else {
            maxCoverpoints = 0;
            for (IndexNode dataseNode : getNodelist(nodelists)) {
                dataseNode.setMaxCovertpoint(nodelists);
                if (dataseNode.getmaxCoverpoints() > maxCoverpoints) {
                    maxCoverpoints = dataseNode.getmaxCoverpoints();
                }
            }
        }
    }

    /*
     * build the bounding box for the whole data lake
     */
    public void buildBoundingBox(Map<Integer, IndexNode> nodelists,
                                 Map<Integer, Map<Integer, IndexNode>> datasetIndex, int dimension) {
        mbrmax = new double[dimension];
        mbrmin = new double[dimension];
        if (isRootLeaf()) {
            int datasetid = rootToDataset;
            IndexNode root = datasetIndex.get(datasetid).get(1);
            mbrmax = root.getMBRmax();
            mbrmin = root.getMBRmin();
        } else {
            for (int i = 0; i < dimension; i++) {
                mbrmin[i] = Double.MAX_VALUE;
            }
            for (IndexNode dataseNode : getNodelist(nodelists)) {
                dataseNode.buildBoundingBox(nodelists, datasetIndex, dimension);
                double nodembrmax[] = dataseNode.getMBRmax();
                double nodembrmin[] = dataseNode.getMBRmin();
                for (int i = 0; i < dimension; i++) {
                    if (mbrmin[i] > nodembrmin[i])
                        mbrmin[i] = nodembrmin[i];
                    if (mbrmax[i] < nodembrmax[i])
                        mbrmax[i] = nodembrmax[i];
                }
            }
        }
    }

    public int getmaxCoverpoints() {
        return maxCoverpoints;
    }


    /*
     * this is for the fair k-means
     */
    public void addSumFair(double[] sum, int userID[], Map<Integer, Integer> userNumber, int traidx) {
        for (int i = 0; i < sum.length; i++)
            this.sum[i] += sum[i] * 1 / userNumber.get(userID[traidx - 1]);
    }

    public void setTotalCoveredPointsFair(double totalCoveredPointsFair) {
        this.totalCoveredPointsFair = totalCoveredPointsFair;
    }
}
