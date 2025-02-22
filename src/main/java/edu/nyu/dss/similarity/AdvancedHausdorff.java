package edu.nyu.dss.similarity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.rmit.trajectory.clustering.kmeans.IndexNode;
import org.paukov.combinatorics3.Generator;

public class AdvancedHausdorff extends Hausdorff{
	
	static List<List<Integer>> permutations;
	
	static ArrayList<Integer> outlierID;// storing the outlier point id
	static ArrayList<Double> outlierDis;// storing the outlier point id
	static boolean selfOutlier = false; // only set as true when
	static int outlierThreshold = 0; // set as 0 for normal Hausdorff
	
	
	/* set the approximate version and outlier*/
	static void setParameter(boolean selfOutliers, boolean apporixmates) {
		selfOutlier = selfOutliers;
	}
	
	/*
     * based on radius, leaf (capacity), or depth, or covered points
     */
    static boolean stopSplitCondition(IndexNode a, int option, int dimension, boolean nonselectedDimension[], boolean dimensionAll) {
    	if(a==null)
    		return true;
    	
    	if(option==1) {
    		if(a.getRadius(nonselectedDimension, dimension, dimensionAll, weight)>radiusThreshold)
    			return false;
    		else {
				return true;
			}
    	}else if(option==2) {
    		if(a.getTotalCoveredPoints()>coveredPointsThreshold)
    			return false;
    		else {
				return true;
			}
    	}else if(option==3) {//this is for node b
    		if(!a.isLeaf())
    			return false;
    		else {
				return true;
			}
    	}
    	return false;
    }
    
    /*
     * split the first node, or the second node based on 
     */
    static boolean splitPriority(IndexNode a, IndexNode b, int option, boolean asplit, boolean bsplit, int dimension, boolean nonselectedDimension[], boolean dimensionAll) {
    	if(asplit == true)
    		return false;
    	else if(bsplit == true)
    		return true;
    	
    	if(option==1) {//compute the similarity 
    		if(a.getRadius(nonselectedDimension, dimension, dimensionAll, weight)>b.getRadius(nonselectedDimension, dimension, dimensionAll, weight))
    			return true;
    		else {
				return false;
			}
    	}else if(option==2) {
    		if(a.getTotalCoveredPoints()>b.getTotalCoveredPoints())
    			return true;
    		else {
				return false;
			}
    	}
    	return false;
    }
    
    /*
     * pami15, early breaking
     */
    public static double earlyBreaking(double [][]point1xys, double [][]point2xys, int dimension, boolean directed, 
    		ArrayList<Integer> aCoveredPoints, ArrayList<Integer> bCoveredPoints) {
    	int x = aCoveredPoints.size();
    	int y = bCoveredPoints.size();
    	double[][] dist_matrix;
        dist_matrix = new double[x][y];
		//TODO modified to minus num
		//double cmax = 0;
		double cmax = -1000;
    	for(int i=0; i<x; i++) {
    		double cmin= Double.MAX_VALUE;
    		for(int j=0; j<y; j++) {
    			/*
    			 * we add an early breaking on Euclidean distance, good for high-dimension dataset
    			 */
    			dist_matrix[i][j] = EuclideanDis(point1xys[aCoveredPoints.get(i)-1], point2xys[bCoveredPoints.get(j)-1], dimension);
    			if(dist_matrix[i][j]<cmin)
    				cmin = dist_matrix[i][j];
    			if(cmin<cmax)
    				break;
    		}
    		if(cmin>cmax && Double.MAX_VALUE>cmin)
    			cmax = cmin;
    	}
    	
    	if(!directed)
	    	for(int j=0; j<y; j++) {
	    		double cmin= Double.MAX_VALUE;
	    		for(int i=0; i<x; i++) {
	    			if(dist_matrix[i][j] == 0)
	    				dist_matrix[i][j] = EuclideanDis(point1xys[aCoveredPoints.get(i)-1], point2xys[bCoveredPoints.get(j)-1], dimension);
	    			if(dist_matrix[i][j]<cmin)
	    				cmin = dist_matrix[i][j];
	    			if(cmin<cmax)
	    				break;
	    		}
	    		if(cmin>cmax && Double.MAX_VALUE>cmin)
	    			cmax = cmin;
	    	}
    	return cmax;
    }
    
    /*
     * using a brute force to compute the distance
     * 
     * this is not efficient version, we need to have fast version without constructing the matrix
     */
    static double computeNodeDistance(IndexNode a, IndexNode b, int dimension, double[][] dataMatrixa, double[][] dataMatrixb,
                                      boolean direction, int apointid, int bpointid) {
    	// get all the covered points of two nodes
    	ArrayList<Integer> aCoveredPoints = new ArrayList<Integer>();
    	if(apointid>0)
    		aCoveredPoints.add(apointid);
    	else
    		a.getAllCoveredPoints(aCoveredPoints);
    	ArrayList<Integer> bCoveredPoints = new ArrayList<Integer>();
    	if(bpointid>0)
    		bCoveredPoints.add(bpointid);
    	else
    		b.getAllCoveredPoints(bCoveredPoints);
    	return earlyBreaking(dataMatrixa, dataMatrixb, dimension, direction, aCoveredPoints, bCoveredPoints);// take the early breaking idea
    }
	
	 /*
     * our own method, with a general index, ball-tree, can work for any types of index tree.
     * An incremental Hausdorff distance calculation algorithm
     * 1) it can be used to detect own outliers, following the definition sigmod'00 for self outlier detection, we remove the self-pair
     * 2) support elbow method to return robust Hausdorff measure, leave it for hausdorff distance evaluation
     * 
     * we need to input the first pair back to the queue
     */
    public static Pair<Double, PriorityQueue<queueMain>> IncrementalDistance(double [][]point1xys, double [][]point2xys,
                                                                             int dimension, IndexNode X, IndexNode Y, int splitOption, int fastMode,
                                                                             double error, boolean reverse, double directDis, boolean topkEarlyBreaking,
                                                                             Map<Integer, IndexNode> nodelist, Map<Integer, IndexNode> nodelist1,
                                                                             boolean nonselectedDimension[], boolean dimensionAll) {
    	radiusThreshold = error;// the error for approximate search
    	if(!reverse) {
    		disCompTime = 0;
    		EuclideanCounter = 0;
    	}
    	outlierID = new ArrayList<Integer>();
    	outlierDis = new ArrayList<Double>();
    	int counter = 0;
    	if(splitOption==0)
    		fastMode = 0;//exact search, explore every point
    	if(tightBound==2)// MBR search
    		permutations = Generator.permutation(0, 1).withRepetitions(dimension-1).stream().collect(Collectors.<List<Integer>>toList());
    	PriorityQueue<queueMain> aHeaps = new PriorityQueue<queueMain>();
    	PriorityQueue<queueSecond> secondHeaps = new PriorityQueue<queueSecond>();
    	queueSecond secondq = new queueSecond(Y, 0, 0);
    	secondHeaps.add(secondq);
//    	distance is never used, so i commented it and changed it to ub.
    	double distance = 0;
    	queueMain mainq = new queueMain(X, secondHeaps, Double.MAX_VALUE, 0);
    	aHeaps.add(mainq);
    	double ub = 0;
    	//the outlier can be put alone, here
    	while(!aHeaps.isEmpty()) {
//    		mainq = aHeaps.poll();
			mainq = aHeaps.peek();
    		secondHeaps = mainq.getQueue();
    		secondq = secondHeaps.peek();// we need to think about here peek k continuous points, or we just set k=1
    		IndexNode anode = mainq.getIndexNode();
    		ub = mainq.getbound();
//			<=改成了<, 不然直接终止了
//			注释掉了这个方法出口，不知道他有什么意义
//    		if(reverse && ub<directDis)// for the directed early breaking
//    			return new MutablePair<>(directDis, aHeaps);
    		IndexNode bnode = secondq.getNode();
    	//	if(topkEarlyBreaking && anode!=null && bnode!=null && directDis<secondq.getbound())//for top-k search and pruning, estimate the bound
    	//		continue;
    		boolean asplit = stopSplitCondition(anode, splitOption);
    		boolean bsplit = stopSplitCondition(bnode, splitOption);
    		if( asplit == true &&  bsplit == true) {//if two nodes are null, i.e., they are points // we can change it to radius threshold
    			if(topkEarlyBreaking){// using radius to estimate the lower bound, if the lower bound still big, prune it, 
    				double temp = ub - radiusThreshold;
//    				if(anode==null && bnode==null)
//    					return new MutablePair<>(ub, aHeaps);
//					push the last mainq back to aHeaps
					if (anode == null && bnode == null) {
//						aHeaps.add(mainq);
						return new MutablePair<>(ub, aHeaps);
					}
    				if(bnode==null)
    					temp = ub;
    				if(directDis < temp) {// the estimated lower bound
    					return new MutablePair<>(ub, aHeaps);
    				}else{// otherwise, continue the exact search, change the split option to 0
    					splitOption = 0;
    					fastMode = 0;
    					if(anode != null)
    						asplit = false;
    					if(bnode != null)
    						bsplit = false;
    					topkEarlyBreaking = false;
    					if(asplit == true && bsplit == true) {
    						return new MutablePair<>(ub, aHeaps);
    					}
    				}
    			}else {
    				// judge whether meet the stopping for outlier, and keep monitoring and infer the outlier
    				outlierID.add(mainq.getpointID());
    				outlierDis.add(ub);
    				// using the elbow method here to determine the precise value
    				if(++counter > outlierThreshold)
    					return new MutablePair<>(ub, aHeaps);
    			}
    		}
    		if(asplit == false || bsplit == false){
    			boolean splitPr = splitPriority(anode, bnode, splitCompare, asplit, bsplit, dimension, nonselectedDimension, dimensionAll);//
    			aHeaps.poll();
				if(splitPr) {
//					poll the largest element from queueMain
//					aHeaps.poll();
    				traverseX(anode, point1xys, point2xys, secondHeaps, dimension, aHeaps, fastMode, topkEarlyBreaking, directDis,
    						false, 0, nodelist, nonselectedDimension, dimensionAll);
    			}else {
    				secondHeaps.poll();
    				ub = traverseY(bnode, point1xys, point2xys, secondHeaps, dimension, aHeaps, ub, anode, mainq.getpointID(),
    						fastMode, topkEarlyBreaking, directDis, false, 0, nodelist1, nonselectedDimension, dimensionAll);
    			}
    		}
    	}
    	Pair<Double, PriorityQueue<queueMain>> resultPair = new MutablePair<>(distance, aHeaps);
    	return resultPair;
    }
    
    /*
     * this is the directed version
     */
    static double IncrementalDistanceDirected(double [][]point1xys, double [][]point2xys, int dimension, IndexNode X, IndexNode Y,
                                              int splitOption, int fastMode, double error, Map<Integer, IndexNode> nodelist, Map<Integer, IndexNode> nodelist1, boolean nonselectedDimension[], boolean dimensionAll) {
    	if(X.getRadius(nonselectedDimension, dimension, dimensionAll,weight)>Y.getRadius(nonselectedDimension, dimension, dimensionAll, weight)) {
    		Pair<Double, PriorityQueue<queueMain>> resultPair = IncrementalDistance(point1xys, point2xys, dimension, X, Y, 
    				splitOption, fastMode, error, false, 0, false, nodelist, nodelist1, nonselectedDimension, dimensionAll);
    		Pair<Double, PriorityQueue<queueMain>> resultPairReverse = IncrementalDistance(point2xys, point1xys, dimension, 
    				Y, X, splitOption, fastMode, error, true, resultPair.getLeft(), false, nodelist1, nodelist, nonselectedDimension, dimensionAll);
    		return Math.max(resultPair.getLeft(), resultPairReverse.getLeft());
    	}else {
    		Pair<Double, PriorityQueue<queueMain>> resultPair = IncrementalDistance(point2xys, point1xys, dimension, Y, X, 
    				splitOption, fastMode, error, false, 0, false,nodelist1, nodelist, nonselectedDimension, dimensionAll);
    		Pair<Double, PriorityQueue<queueMain>> resultPairReverse = IncrementalDistance(point1xys, point2xys, dimension, 
    				X, Y, splitOption, fastMode, error, true, resultPair.getLeft(), false,nodelist, nodelist1, nonselectedDimension, dimensionAll);
    		return Math.max(resultPair.getLeft(), resultPairReverse.getLeft());
    	}
    }
    
    // if it is outlier version, we exclude the self-pair
//	before this function didn't have a return value, so i add one.
    static void traverseX(IndexNode anode, double[][] point1xys, double[][] point2xys, PriorityQueue<queueSecond> secondHeaps,
                          int dimension, PriorityQueue<queueMain> aHeaps, int fastMode, boolean topkEarlyBreaking,
                          double directDis, boolean join, double joinThreshold, Map<Integer, IndexNode> nodelist, boolean nonselectedDimension[], boolean dimensionAll) {
    	ArrayList<double[]> pivtoList = new ArrayList<double[]>();
    	ArrayList<Pair<double[], double[]>> mbrList = new ArrayList<Pair<double[], double[]>>();
		ArrayList<Double> radiusArrayList = new ArrayList<Double>();
		ArrayList<Integer> arrayList = new ArrayList<Integer>(anode.getpointIdList());
		ArrayList<IndexNode> arrayListnode = new ArrayList<IndexNode>(anode.getNodelist(nodelist));
		double newlb, newub;
		if(anode.isLeaf()) {
			for (int a : anode.getpointIdList()) {
				pivtoList.add(point1xys[a]);
				radiusArrayList.add(0.0);
				if(tightBound==2)
					mbrList.add(new MutablePair<double[], double[]>(point1xys[a], point1xys[a]));
			}
		}else {
			for (IndexNode a : anode.getNodelist(nodelist)) {
				pivtoList.add(a.getPivot());
				radiusArrayList.add(a.getRadius(nonselectedDimension, dimension, dimensionAll, weight));
				if(tightBound==2)
					mbrList.add(new MutablePair<double[], double[]>(a.getMBRmin(), a.getMBRmax()));
			}
		}
		int length = pivtoList.size();
		double minub = Double.MAX_VALUE;
		for (int i=0; i<length; i++) {
			PriorityQueue<queueSecond> newsecondHeaps = new PriorityQueue<queueSecond>();

			for (queueSecond aQueueSecond : secondHeaps) {
				IndexNode newnodeb = aQueueSecond.getNode();
				Map<Integer, Pair<double[], double[]>> segmentB=null;
				if(tightBound==2) {//using the MBR to prune
					if(newnodeb != null)
						segmentB = generateSegment(newnodeb.getMBRmin(), newnodeb.getMBRmax(), dimension);
					else
						segmentB = generateSegment(point2xys[aQueueSecond.getPointId()], point2xys[aQueueSecond.getPointId()], dimension);
				}
				//if it has been computed and in the second round, we can retrieve the results and avoid the calculation
				double pivot_distance, bradius = 0;
				EuclideanCounter++;				
				if (newnodeb != null) {
					pivot_distance = newnodeb.getPivotdistance(pivtoList.get(i));
					bradius = newnodeb.getRadius(nonselectedDimension, dimension, dimensionAll, weight);// can be changed to get radius
				} else {
					pivot_distance = Hausdorff.EuclideanDis(point2xys[aQueueSecond.getPointId()], pivtoList.get(i), dimension);
				}
				if(tightBound == 0) {
					newlb = computeNewLowerBound(pivot_distance, radiusArrayList.get(i), bradius);
					newub = computeNewUpperBound(pivot_distance, radiusArrayList.get(i), bradius);
				}else if(tightBound == 1) {
					newlb = pivot_distance - bradius - radiusArrayList.get(i);
					newub = pivot_distance + bradius + radiusArrayList.get(i);
				}else {
					Pair<double[], double[]> mbrA = mbrList.get(i);//get the mbr box
			    	Map<Integer, Pair<double[], double[]>> segmentA = generateSegment(mbrA.getLeft(), mbrA.getRight(), dimension);
			    	newlb = computeMBRLowerBound(segmentA,segmentB,dimension);
					newub = computeMBRUpperBound(segmentA,segmentB,dimension);
				}
				if(join && newlb>joinThreshold) {//if new lb is bigger than a given threshold, filter
					continue;
				}
				
				if(fastMode == 1) {
					if(newnodeb != null || !anode.isLeaf()) {
						if(anode.isLeaf())
							computeNodeDistance(null, newnodeb, dimension, point1xys, point2xys, true, arrayList.get(i), 0);
						else
							computeNodeDistance(arrayListnode.get(i), newnodeb, dimension, point1xys, point2xys, true, 0, aQueueSecond.getPointId());
					}
				}else if(fastMode == 2) {
					if(bradius<radiusThreshold && radiusArrayList.get(i)<radiusThreshold)
						newub = pivot_distance; // approximate solution
				}else if(fastMode == 3) {
					// one point and one leaf case, maybe we can compute in advance, as it is linear to compute rather than quadratic
				}else if(fastMode == 4) {
					// learned distance based on embedding, very similar to RMI of the learned index, hybrid mode
				}
				
				if(!selfOutlier || arrayList.isEmpty() || arrayList.get(i) != aQueueSecond.getPointId()) {// self outlier detection, avoid self-pair, here
					queueSecond secondqb = new queueSecond(newnodeb, newlb, aQueueSecond.getPointId());
					newsecondHeaps.add(secondqb);
					if (newub < minub)
						minub = newub;
				}
			}
			if(anode.isLeaf()) {
				queueMain mainqa = new queueMain(null, newsecondHeaps, minub, arrayList.get(i));
				aHeaps.add(mainqa);
			}else {				
			//	if(topkEarlyBreaking && newsecondHeaps.peek().getNode()!=null && directDis<newsecondHeaps.peek().getbound())//for top-k search and pruning, estimate the bound
	    	//		continue;
				queueMain mainqa = new queueMain(arrayListnode.get(i), newsecondHeaps, minub, 0);
				aHeaps.add(mainqa);
			}
		}
//		return minub;
    }
    
    static double traverseY(IndexNode bnode, double[][] point1xys, double[][] point2xys, PriorityQueue<queueSecond> secondHeaps,
                            int dimension, PriorityQueue<queueMain> aHeaps, double ub, IndexNode anode, int apoint, int fastMode,
                            boolean topkEarlyBreaking, double directDis, boolean join, double joinThreshold, Map<Integer, IndexNode> nodelist, boolean nonselectedDimension[], boolean dimensionAll) {
    	ArrayList<double[]> pivtoList = new ArrayList<double[]>();
    	ArrayList<Pair<double[], double[]>> mbrList = new ArrayList<Pair<double[], double[]>>();
    	ArrayList<Integer> arrayList = new ArrayList<Integer>(bnode.getpointIdList());// this may cost much time
    	ArrayList<IndexNode> arrayListnode = new ArrayList<IndexNode>(bnode.getNodelist(nodelist));
		ArrayList<Double> radiusArrayList = new ArrayList<Double>();
		double lb, newub;
		if(bnode.isLeaf()) {
			for (int a : bnode.getpointIdList()) {
				pivtoList.add(point2xys[a]);
				radiusArrayList.add(0.0);
				if(tightBound==2)//mbr bound, complex
					mbrList.add(new MutablePair<double[], double[]>(point2xys[a-1], point2xys[a-1]));
			}
		}else {
			for (IndexNode a : bnode.getNodelist(nodelist)) {
				pivtoList.add(a.getPivot());
				radiusArrayList.add(a.getRadius(nonselectedDimension, dimension, dimensionAll, weight));
				if(tightBound==2)
					mbrList.add(new MutablePair<double[], double[]>(a.getMBRmin(), a.getMBRmax()));
			}
		}
		int length = pivtoList.size();
		Map<Integer, Pair<double[], double[]>> segmentA=null;
		if(tightBound==2) {
			if(anode != null)
				segmentA = generateSegment(anode.getMBRmin(), anode.getMBRmax(), dimension);
			else
				segmentA = generateSegment(point1xys[apoint], point1xys[apoint], dimension);
		}
		for (int i=0; i<length; i++) {
			double pivot_distance, bradius = 0;
			EuclideanCounter++;
			long startTime = System.nanoTime();
			if (anode != null) {
				pivot_distance = anode.getPivotdistance(pivtoList.get(i));
				bradius = anode.getRadius(nonselectedDimension, dimension, dimensionAll, weight);
			} else {
				pivot_distance = Hausdorff.EuclideanDis(point1xys[apoint], pivtoList.get(i), dimension);
			}
			long endtime = System.nanoTime();
			disCompTime += (endtime-startTime)/1000000000.0;
			if(tightBound==0) {
				lb = computeNewLowerBound(pivot_distance, bradius, radiusArrayList.get(i));
				newub = computeNewUpperBound(pivot_distance, bradius, radiusArrayList.get(i));
			}else if(tightBound==1) {
				lb = pivot_distance-bradius-radiusArrayList.get(i);
				newub = pivot_distance + bradius + radiusArrayList.get(i);
			}else {
				Pair<double[], double[]> mbrB = mbrList.get(i);//get the mbr box
		    	Map<Integer, Pair<double[], double[]>> segmentB = generateSegment(mbrB.getLeft(), mbrB.getRight(), dimension);
				lb = computeMBRLowerBound(segmentA,segmentB,dimension);
				newub = computeMBRUpperBound(segmentA,segmentB,dimension);
			}
			if(join && lb>joinThreshold)// used for join only
				continue;
			boolean self = false;
			if(bnode.isLeaf()) {
//				if bnode == null, bnode is a point
				queueSecond mainqa = new queueSecond(null, lb, arrayList.get(i));
				if(!selfOutlier || arrayList.get(i) != apoint)// self outlier detection, avoid self-pair points
					secondHeaps.add(mainqa);
				else {
					self = true;
				}
			}else {
				queueSecond mainqa = new queueSecond(arrayListnode.get(i), lb, 0);
				secondHeaps.add(mainqa);
			}
			if (fastMode == 1) {//only when early access is used
				if(anode != null || !bnode.isLeaf()) {//no need to recompute if two are points
					if(bnode.isLeaf())
//						no return? then what for?
//						need to delete
						computeNodeDistance(anode, null, dimension, point1xys, point2xys, true, 0, arrayList.get(i));
					else
						computeNodeDistance(anode, arrayListnode.get(i), dimension, point1xys, point2xys, true, apoint, 0);
				}
			} else if (fastMode == 2) {//the approximate version
				if(bradius<radiusThreshold && radiusArrayList.get(i)<radiusThreshold)
					newub = pivot_distance; // for generating ground truth in a fast way, and estimating the lower bound based on the predicted distance
			} else if(fastMode == 3) {
				// one point and one leaf case, maybe we can compute in advance, as it is linear to compute rather than quadratic
			} else if(fastMode == 4) {
				// learned distance based on embedding, very similar to RMI (learned index) of the learned index, hybrid mode
			}
			if (!self && newub < ub)
				ub = newub;
		}
	//	if(topkEarlyBreaking && anode!= null && secondHeaps.peek().getNode()!=null && directDis<secondHeaps.peek().getbound())//for top-k search and pruning, estimate the bound
	//		return ub;
//		增加对secondHeaps是否为空的判断，若为空，说明都被过滤了，则不添加到队列中
		if (secondHeaps.isEmpty()) {
			return ub;
		}
		queueMain mainqa = new queueMain(anode, secondHeaps, ub, apoint);
		aHeaps.add(mainqa);
		return ub;
    }

    /*
     * we propose tight upper bounds based half-ball theory, which is tighter than using whole-ball
     */
    static double computeNewUpperBound(double pivotDis, double radiusA, double radiusB) {
    	return Math.sqrt((pivotDis)*(pivotDis) + radiusB*radiusB)+radiusA;
    }
    
    /*
     * we use tighter lower bounds based on balls
     */
//	need to modify, compare with 0
//	lower bound should not be less than 0
    static double computeNewLowerBound(double pivotDis, double radiusA, double radiusB) {
//    	return pivotDis-radiusB;
		return Math.max(pivotDis - radiusB, 0);
    }

    
    /*
     * we compute the MBR upper bound, for general cases, i.e., any dimensional data
     */
    static double computeMBRUpperBound(Map<Integer, Pair<double[], double[]>> segmentA, Map<Integer, Pair<double[], double[]>> segmentB, int dimension) {
    	//TODO modified to minus num
    	//double max = 0;
		double max = -1000;
    	for(int segid:segmentA.keySet()) {
    		Pair<double[], double[]> aaPair = segmentA.get(segid);
    		double min = Double.MAX_VALUE;
    		for(int segidB:segmentB.keySet()) {
    			Pair<double[], double[]> bbPair = segmentB.get(segidB);
    			double mindist = segmentMaxDis(aaPair, bbPair, dimension);
    			if(mindist<min)
    				min = mindist;
    		}
    		if(min>max)
    			max = min;
    	}
    	return max;
    }
    
    /*
     * we compute the MBR lower bound, for general cases, i.e., any dimensional data
     */
    static double computeMBRLowerBound(Map<Integer, Pair<double[], double[]>> segmentA, Map<Integer, Pair<double[], double[]>> segmentB, int dimension) {
		//TODO modified to minus num
		//double max = 0;
		double max = -1000;
    	for(int segid:segmentA.keySet()) {
    		Pair<double[], double[]> aaPair = segmentA.get(segid);
    		double min = Double.MAX_VALUE;
    		for(int segidB:segmentB.keySet()) {
    			Pair<double[], double[]> bbPair = segmentB.get(segidB);
    			double mindist = segmentMinDis(aaPair, bbPair, dimension);
    			if(mindist<min)
    				min = mindist;
    		}
    		if(min>max)
    			max = min;
    	}
    	return max;
    }
    
    /*
     * generate all the edges of an MBR in d-dimensions
     */
    static Map<Integer, Pair<double[], double[]>> generateSegment(double []mbrMinA, double []mbrMaxA, int dimension) {
    	Map<Integer, Pair<double[], double[]>> segmentMapA = new HashMap<Integer, Pair<double[],double[]>>();
    	int segcount = 0;
    	for(int i=0; i<dimension; i++) {
    		double min = mbrMinA[i];
    		double max = mbrMaxA[i];
    		for(List<Integer> point: permutations) {
    			double []pointA = new double[dimension];
    			double []pointB = new double[dimension];
    			for(int j=0; j<dimension; j++) {
    				if(j>i) {
    					if(point.get(j-1)==0) {
    						pointA[j] = mbrMaxA[j];
    						pointB[j] = mbrMaxA[j];
    					}else {
    						pointA[j] = mbrMinA[j];
    						pointB[j] = mbrMinA[j];
    					}
    				}else if(j==i){
    					pointA[j] = min;
    					pointB[j] = max;
    				}else {
    					if(point.get(j)==0) {
    						pointA[j] = mbrMaxA[j];
    						pointB[j] = mbrMaxA[j];
    					}else {
    						pointA[j] = mbrMinA[j];
    						pointB[j] = mbrMinA[j];
    					}
    				}
    			}
    			segmentMapA.put(segcount++, new MutablePair<>(pointA, pointB));
    		}
    	}
    	return segmentMapA;
    }
    
    /*
     * compute the minimum distance between two segments by finding nearest of two points, from each other
     */
    static double segmentMinDis(Pair<double[], double[]> aaPair, Pair<double[], double[]> bbPair, int dimension) {
    	double a = pointToSegmentDis(aaPair.getLeft(), bbPair, dimension);
    	double b = pointToSegmentDis(aaPair.getRight(), bbPair, dimension);
    	double c = pointToSegmentDis(bbPair.getLeft(), aaPair, dimension);
    	double d = pointToSegmentDis(bbPair.getRight(), aaPair, dimension);
    	return Math.min(a, Math.min(b, Math.min(c, d)));
    }
    
    /*
     * compute the minimum distance between a point and a segment
     */
    static double pointToSegmentDis(double []start, Pair<double[], double[]> bbPair, int dimension) {
    	double []point1 = bbPair.getLeft();
    	double []point2 = bbPair.getRight();
    	int di = 0;
    	for(int d=0; d<dimension; d++) {
    		if(point1[d] != point2[d]) {
    			di = d;
    			break;
    		}
    	}
    	if(start[di]>=Math.min(point1[di], point2[di]) && start[di]<=Math.max(point1[di], point2[di])) {
    		point1[di] = start[di];
    		return Hausdorff.EuclideanDis(start, point1, dimension);
    	}else {
    		return Math.min(Hausdorff.EuclideanDis(start, point1, dimension), Hausdorff.EuclideanDis(start, point2, dimension));
    	}
    }
    
    /*
     * compute the maximum distance between two segments, can be faster
     */
    static double segmentMaxDis(Pair<double[], double[]> aaPair, Pair<double[], double[]> bbPair, int dimension) {
    	double a = Hausdorff.EuclideanDis(aaPair.getLeft(), bbPair.getLeft(), dimension);
    	double b = Hausdorff.EuclideanDis(aaPair.getRight(), bbPair.getRight(), dimension);
    	double c = Hausdorff.EuclideanDis(aaPair.getLeft(), bbPair.getRight(), dimension);
    	double d = Hausdorff.EuclideanDis(aaPair.getRight(), bbPair.getLeft(), dimension);
    	return Math.max(a, Math.max(b, Math.max(c, d)));
    }
    
}
