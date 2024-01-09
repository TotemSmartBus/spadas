/* 
 * Copyright (C) 2015 Saúl Vargas http://saulvargas.es
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package es.saulvargas.balltrees;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import static java.lang.Math.max;
import static java.lang.Math.sqrt;

import java.util.*;


import edu.rmit.trajectory.clustering.kmeans.IndexNode;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Ball tree.
 *
 * @author Saúl Vargas (Saul.Vargas@glasgow.ac.uk)
 */
@Data
public class BallTreeMatrix extends BinaryTree {

    private static final Random random = new Random();
    static double weight[];// indicate the weight on each dimension, for normalization
    
    public BallTreeMatrix(){
        super();
    };
    public BallTreeMatrix(NodeBall root) {
        super(root);
    }
    
    public static void setWeight(int dimension, double []weightinput) {
    	weight = new double[dimension];
		for(int i=0; i< dimension; i++)
			if(weightinput==null)
				weight[i] = 1.0;
			else
				weight[i] = weightinput[i];
    }

    @Override
    public Ball getRoot() {
        return (Ball) super.getRoot();
    }

    public static IndexNode create(double[][] itemMatrix, int leafThreshold, int maxDepth) {
        int[] rows = new int[itemMatrix.length];
        for (int row = 0; row < itemMatrix.length; row++) {
            rows[row] = row;
        }
        Ball root = new Ball(rows, itemMatrix);
        //TODO
        //indexNode rootKmeans = new indexNode(itemMatrix[0].length);
        IndexNode rootKmeans = new IndexNode(2);
        setWeight(itemMatrix[0].length, null);// set all as 1
        int depth = 0;
        if (rows.length > leafThreshold && depth < maxDepth) {
            createChildren(root, leafThreshold, depth + 1, maxDepth);
        }
//        if (rows.length > leafThreshold) {
//            createChildren(root, leafThreshold, depth + 1, maxDepth);
//        }
        root.traverseConvert(rootKmeans, itemMatrix[0].length);
        return rootKmeans;
    }
    
    public static IndexNode create(double[][] itemMatrix, int leafThreshold, int maxDepth, double []weightinput, int dimension) {
        int[] rows = new int[itemMatrix.length];
        for (int row = 0; row < itemMatrix.length; row++) {
            rows[row] = row;
        }
        Ball root = new Ball(rows, itemMatrix);
        IndexNode rootKmeans = new IndexNode(dimension);
        setWeight(dimension, weightinput);
        //TODO set custom wetght for argo
        //weight[2]=0;
        int depth = 0;
        if (rows.length > leafThreshold && depth < maxDepth) {
            createChildren(root, leafThreshold, depth + 1, maxDepth);
        }
//        if (rows.length > leafThreshold) {
//            createChildren(root, leafThreshold, depth + 1, maxDepth);
//        }
//        修正中心和半径，确保父节点包含子节点，但bound可能会不太紧密
        refineNode(root, dimension);
        root.traverseConvert(rootKmeans, dimension);
        return rootKmeans;
    }

    /**
     * 自上而下地递归地更新Ball对象的中心和半径
     * 类似于树的后序遍历
     * 测试效果并不理想
     * @param parent
     */
    private static void refineNode(Ball parent, int dimension) {
        if (parent.isLeaf()) {
            return;
        }
        refineNode(parent.getLeftChild(), dimension);
        refineNode(parent.getRightChild(), dimension);
//        更新父节点的中心
        double[] pivot = new double[dimension];
        double distOfCenter = 0;
        for (int i = 0; i < dimension; i++) {
            pivot[i] = (parent.getLeftChild().getCenter()[i] + parent.getRightChild().getCenter()[i]) / 2;
            distOfCenter += Math.pow((parent.getLeftChild().getCenter()[i] - parent.getRightChild().getCenter()[i]) / 2, 2);
        }
        parent.setCenter(pivot);
//        更新父节点的半径
        double radiLarger = Math.max(parent.getLeftChild().getRadius(), parent.getRightChild().getRadius());
        distOfCenter = Math.sqrt(distOfCenter);
        parent.setRadius(radiLarger + distOfCenter);
    }

    private static void createChildren(Ball parent, int leafThreshold, int depth, int maxDepth) {
        IntArrayList leftRows = new IntArrayList();
        IntArrayList rightRows = new IntArrayList();

        splitItems(parent.getRows(), parent.getItemMatrix(), leftRows, rightRows);
        parent.clearRows();

        Ball leftChild = new Ball(leftRows.toIntArray(), parent.getItemMatrix());
        parent.setLeftChild(leftChild);

//        增加测试，看是否真的存在父节点无法包含子节点的情况
        

        if (leftChild.getRows().length > leafThreshold && depth < maxDepth) {
            createChildren(leftChild, leafThreshold, depth + 1, maxDepth);
        }

        Ball rightChild = new Ball(rightRows.toIntArray(), parent.getItemMatrix());
        parent.setRightChild(rightChild);
        if (rightChild.getRows().length > leafThreshold && depth < maxDepth) {
            createChildren(rightChild, leafThreshold, depth + 1, maxDepth);
        }
    }

    protected static void splitItems(int[] rows, double[][] itemMatrix, IntArrayList leftRows, IntArrayList rightRows) {
        // pick random element
        double[] x = itemMatrix[rows[random.nextInt(rows.length)]];
        // select furthest point A to x
        double[] A = x;
        double dist1 = 0;
        for (int row : rows) {
            double[] y = itemMatrix[row];
            double dist2 = distance2(x, y);
            if (dist2 > dist1) {
                A = y;
                dist1 = dist2;
            }
        }
        // select furthest point B to A
        double[] B = A;
        dist1 = 0;
        for (int row : rows) {
            double[] y = itemMatrix[row];
            double dist2 = distance2(A, y);
            if (dist2 > dist1) {
                B = y;
                dist1 = dist2;
            }
        }

        // split data according to A and B proximity
        for (int row : rows) {
            double[] y = itemMatrix[row];
            double distA = distance2(A, y);
            double distB = distance2(B, y);

            if (distA <= distB) {
                leftRows.add(row);
            } else {
                rightRows.add(row);
            }
        }
    }
    
    //the above function can be optimized to build a balanced kd-tree, by changing the spliting rules to Median of medians or midvalue
    

    @Data
    public static class Ball extends BinaryTree.NodeBall {

        public double[] center;
        public double radius;
        public int[] rows;//it
        public double[][] itemMatrix;
        public HashMap<Integer, double[]> itemMatrixMapping;
        @Getter
        public HashMap<Integer, Double> ubMove;

        public void setCenter(double[] center) {
            this.center = center;
        }

        public void setRadius(double radius) {
            this.radius = radius;
        }

        public int[] rowsID;

        public Ball(int[] rows, double[][] itemMatrix) {
            this.rows = rows;
            this.itemMatrix = itemMatrix;
            calculateCenter();
            calculateRadius();
        }

        public Ball(int[] rows, HashMap<Integer, Double> ubMove, HashMap<Integer, double[]> itemMatrixMapping) {
            this.rows = rows;
            this.itemMatrixMapping = itemMatrixMapping;
            this.ubMove = ubMove;
//            this.rowsID = rows;
            this.calculateCenterEMD();
            this.calculateRadiusEMD();
        }

        @Override
        public Ball getParent() {
            return (Ball) super.getParent();
        }

        @Override
        public Ball getLeftChild() {
            return (Ball) super.getLeftChild();
        }

        @Override
        public Ball getRightChild() {
            return (Ball) super.getRightChild();
        }



        private void calculateCenter() {
            //TODO set it 2, fix it afterwards
            //center = new double[itemMatrix[0].length];
            center = new double[2];

            for (int row : rows) {
                for (int i = 0; i < center.length; i++) {
                    center[i] += itemMatrix[row][i];
                }
            }
            for (int i = 0; i < center.length; i++) {
                center[i] /= rows.length;
            }
        }

        private void calculateCenterEMD() {
            center = new double[2];

            for (int row : rows) {
                for (int i = 0; i < center.length; i++) {
                    center[i] += itemMatrixMapping.get(row)[i];
                }
            }
            for (int i = 0; i < center.length; i++) {
                center[i] /= rows.length;
            }
        }

        private void calculateRadius() {
            radius = 0;

            for (int row : rows) {
                radius = max(radius, distance2(center, itemMatrix[row]));
            }
            radius = sqrt(radius);
            if (radius > 100) {
                System.out.println();
            }
        }

        private void calculateRadiusEMD() {
            radius = 0;

            for (int row : rows) {
                radius = max(radius, distance2(center, itemMatrixMapping.get(row)));
            }
            radius = sqrt(radius);
        }

        public double mip(double[] q) {
            return dotProduct(q, center) + radius * norm(q);
        }

        public double mip(Ball ball) {
            double[] p0 = center;
            double[] q0 = ball.getCenter();
            double rp = radius;
            double rq = ball.getRadius();
            return dotProduct(p0, q0) + rp * rq + rq * norm(p0) + rp * norm(q0);
        }

        public double[] getCenter() {
            return center;
        }

        public double getRadius() {
            return radius;
        }

//        public int[] getRows() {
//            return rows;
//        }

        public void clearRows() {
            rows = null;
        }

//        public double[][] getItemMatrix() {
//            return itemMatrix;
//        }
        
        public int traverseConvert(IndexNode rootKmeans, int dimension) {
    		rootKmeans.setRadius(radius);
            rootKmeans.setPivot(center);//
    		if(rows != null){//for the leaf node
    			Set<Integer> aIntegers = new HashSet<Integer>();
    			double []sumOfPoints = new double[dimension];
    			for(int id : rows) {
//                    他妈的为什么要+1？？？
//                    我偏不要+1！！！
    				aIntegers.add(id);//TODO the pointid 注意此处+1
    				for(int i=0; i<dimension; i++)
    					sumOfPoints[i] += itemMatrix[id][i];
    			}
    			rootKmeans.setSum(sumOfPoints);
    			rootKmeans.addPoint(aIntegers);		
    			rootKmeans.setTotalCoveredPoints(aIntegers.size());
    			return aIntegers.size();
    		}else {   			
    			int count = 0;
				IndexNode childleftnodekmeans = new IndexNode(dimension);
				rootKmeans.addNodes(childleftnodekmeans);
				count += getLeftChild().traverseConvert(childleftnodekmeans, dimension);
				
				IndexNode childrightnodekmeans = new IndexNode(dimension);
				rootKmeans.addNodes(childrightnodekmeans);
				count += getRightChild().traverseConvert(childrightnodekmeans, dimension);
    			rootKmeans.setTotalCoveredPoints(count);
    			return count;
    		}		
    	}

        public int traverseConvert2(IndexNode rootKmeans, int dimension) {
            double[] d = new double[this.rows.length];
            int index = 0;
            int[] var5 = this.rows;
            int var6 = var5.length;

            int count;
            for(count = 0; count < var6; ++count) {
                int row = var5[count];
                d[index] = this.ubMove.get(row);
                ++index;
            }

            double maxUbMove = this.calcateMaxUbMove(d);
            rootKmeans.setEMDRadius(this.radius, maxUbMove);
            rootKmeans.setPivot(this.center);
            if (this.getLeftChild() != null && this.getRightChild() != null) {
                count = 0;
                IndexNode childleftnodekmeans = new IndexNode(dimension);
                count = count + this.getLeftChild().traverseConvert2(childleftnodekmeans, dimension);
                rootKmeans.addNodes(childleftnodekmeans);
                IndexNode childrightnodekmeans = new IndexNode(dimension);
                count += this.getRightChild().traverseConvert2(childrightnodekmeans, dimension);
                rootKmeans.addNodes(childrightnodekmeans);
                rootKmeans.setTotalCoveredPoints(count);
                return count;
            } else {
                Set<Integer> aIntegers = new HashSet();
                double[] sumOfPoints = new double[dimension];
                int[] var9 = this.rows;
                int var10 = var9.length;

                for(int var11 = 0; var11 < var10; ++var11) {
                    int id = var9[var11];
//                    id + 1 -> id
                    aIntegers.add(id);

                    for(int i = 0; i < dimension; ++i) {
                        sumOfPoints[i] += this.itemMatrixMapping.get(id)[i];
                    }
                }

                rootKmeans.setSum(sumOfPoints);
                rootKmeans.addPoint(aIntegers);
                rootKmeans.setTotalCoveredPoints(aIntegers.size());
                return aIntegers.size();
            }
        }

        public double calcateMaxUbMove(double[] ubMove) {
            double u = -1.0E8;

            for(int i = 0; i < ubMove.length; ++i) {
                if (u < ubMove[i]) {
                    u = ubMove[i];
                }
            }

            return u;
        }

        public double calcateMinUbMove(double[] ubMove) {
            double u = 1.0E8;

            for(int i = 0; i < ubMove.length; ++i) {
                if (u > ubMove[i]) {
                    u = ubMove[i];
                }
            }

            return u;
        }
    }

    public static double distance(double[] x, double[] y) {
        return sqrt(distance2(x, y));
    }

    public static double distance2(double[] x, double[] y) {
        double d = 0.0;
        for (int i = 0; i < x.length; i++) {
        	if(weight==null)//normal case
        		d += (x[i] - y[i]) * (x[i] - y[i]);
        	else{
        	    //TODO weight size maybe less than sizeof x
                if(i<weight.length)
                    d += (x[i] - y[i]) * (x[i] - y[i])*weight[i]*weight[i];
            }

        }
        return d;
    }

    public static double norm(double[] x) {
        return sqrt(norm2(x));
    }

    public static double norm2(double[] x) {
        return dotProduct(x, x);
    }

    public static double dotProduct(double[] x, double[] y) {
        double p = 0.0;
        for (int i = 0; i < x.length; i++) {
            p += x[i] * y[i];
        }
        return p;
    }
}
