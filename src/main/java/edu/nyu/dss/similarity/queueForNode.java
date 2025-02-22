package edu.nyu.dss.similarity;


import edu.rmit.trajectory.clustering.kmeans.IndexNode;
import edu.rmit.trajectory.clustering.kpaths.Util;

/*
 * this class is for the heap method
 */
public class queueForNode implements Comparable<queueForNode>{
	double bound;
	IndexNode anode;
	IndexNode bnode;

	
	public double getbound() {
		return bound;
	}
	
	public IndexNode getNode() {
		return bnode;
	}

	
	public queueForNode(IndexNode a, IndexNode b) {
		this.anode = a;
		this.bnode = b;
		double[] pivota = anode.getPivot();
		double[] pivotb = bnode.getPivot();
		bound = Util.EuclideanDis(pivota, pivotb, pivota.length);
		bound = Math.max(bound - bnode.getRadius(), 0);
	}
	
	@Override
    public int compareTo(queueForNode other) {
		double gap = this.getbound() - other.getbound();
		
		if(gap>0)
			return 1;
		else if(gap==0)
			return 0;
		else {
			return -1;
		}
    }
	
}
