package edu.indiana.d2i.lib.planner;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;

public interface Planner {
	/** <hash code for key, reducer> */
	public List<IntWritable>[] getPartitionPlan(Map<Integer, Double> keyValueSize, int partitions);
}
