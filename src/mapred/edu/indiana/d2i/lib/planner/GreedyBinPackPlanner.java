package edu.indiana.d2i.lib.planner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;

public class GreedyBinPackPlanner implements Planner {

	private int findMinimum(double[] bins) {
		double min = Double.MAX_VALUE;
		int index = 0;
		for (int i = 0; i < bins.length; i++) {
			if (bins[i] < min) {
				index = i;
				min = bins[i];
			}
		}
		return index;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<IntWritable>[] getPartitionPlan(
			Map<Integer, Double> keyWeights, int numPartitions) {
		List<IntWritable>[] mappings = (List<IntWritable>[]) new ArrayList[numPartitions];
		
		Comparator<Double> cmp = new Comparator<Double>() {
			 public int compare(Double s1, Double s2) {
				 if (s1.doubleValue() < s2.doubleValue()) return 1;
				 else if (s1.doubleValue() > s2.doubleValue()) return -1;
				 else return 0;
		     }
		};
		Map<Double, List<Integer>> dataSizes = 
			new TreeMap<Double, List<Integer>>(cmp);
		Iterator<Integer> keyValueSizeIter = keyWeights.keySet().iterator();
		while (keyValueSizeIter.hasNext()) {
			Integer keyHashcode = keyValueSizeIter.next();
			Double valueSize = keyWeights.get(keyHashcode);
			if (dataSizes.containsKey(valueSize)) {
				dataSizes.get(valueSize).add(keyHashcode);
			} else {
				List<Integer> keylist = new ArrayList<Integer>();
				keylist.add(keyHashcode);
				dataSizes.put(valueSize, keylist);
			}
		}

		double[] bins = new double[numPartitions];
		Arrays.fill(bins, 0);
		Iterator<Double> iterator = dataSizes.keySet().iterator();
		while (iterator.hasNext()) {
			Double valueSize = iterator.next();
			for (Integer keyhashcode : dataSizes.get(valueSize)) {
				int target = findMinimum(bins);
				bins[target] += valueSize;
				if (mappings[target] == null) {
					mappings[target] = new ArrayList<IntWritable>();					
				} 
				mappings[target].add(new IntWritable(keyhashcode));
			}
		}
		
		return mappings;
	}

}
