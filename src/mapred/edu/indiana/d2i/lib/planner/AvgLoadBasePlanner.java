package edu.indiana.d2i.lib.planner;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;

public class AvgLoadBasePlanner implements Planner {

	@Override
	public List<IntWritable>[] getPartitionPlan(Map<Integer, Double> outkeyValueSize, int partitions) {
		Map<Integer, Integer> mappings = new HashMap<Integer, Integer>();
		
		// read <value size, <key1, key2, ...>>
		long total = 0;
		long avgLoad = 0;
		
		Comparator<Integer> cmp = new Comparator<Integer>() {
			 public int compare(Integer s1, Integer s2) {
				 if (s1.intValue() < s2.intValue()) return 1;
				 else if (s1.intValue() > s2.intValue()) return -1;
				 else return 0;
		     }
		};
		Map<Integer, List<Integer>> dataSizes = 
			new TreeMap<Integer, List<Integer>>(cmp);
		
		Iterator<Integer> keyValueSizeIter = outkeyValueSize.keySet().iterator();
		while (keyValueSizeIter.hasNext()) {
			Integer keyHashcode = keyValueSizeIter.next();
//			Integer valueSize = outkeyValueSize.get(keyHashcode);
//			if (dataSizes.containsKey(valueSize)) {
//				dataSizes.get(valueSize).add(keyHashcode);
//			} else {
//				List<Integer> keylist = new ArrayList<Integer>();
//				keylist.add(keyHashcode);
//				dataSizes.put(valueSize, keylist);
//			}
//			total += valueSize;
		}
		
		System.out.println("total load " + total);
		System.out.println("dataSizes.size() " + dataSizes.size());
		System.out.println("avgLoad " + (total / partitions));
		
		// rewrite this by Strategy Pattern!!!
		
		// debug
//		Path path = new Path("/tmp");
//		FileSystem fs = FileSystem.get(conf);
//		BufferedWriter br = 
//			new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
//		int count = 0;
		
		avgLoad = total / partitions;
		int curReducer = 0;
		long curWorkload = 0;
		Iterator<Integer> iterator = dataSizes.keySet().iterator();
		while (iterator.hasNext()) {
			Integer size = iterator.next();
			List<Integer> keys = dataSizes.get(size);
			
			
//			br.write(size + "\t" + keys.size() + "\t" + keys + "\n");
			
			
			for (int i = 0; i < keys.size(); i++) {
				if (curWorkload > avgLoad) {				
//					br.write("!!! count " + (count+1) + "\t" + curWorkload + "\n");
					
					curReducer++;
					if (curReducer >= partitions) curReducer = partitions-1;
					curWorkload = 0;
					
					System.out.println("shift reducer to " + curReducer);
				}
				curWorkload += size;
				mappings.put(keys.get(i), curReducer);
				
//				count++;
			}
		}
		
//		br.close();
		
		
		// write mapping
//		Path mappingPath = new Path("/mapping");
//		FileSystem mappingFS = FileSystem.get(conf);
//		BufferedWriter mappingBR = 
//			new BufferedWriter(new OutputStreamWriter(mappingFS.create(mappingPath,true)));
//		Iterator<Integer> mappingIter = mappings.keySet().iterator();
//		while (mappingIter.hasNext()) {
//			Integer keycode = mappingIter.next();
//			Integer reduceno = mappings.get(keycode);
//			mappingBR.write(keycode + "\t" + reduceno + "\n");
//		}
//		mappingBR.close();		
		
//		return mappings;
		return null;
	}

}
