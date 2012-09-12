package edu.indiana.d2i.lib.utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ProvenanceUtilities {
	private static final class ValueComparator<V extends Comparable<? super V>>
			implements Comparator<Map.Entry<?, V>> {
		public int compare(Map.Entry<?, V> o1, Map.Entry<?, V> o2) {
//			return o1.getValue().compareTo(o2.getValue());
			return o2.getValue().compareTo(o1.getValue()); // descend order
		}
	}

	public static <K, V extends Comparable<? super V>> List<Map.Entry<K, V>> sortMapByValue(
			Map<K, V> map) {
		List<Map.Entry<K, V>> sortedList = new ArrayList<Map.Entry<K, V>>();
		sortedList.addAll(map.entrySet());
		Collections.sort(sortedList, new ValueComparator<V>());
		return sortedList;
	}
	
	public static void main(String[] args) {
		Map<Integer, Long> map = new HashMap<Integer, Long>();
		map.put(Integer.valueOf(1), Long.valueOf((long) 8));
		map.put(Integer.valueOf(2), Long.valueOf((long) 8));
		map.put(Integer.valueOf(3), Long.valueOf((long) 10));
		map.put(Integer.valueOf(4), Long.valueOf((long) 7));
		
		List<Entry<Integer, Long>> sorted = sortMapByValue(map);
		System.out.println(sorted);
	}
}
