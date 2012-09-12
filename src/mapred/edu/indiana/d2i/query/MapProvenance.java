package edu.indiana.d2i.query;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.TreeMap;

public class MapProvenance extends AbstractProvenance {
	public MapProvenance(InputStreamReader in, String identifier) throws IOException {
		super(in, identifier);
	}
	
	public TreeMap<Integer, Integer> getDataDistribution() {
		return dataDistribution;
	}
}
