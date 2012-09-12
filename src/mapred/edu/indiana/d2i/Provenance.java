package edu.indiana.d2i;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Provenance {
	private TYPE type;
	// map provenance
	private String reduceINKey;
	private Set<String> inputURIs = new HashSet<String>();
	// reduce provenance
	private String reduceOUTKey;
	private String reduceOUTValue;
//	private String reduceOUTFileName; // needs to be added
	
	public enum TYPE {MAP_PROVENANCE, REDUCE_PROVENANCE};
	
	public Provenance(TYPE type, String key, Set<String> values) {
		this.type = type;
		reduceINKey = key;
		inputURIs = values;
	}
	
	public Provenance(TYPE type, String key, String value) {
		this.type = type;
		reduceOUTKey = key;
		reduceOUTValue = value;
	}
	
	/* a list of key-value pairs */
	public List<HashMap<String, String>> getProvenances() {
		List<HashMap<String, String>> provenances = 
			new ArrayList<HashMap<String, String>>();
		
		if (type == TYPE.MAP_PROVENANCE) {
			// <INKEY, inputURI-1, ..., inputURI-k>
//			provenances.add(reduceINKey);
			for (String inputURI : inputURIs) {
				HashMap<String, String> keyvalue = new HashMap<String, String>();
				keyvalue.put(reduceINKey, inputURI);
				provenances.add(keyvalue);
			}
		}
		else {
			// <OUTKEY, OUTVALUE>
			HashMap<String, String> keyvalue = new HashMap<String, String>();
			keyvalue.put(reduceOUTKey, reduceOUTValue);
			provenances.add(keyvalue);
		}
		
		return provenances;
	}
	
	public Provenance.TYPE getType() {
		return this.type;
	}
}
