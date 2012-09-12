package edu.indiana.d2i.query;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

public abstract class AbstractProvenance {
	// <id, size_input_data>
	protected TreeMap<Integer, Integer> dataDistribution = null;
	protected Map<String, String> attributes = null;
	protected String identifier = null;

	public AbstractProvenance(InputStreamReader in, String identifier)
			throws IOException {
		attributes = new HashMap<String, String>();
		dataDistribution = new TreeMap<Integer, Integer>();
		this.identifier = identifier;

		BufferedReader reader = new BufferedReader(in);
		String line = null;
		while ((line = reader.readLine()) != null) {
			StringTokenizer pair = new StringTokenizer(line);
			String nextToken = pair.nextToken();
			if (!nextToken.contains("DataDistribution")) {
				attributes.put(nextToken, pair.nextToken());
			} else {
				while (pair.hasMoreTokens()) {
					dataDistribution.put(Integer.valueOf(pair.nextToken()),
							Integer.valueOf(pair.nextToken()));
				}
			}
		}
	}

	public Map<String, String> getAttrs() {
		return attributes;
	}

	public String getAttrValue(String attrName) {
		return attributes.get(attrName);
	}

	public String getIdentifier() {
		return identifier;
	}
}
