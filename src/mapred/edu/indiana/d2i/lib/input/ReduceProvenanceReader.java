package edu.indiana.d2i.lib.input;

import java.io.File;
import java.io.FileWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import edu.indiana.d2i.lib.ProvenanceConstants;

public class ReduceProvenanceReader extends ProvenanceReader {
	public ReduceProvenanceReader(Configuration conf, String jobID) throws Exception {
		super(conf, jobID);
	}

	@Override
	protected void initializeFilePatterns() {
		filePatterns.put(READERNAME.KEYDIST, "reduce_keydist");
		filePatterns.put(READERNAME.STATISTIC, "reduce_statistic");
	}
	
	public static void main(String[] args) throws Exception {
		String jobID = "job_201201021130_0001";
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://127.0.0.1:54310");
		conf.set(ProvenanceConstants.PROVENANCE_STORE_DIR, "/provenance/");
		
//		String jobID = args[0];
//		Configuration conf = new Configuration();
//		conf.set("fs.default.name", args[1]);
//		conf.set(ProvenanceConstants.PROVENANCE_STORE_DIR, args[2]);
		
		ProvenanceReader reader = new ReduceProvenanceReader(conf, jobID);
		List<Map<String, String>> taskStatistic = reader.readTaskStatistic();
		
		FileWriter writer = new FileWriter(new File("reducestatistic.txt"));
		for (Map<String, String> map : taskStatistic) {
			Iterator<String> iterator = map.keySet().iterator();
			while (iterator.hasNext()) {
				String key = iterator.next();
				writer.write(key + "\t" + map.get(key) + "\n");
			}
		}
		writer.close();
	}
}
