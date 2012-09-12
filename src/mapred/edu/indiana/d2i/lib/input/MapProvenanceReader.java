package edu.indiana.d2i.lib.input;

import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import edu.indiana.d2i.lib.ProvenanceConstants;

public class MapProvenanceReader extends ProvenanceReader {
	public MapProvenanceReader(Configuration conf, String jobID) throws Exception {
		super(conf, jobID);
	}

	@Override
	protected void initializeFilePatterns() {
		filePatterns.put(READERNAME.KEYDIST, "map_keydist");
		filePatterns.put(READERNAME.STATISTIC, "map_statistic");
	}
	
	public static void main(String[] args) throws Exception {
		String jobID = "job_201201061441_0009";
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:54310");
		conf.set(ProvenanceConstants.PROVENANCE_STORE_DIR, "/provenance/");
		
//		ProvenanceReader reader = new MapProvenanceReader(conf, jobID);
//		Map<Integer, Long> keyDist = reader.readKeyDistribution();
//		
//		FileWriter writer = new FileWriter(new File("mapkeydist.txt"));
//		Iterator<Integer> iterator = keyDist.keySet().iterator();
//		while (iterator.hasNext()) {
//			Integer key = iterator.next();
//			writer.write(key + "\t" + keyDist.get(key) + "\n");
//		}
//		writer.close();
		
		String pathStr = conf.get("fs.default.name") + "/provenance/" + jobID;
		pathStr += "/" + "map_keydist0";
		FileSystem myfs = FileSystem.get(new URI(pathStr), conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(myfs, new Path(pathStr), conf);
		IntWritable key = new IntWritable();
		IntWritable val = new IntWritable();
		while (reader.next(key, val)) {
			System.out.println(key.get() + "\t" + val.get());
		}
		
	}
}
