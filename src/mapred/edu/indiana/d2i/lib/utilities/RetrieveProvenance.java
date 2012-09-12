package edu.indiana.d2i.lib.utilities;

import java.io.File;
import java.io.FileWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.indiana.d2i.lib.ProvenanceConstants;
import edu.indiana.d2i.lib.input.MapProvenanceReader;
import edu.indiana.d2i.lib.input.ProvenanceReader;
import edu.indiana.d2i.lib.input.ReduceProvenanceReader;

public class RetrieveProvenance extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		System.out.println("hello world!!!");
		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", args[0]);
		conf.set(ProvenanceConstants.PROVENANCE_STORE_DIR, args[1]);
		String jobID = args[2];
		
		// get reduce statistic
		ProvenanceReader reduceReader = new ReduceProvenanceReader(conf, jobID);
		List<Map<String, String>> taskStatistic = reduceReader.readTaskStatistic();
		FileWriter writer = new FileWriter(new File("reducestatistic.txt"));
		for (Map<String, String> map : taskStatistic) {
			Iterator<String> iterator = map.keySet().iterator();
			while (iterator.hasNext()) {
				String key = iterator.next();
				writer.write(key + "\t" + map.get(key) + "\n");
			}
		}		
		writer.close();
		
		// get map key dist
		ProvenanceReader mapReader = new MapProvenanceReader(conf, jobID);
		List<Map<String, String>> mapStatistic = mapReader.readTaskStatistic();
		FileWriter mapkeyWriter = new FileWriter(new File("mapstatistic.txt"));
		for (Map<String, String> map : mapStatistic) {
			Iterator<String> iterator = map.keySet().iterator();
			while (iterator.hasNext()) {
				String key = iterator.next();
				mapkeyWriter.write(key + "\t" + map.get(key) + "\n");
			}
		}
		mapkeyWriter.close();
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new RetrieveProvenance(), args);
		System.exit(0);
	}
}
