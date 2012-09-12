package edu.indiana.d2i.lib.utilities;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.indiana.d2i.lib.ProvenanceConstants;
import edu.indiana.d2i.lib.io.ProvenanceReaderFactory;

public class MapInOutDebugger extends Configured implements Tool {

	private static class ValueSizePair {
		public int invalueSize = 0;
		public int outvalueSize = 0;
		
		public ValueSizePair(int invalueSize, int outvalueSize) {
			this.invalueSize = invalueSize;
			this.outvalueSize = outvalueSize;
		}
	}

	
	private Map<Integer, Double> aggregateKeyDistribution(
			List<Reader> inreaderList, List<Reader> outreaderList, double alpha) throws IOException {
		Map<Integer, ValueSizePair> keyValue = new HashMap<Integer, ValueSizePair>();
		IntWritable key = new IntWritable();
		IntWritable value = new IntWritable();
		if (inreaderList != null) {
			for (Reader reader : inreaderList) {
				while (reader.next(key, value)) {			
					if (keyValue.containsKey(key.get())) {
						keyValue.get(key.get()).invalueSize += value.get();
					} else {
						ValueSizePair triple = new ValueSizePair(value.get(), 0);	
						keyValue.put(key.get(), triple);
					}
				}
			}
		}
		
		if (outreaderList != null) {
			for (Reader reader : outreaderList) {
				while (reader.next(key, value)) {			
					if (keyValue.containsKey(key.get())) {
						keyValue.get(key.get()).outvalueSize += value.get();
					} else {
						ValueSizePair triple = new ValueSizePair(0, value.get());	
						keyValue.put(key.get(), triple);
					}
				}
			}
		}

		// debug
		FileWriter inoutDebug = new FileWriter("inoutdebug.txt");
		
		// weight
		Map<Integer, Double> result = new HashMap<Integer, Double>();
		Iterator<Integer> iterator = keyValue.keySet().iterator();
		while (iterator.hasNext()) {
			Integer keycode = iterator.next();
			ValueSizePair valuecode = keyValue.get(keycode);
			result.put(keycode, valuecode.invalueSize*alpha + valuecode.outvalueSize*(1-alpha));
			
			inoutDebug.write(keycode + " " + valuecode.invalueSize + 
					" " + valuecode.outvalueSize + "\n");
		}
		inoutDebug.close();

		return result;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		String jobID = args[0];
		double alpha = Double.valueOf(args[1]);
		
		Configuration conf = new Configuration();
		List<Reader> outreaders, inreaders;
		outreaders = ProvenanceReaderFactory.createReader(
				conf,
				new Path(conf.get(ProvenanceConstants.PROVENANCE_STORE_DIR,
						ProvenanceConstants.PROVENANCE_STORE_DIR_DEFAULTNAME)
						+ jobID), "map_keydist");
		inreaders = ProvenanceReaderFactory.createReader(
				conf,
				new Path(conf.get(ProvenanceConstants.PROVENANCE_STORE_DIR,
						ProvenanceConstants.PROVENANCE_STORE_DIR_DEFAULTNAME)
						+ jobID), "map_inkeydist");
		
		aggregateKeyDistribution(inreaders, outreaders, alpha);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new MapInOutDebugger(), args);
		System.exit(0);
	}
}
