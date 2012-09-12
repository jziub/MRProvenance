package edu.indiana.d2i.lib.planner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;

import edu.indiana.d2i.lib.ProvenanceConstants;
import edu.indiana.d2i.lib.io.ArrayOfListIntWritable;
import edu.indiana.d2i.lib.io.IntWritablePair;
import edu.indiana.d2i.lib.io.ProvenanceReaderFactory;
import edu.indiana.d2i.lib.io.ProvenanceWriterFactory;

public class PartitionPlanDriver {
	private Configuration conf = null;
	List<IntWritable>[] mappings = null;

	private Planner planner = null;
	private double alpha = 0.5; // 0.8

	private static final Log LOG = LogFactory.getLog(PartitionPlanDriver.class);
	
	// <key, size>
	private Map<Integer, Double> aggregateKeyDistribution(
			List<Reader> inreaderList, List<Reader> outreaderList) throws IOException {
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

		// weight
		Map<Integer, Double> result = new HashMap<Integer, Double>();
		Iterator<Integer> iterator = keyValue.keySet().iterator();
		while (iterator.hasNext()) {
			Integer keycode = iterator.next();
			ValueSizePair valuecode = keyValue.get(keycode);
			result.put(keycode, valuecode.invalueSize*alpha + valuecode.outvalueSize*(1-alpha));
		}

		return result;
	}
	
	public enum PlanType {MAPIN, MAPOUT, MAPINOUT, NOPLAN};
	
	@SuppressWarnings("unchecked")
	public PartitionPlanDriver(Configuration conf, String jobID,
			int numReducers, Planner planner, PlanType type) throws Exception {
		this.conf = conf;
		this.planner = planner;
		this.alpha = conf.getFloat(ProvenanceConstants.ALPHA_MAP_INOUT_TRADEOFF, 0);

		List<Reader> outreaders, inreaders;
		inreaders = outreaders = null;
		switch (type) {
		case MAPOUT:
			outreaders = ProvenanceReaderFactory.createReader(
					this.conf,
					new Path(conf.get(ProvenanceConstants.PROVENANCE_STORE_DIR,
							ProvenanceConstants.PROVENANCE_STORE_DIR_DEFAULTNAME)
							+ jobID), "map_keydist");
			break;
		case MAPIN:
			inreaders = ProvenanceReaderFactory.createReader(
			this.conf,
			new Path(conf.get(ProvenanceConstants.PROVENANCE_STORE_DIR,
					ProvenanceConstants.PROVENANCE_STORE_DIR_DEFAULTNAME)
					+ jobID), "map_inkeydist");
			break;
		case MAPINOUT:
			outreaders = ProvenanceReaderFactory.createReader(
					this.conf,
					new Path(conf.get(ProvenanceConstants.PROVENANCE_STORE_DIR,
							ProvenanceConstants.PROVENANCE_STORE_DIR_DEFAULTNAME)
							+ jobID), "map_keydist");
			inreaders = ProvenanceReaderFactory.createReader(
					this.conf,
					new Path(conf.get(ProvenanceConstants.PROVENANCE_STORE_DIR,
							ProvenanceConstants.PROVENANCE_STORE_DIR_DEFAULTNAME)
							+ jobID), "map_inkeydist");
			break;
		default:
			throw new RuntimeException("Unable to make partition plan!");
		}

		Map<Integer, Double> keyWeights = aggregateKeyDistribution(inreaders, outreaders);
		LOG.info("aggregate key distribution from map output");
		
		mappings = (List<IntWritable>[]) new ArrayList[numReducers];
		mappings = this.planner.getPartitionPlan(keyWeights, numReducers);
		LOG.info("generate partition plan for key distribution");
	}

	/** <key hash code, reducer> */
	public List<IntWritable>[] getPartitionPlan() {
		return mappings;
	}

	public static void main(String[] args) throws Exception {
		String jobID = "job_201201191809_0011";
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:54310/");
		int numReducers = 5;

		// Planner planner = new AvgLoadBasePlanner();
		Planner planner = new GreedyBinPackPlanner();
		PartitionPlanDriver planDriver = new PartitionPlanDriver(conf, jobID,
				numReducers, planner, PartitionPlanDriver.PlanType.MAPOUT);
		List<IntWritable>[] plan = planDriver.getPartitionPlan();
		ArrayOfListIntWritable array = new ArrayOfListIntWritable(plan);
//		List<IntWritable>[] list = array.getArrayOfListInt();
//		for (int i = 0; i < list.length; i++) {
//			for (IntWritable element : list[i]) {
//				System.out.print(element + " ");
//			}
//			System.out.println("");
//		}

		Configuration local = new Configuration();
		String filename = "./partition_plan";
		Writer writer = ProvenanceWriterFactory.createWriter(local, filename,
				IntWritable.class, ArrayOfListIntWritable.class);
		writer.append(new IntWritable(0), array);
		writer.close();
		System.out.println("finish writing");

		Reader reader = ProvenanceReaderFactory.createReader(
				local, new Path(filename));
		IntWritable key = new IntWritable();
		ArrayOfListIntWritable val = new ArrayOfListIntWritable();
		Map<Integer, Integer> myplan = new HashMap<Integer, Integer>(); 
		while (reader.next(key, val)) {
			List<IntWritable>[] keys = val.getArrayOfListInt();
			for (int i = 0; i < keys.length; i++) {
				for (IntWritable keyhashcode : keys[i]) {
					myplan.put(keyhashcode.get(), i);
				}
			}
		}
	}
}
