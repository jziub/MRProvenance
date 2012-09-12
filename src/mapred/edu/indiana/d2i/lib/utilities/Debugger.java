package edu.indiana.d2i.lib.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
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

import edu.indiana.d2i.lib.io.ProvenanceReaderFactory;
import edu.indiana.d2i.lib.planner.GreedyBinPackPlanner;
import edu.indiana.d2i.lib.planner.Planner;
import edu.indiana.d2i.lib.planner.ValueSizePair;

public class Debugger extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String ingraph = args[0];
		String outgraph = args[1];
//		String outgraph = args[0];
//		String ingraph = args[1];
		int partitions = Integer.valueOf(args[2]);
		double alpha = Double.valueOf(args[3]);
		String resultFile = args[4];
		int    useRange = Integer.valueOf(args[5]);
		
		// in, out
		Map<Integer, ValueSizePair> keyValue = new HashMap<Integer, ValueSizePair>();
		BufferedReader mapinreader = new BufferedReader(new FileReader(outgraph));
		String line = null;
		while ((line = mapinreader.readLine()) != null) {
			String[] splits = line.split("\\s+");
			int keycode = Integer.valueOf(splits[0]);
			keyValue.put(keycode, new ValueSizePair(1+4+4+(splits.length-1)*4, 0));
		}
		
//		FileWriter debug = new FileWriter("./weights.txt");
		BufferedReader mapoutreader = new BufferedReader(new FileReader(ingraph));
		while ((line = mapoutreader.readLine()) != null) {
			String[] splits = line.split("\\s+");
			int keycode = Integer.valueOf(splits[0]);
			keyValue.get(keycode).outvalueSize = 
				(1+4+4)*(splits.length-1) + keyValue.get(keycode).invalueSize;
//			debug.write((1+4+4)*(splits.length-1) + "\t" + keyValue.get(keycode).invalueSize + "\n");
		}
//		debug.close();
		
		// weight
		FileWriter debugWeights = new FileWriter("./weights.txt");
		Map<Integer, Double> weights = new HashMap<Integer, Double>();
		Iterator<Integer> iterator = keyValue.keySet().iterator();
		while (iterator.hasNext()) {
			Integer keycode = iterator.next();
			ValueSizePair valuecode = keyValue.get(keycode);
			weights.put(keycode, 
					valuecode.invalueSize*alpha + valuecode.outvalueSize*(1-alpha));
			
			double weight = valuecode.invalueSize*alpha + valuecode.outvalueSize*(1-alpha);
			debugWeights.write(keycode + " " + valuecode.invalueSize + " " + valuecode.outvalueSize + " " + weight + "\n");
		}
		debugWeights.close();
		
		// make plan
		List<IntWritable>[] plan = (List<IntWritable>[]) new ArrayList[partitions];
		if (useRange != 1) {
			Planner planner = new GreedyBinPackPlanner();
			plan = planner.getPartitionPlan(weights, partitions);
		} else {
			int size = keyValue.size();
			for (int i = 1; i <= size; i++) {
				int index = (int) (((float) i / (float) size) * partitions) % partitions;
				if (plan[index] == null) {
					plan[index] = new ArrayList<IntWritable>();					
				} 
				plan[index].add(new IntWritable(i));
			}
		}
		
		// apply the plan
		int[] reduceOutput = new int[partitions];
		Arrays.fill(reduceOutput, 0);
		for (int i = 0; i < plan.length; i++) {
			for(IntWritable elem : plan[i]) {
				reduceOutput[i] += keyValue.get(elem.get()).invalueSize;
			}
		}
		
		// write reduceOutput to file
		FileWriter writer = new FileWriter(resultFile);
		for (int i = 0; i < reduceOutput.length; i++) {
			writer.write(i + "\t" + reduceOutput[i] + "\n");
		}
		writer.close();		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new Debugger(), args);
		System.exit(0);
	}
}
