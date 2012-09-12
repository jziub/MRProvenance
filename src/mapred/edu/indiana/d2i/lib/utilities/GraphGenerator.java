package edu.indiana.d2i.lib.utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.indiana.d2i.lib.planner.GreedyBinPackPlanner;
import edu.indiana.d2i.lib.planner.Planner;


public class GraphGenerator extends Configured implements Tool {
	
	private static final Logger LOG = Logger.getLogger(GraphGenerator.class);
	
	private static class MapClass extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private static IntWritable outgoStart = new IntWritable();
		private static IntWritable outgoEnd = new IntWritable();
		
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
			String[] splits = line.toString().split(" ");
			outgoEnd.set(Integer.valueOf(splits[0]));
			for (int i = 1; i < splits.length; i++) {
				outgoStart.set(Integer.valueOf(splits[i]));
				context.write(outgoStart, outgoEnd);
			}
		}
	}
	
	private static class ReduceClass extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
		private static Text output = new Text();
		
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			StringBuilder builder = new StringBuilder();
			Iterator<IntWritable> iterator = values.iterator();
			while (iterator.hasNext()) {
				IntWritable outgoEnd = iterator.next();
				builder.append(outgoEnd.get() + " ");
			}
			output.set(builder.toString());
			context.write(key, output);
		}
	}
	
	class Pair {
	    public int first;
	    public int second;
	    public Pair(int first, int second) {
	    	this.first = first;
	    	this.second = second;
	    }
	}
	
	private int[] zipfOutedge(int[] inedgenums, int partitions) {
		int[] outedgenum = new int[inedgenums.length];
		
		// generate plan
		Map<Integer, Double> weights = new HashMap<Integer, Double>();
		for (int i = 0; i < inedgenums.length; i++) {
			weights.put(i+1, (double)inedgenums[i]);
		}
		Planner planner = new GreedyBinPackPlanner();
		List<IntWritable>[] plan = planner.getPartitionPlan(weights, partitions);
		
		// generate out edge
		int count = 0;
		for (int i = 0; i < plan.length; i++) {
			for(IntWritable elem : plan[i]) {
				outedgenum[elem.get()-1] = inedgenums[count++];
			}
		}
		
		return outedgenum;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		int nodesNum = Integer.valueOf(args[0]);
		int maxEdgeNum = Integer.valueOf(args[1]);
		double skew = Double.valueOf(args[2]);
		String infile = args[3];
		String outfile = args[4];
		int evenDist = Integer.valueOf(args[5]);
		int partitions = Integer.valueOf(args[6]);
		
		Configuration conf = new Configuration();
		Path inpath = new Path(infile, "incoming.txt");
		Path outpath = new Path(outfile);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(inpath)) fs.delete(inpath, true);
		if (fs.exists(outpath)) fs.delete(outpath, true);
		FSDataOutputStream writer = fs.create(inpath);
		
		LOG.info("Generating Zipf distribution...");
		ZipfGenerator zipf = new ZipfGenerator(nodesNum, skew);
		int[] inedgenums = zipf.getRankArray2(maxEdgeNum);		
		int[] outedgenum = new int[inedgenums.length];
		if (evenDist == 1) {
			LOG.info("even distribution of out edge");
//			outedgenum = Arrays.copyOf(inedgenums, inedgenums.length);
//			for (int i = outedgenum.length; i > 1; i--) {
//				int temp = outedgenum[i - 1];
//				int randIx = (int) (Math.random() * i);
//				outedgenum[i - 1] = outedgenum[randIx];
//				outedgenum[randIx] = temp;
//			}
			int avg = (int)Math.ceil((double)(maxEdgeNum / nodesNum)) + 1;
			Arrays.fill(outedgenum, avg);
		} else {
			LOG.info("Zipf distribution of out edge");
			outedgenum = zipfOutedge(inedgenums, partitions);
			
//			for (int i = 0; i < inedgenums.length; i++) {
//				outedgenum[inedgenums.length - 1 - i] = inedgenums[i];
//			}
		}
		
		
//		int[] outedgenum = new int[inedgenums.length];
//		int end = inedgenums.length - 1;
//		int start = 0;
//		for (int i = 0; i < inedgenums.length; i++) {
//			if (i%2 == 1) outedgenum[end--] = inedgenums[i];
//			else outedgenum[start++] = inedgenums[i];
//		}
//		int[] outedgenum = new int[inedgenums.length];
//		int avg = (int)Math.ceil((double)(maxEdgeNum / nodesNum)) + 1;
//		Arrays.fill(outedgenum, avg);		
		
//		int[] outedgenum = new int[inedgenums.length];		
		
		LOG.info("Shuffling the array...");
		// <node, out_edge_num>
		List<Pair> lst = new ArrayList<Pair>();
		for (int i = 1; i <= nodesNum; i++) lst.add(new Pair(i, outedgenum[i-1]));
		Collections.shuffle(lst);
		
		int length = lst.size();
		// generate incoming edges
		String linebreak = "\n";
		int actualEdges = 0;
		LOG.info("Generating incoming edges...");
		for (int i = 1; i <= nodesNum; i++) {
			int edges = inedgenums[i-1];
			edges = (edges > nodesNum) ? nodesNum : edges;
			writer.write(new String(i + " ").getBytes("UTF-8"));
			
			int index = i % length;
			for (int j = 1; j <= edges; j++) {				
				index = (j+i) % length;
				int vertex = lst.get(index).first;
				writer.write(new String(vertex + " ").getBytes("UTF-8"));
				lst.get(index).second--;
				if (lst.get(index).second == 0) {
					lst.set(index, lst.get(length-1));
					length--;
				}
//				LOG.info("node " + i + " finishes " + j + " edge");
			}			
			writer.write(linebreak.getBytes("UTF-8"));
			if (i % (nodesNum/100) == 0)
				LOG.info("progress: " + ((double)i / nodesNum * 100) + "%");
//			LOG.info("node " + i);
			
			actualEdges += edges;
		}
		writer.close();
		LOG.info("Graph has " + nodesNum + " nodes, and " + actualEdges + " edges.");
		
		// revert to outgoing edges
		Job job = new Job(conf, "Graph Generator");
		job.setJarByClass(GraphGenerator.class);
		job.setNumReduceTasks(1);
		job.setMapperClass(MapClass.class);
//		job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(ReduceClass.class);
		
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.getConfiguration().set("mapred.child.java.opts", "-Xmx3072m");
		
		FileInputFormat.setInputPaths(job, inpath);
	    FileOutputFormat.setOutputPath(job, outpath);
	    FileSystem.get(getConf()).delete(outpath, true);
		
		job.waitForCompletion(true);
		
		System.exit(0);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new GraphGenerator(), args);
		System.exit(0);
	}
}
