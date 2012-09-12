package edu.indiana.d2i.lib.utilities;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GraphReverse extends Configured implements Tool {
	
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
	
	@Override
	public int run(String[] args) throws Exception {
		String input = args[0];
		String output = args[1];
		
		// revert edges
		Job job = new Job(getConf(), "Graph Generator");
		job.setJarByClass(GraphGenerator.class);
		job.setNumReduceTasks(1);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.getConfiguration().set("mapred.child.java.opts", "-Xmx3072m");
		
		FileInputFormat.setInputPaths(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
		
		System.exit(0);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new GraphReverse(), args);
		System.exit(0);
	}
}
