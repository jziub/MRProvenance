package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.indiana.d2i.lib.ProvenanceJobHelper;

public class WordCount extends Configured implements Tool {

	// public static class TokenizerMapper extends
	// Mapper<Object, Text, Text, IntWritable> {
	//
	// private final static IntWritable one = new IntWritable(1);
	// private Text word = new Text();
	//
	// public void map(Object key, Text value, Context context)
	// throws IOException, InterruptedException {
	// StringTokenizer itr = new StringTokenizer(value.toString());
	// while (itr.hasMoreTokens()) {
	// word.set(itr.nextToken());
	// context.write(word, one);
	// }
	// }
	// }
	//
	// public static class IntSumReducer extends
	// Reducer<Text, IntWritable, Text, IntWritable> {
	// private IntWritable result = new IntWritable();
	//
	// public void reduce(Text key, Iterable<IntWritable> values,
	// Context context) throws IOException, InterruptedException {
	// int sum = 0;
	// for (IntWritable val : values) {
	// sum += val.get();
	// }
	// result.set(sum);
	// context.write(key, result);
	// }
	// }
	//
	// public static void main(String[] args) throws Exception {
	// Configuration conf = new Configuration();
	// String[] otherArgs = new GenericOptionsParser(conf, args)
	// .getRemainingArgs();
	// if (otherArgs.length != 2) {
	// System.err.println("Usage: wordcount <in> <out>");
	// System.exit(2);
	// }
	// Job job = new Job(conf, "word count");
	// job.setJarByClass(WordCount.class);
	// job.setMapperClass(TokenizerMapper.class);
	// job.setCombinerClass(IntSumReducer.class);
	// job.setReducerClass(IntSumReducer.class);
	// job.setOutputKeyClass(Text.class);
	// job.setOutputValueClass(IntWritable.class);
	// FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	// FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	// System.exit(job.waitForCompletion(true) ? 0 : 1);
	// }

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(3);
//		ProvenanceJobHelper.setProvenanceWrappers(job);
		ProvenanceJobHelper provenanceJob = new ProvenanceJobHelper(job);
//		provenanceJob.captureReducerStatistic(true).captureMapperOutputKey(true);
//		provenanceJob.captureReducerStatistic(true);
		provenanceJob.captureReducerStatistic(true).captureMapperOutputKey(true).planPartition();
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.exit((provenanceJob.submitJobAndReturnJobID(true) != null) ? 0 : 1);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new WordCount(), args);
		System.exit(0);
	}
}
