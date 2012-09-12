package edu.indiana.d2i.test;

import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

//import org.apache.commons.math.distribution.ZipfDistribution;
//import org.apache.commons.math.distribution.ZipfDistributionImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import edu.indiana.d2i.lib.ProvenanceConstants;
import edu.indiana.d2i.lib.io.ArrayOfListIntWritable;
import edu.indiana.d2i.lib.io.ProvenanceReaderFactory;

public class TestProvenanceReader {
	private String HDFS_URI = "hdfs://127.0.0.1:54310";
	private String PROVENANCE_ROOT = "/provenance/";
	private FileSystem fs = null;
	private Path queryPath = null;
	private Configuration conf = null;
	
	public TestProvenanceReader(String jobID) throws Exception {
		conf = new Configuration();
		URI uri = new URI(HDFS_URI + PROVENANCE_ROOT + jobID);
		fs = FileSystem.get(uri, conf);
		queryPath = new Path(PROVENANCE_ROOT + jobID);
	}
	
	public void read(String filename, final String pattern) throws Exception {
		Set<String> set = new HashSet<String>();
		
		FileWriter writer = new FileWriter(new File(filename));
		PathFilter mapkeyfilter = new PathFilter() {
			@Override
			public boolean accept(Path path) {
				if (path.getName().contains(pattern))
					return true;
				else
					return false;
			}
		};
		FileStatus[] mapkeyStatus = fs.listStatus(queryPath, mapkeyfilter);
		for (FileStatus fileStatus : mapkeyStatus) {
			String name = fileStatus.getPath().toUri().getPath();
			FileSystem mapkeyfs = FileSystem.get(URI.create(HDFS_URI + name), conf);
			SequenceFile.Reader mapkeyReader = new SequenceFile.Reader(
					mapkeyfs, new Path(name), conf);

			Writable key = (Writable) ReflectionUtils.newInstance(
					mapkeyReader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(
					mapkeyReader.getValueClass(), conf);
			while (mapkeyReader.next(key, value)) {
//				if (set.contains(key.toString())) {
//					System.out.println("duplicate: " + key.toString());
//				} else {
//					set.add(key.toString());
//				}
				writer.write(key.toString() + "\t" + value.toString() + "\n");
			}
		}
		
		writer.close();
	}

	public static int powerLaw(int start, int end, double skew) {
		// x = [(x1^(n+1) - x0^(n+1))*y + x0^(n+1)]^(1/(n+1))
		double y = Math.random();
		double x = Math.pow((Math.pow(end, skew+1) - Math.pow(start, skew+1))*y + Math.pow(start, skew+1), 1/(skew+1));
		return (int)x;
	}
	
	public static void main(String[] args) throws Exception {
		String jobID = "job_201201161013_0003";
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:54310");
		
		String input = "/provenance/job_201201261543_0001/partition_plan";
		String output = "./plan";
		Reader reader = ProvenanceReaderFactory.createReader(conf, new Path(input));
		FileWriter writer = new FileWriter(new File(output));
		
		IntWritable key = new IntWritable();
		ArrayOfListIntWritable value = new ArrayOfListIntWritable();
		while (reader.next(key, value)) {
			List<IntWritable>[] keys = value.getArrayOfListInt();
			for (int i = 0; i < keys.length; i++) {
				for (IntWritable keyhashcode : keys[i]) {
					writer.write(keyhashcode + "\t" + i + "\n");
				}
			}
		}
		reader.close();
		writer.close();
		
		FileWriter writer2 = new FileWriter(new File("mapkeydist.txt"));
		List<Reader> readers = ProvenanceReaderFactory.createReader(
				conf, new Path("/provenance/job_201201261543_0001/"), "map_keydist");
		IntWritable key2 = new IntWritable();
		IntWritable val2 = new IntWritable();
		for (Reader reader2 : readers) {
			while (reader2.next(key2, val2)) {
				writer2.write(key2.get() + "\t" + val2.get() + "\n");
			}
		}
		writer2.close();
		
		/* read */
//		jobID = "job_201201111031_0009";
//		FileWriter plan = new FileWriter(new File("planner.txt"));
//		conf.set(ProvenanceConstants.PARTITION_PLAN_LOCATION, "/provenance/" + jobID + "/partition_plan");
//		Reader reader = ProvenanceReaderFactory.createReader(
//				conf, new Path(conf.get(ProvenanceConstants.PARTITION_PLAN_LOCATION)));
//		IntWritable mykey = new IntWritable();
//		IntWritable myval = new IntWritable();
//		while (reader.next(mykey, myval)) {
//			plan.write(mykey.get() + "\t" + myval.get() + "\n");
//		}
		
//		jobID = "job_201201201410_0005";
//		FileWriter writer = new FileWriter(new File("reducestatistic.txt"));
//		List<Reader> readers = ProvenanceReaderFactory.createReader(
//				conf, new Path("/provenance/"+jobID), "reduce_statistic");
//		Text key = new Text();
//		Text val = new Text();
//		for (Reader reader : readers) {
//			while (reader.next(key, val)) {
//				writer.write(key + "\t" + val + "\n");
//			}
//		}
//		writer.close();
		
//		jobID = "job_201201191809_0011";
//		conf.set(ProvenanceConstants.PARTITION_PLAN_LOCATION, "/provenance/"+jobID+"/partition_plan");
//		Reader reader = ProvenanceReaderFactory.createReader(
//				conf, new Path(conf.get(ProvenanceConstants.PARTITION_PLAN_LOCATION)));
//		IntWritable key = new IntWritable();
//		ArrayOfListIntWritable val = new ArrayOfListIntWritable();
//		Map<Integer, Integer> plan = new HashMap<Integer, Integer>(); 
//		while (reader.next(key, val)) {
//			List<IntWritable>[] keys = val.getArrayOfListInt();
//			for (int i = 0; i < keys.length; i++) {
//				for (IntWritable keyhashcode : keys[i]) {
//					plan.put(keyhashcode.get(), i);
//				}
//			}
//		}
		
//		int numberOfElements = 10;
//		double exponent = 0.1;
//		ZipfDistributionImpl zipF = new ZipfDistributionImpl(numberOfElements, exponent);
//		for (int i = 0; i < numberOfElements; i++) {
////			System.out.println(zipF.probability(i));
////			System.out.println(zipF.cumulativeProbability(i));
////			System.out.println("###");
//			System.out.println(powerLaw(1, numberOfElements, exponent));
//		}
//		
		
//		FileWriter writer2 = new FileWriter(new File("reducekeydist0.txt"));
//		List<Reader> readers2 = ProvenanceReaderFactory.createReader(
//				conf, new Path("/provenance/"+jobID), "reduce_keydist0");
//		IntWritable key2 = new IntWritable();
//		Text val2 = new Text();
//		for (Reader reader : readers2) {
//			while (reader.next(key2, val2)) {
//				writer2.write(key2 + "\t" + val2 + "\n");
//			}
//		}
//		writer2.close();
		
		
	}
}
