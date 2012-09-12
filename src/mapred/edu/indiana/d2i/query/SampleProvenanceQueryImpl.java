package edu.indiana.d2i.query;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.ReflectionUtils;

import edu.indiana.d2i.lib.ProvenanceConstants;

@SuppressWarnings("rawtypes")
public class SampleProvenanceQueryImpl<KEY extends WritableComparable, VALUE extends Writable> {
	private final String HDFS_URI = "hdfs://127.0.0.1:54310";
	private final String PROVENANCE_ROOT = "/provenance/";
	private FileSystem fs = null;
	private Configuration conf = null;
	private Path queryPath = null;

	//
	private String jobIDNum = null;

	//
	private Partitioner<KEY, VALUE> partitioner = null;
	private int numPartitions = 0;

	private String getReduceFile(KEY key, VALUE value, String suffix) {
		// get the reduce provenance file name
		int partition = partitioner.getPartition(key, value, numPartitions);
		StringBuilder reduceFile = new StringBuilder();
		reduceFile.append(queryPath.toUri().getPath() + Path.SEPARATOR);
		reduceFile.append("task_");
		reduceFile.append(jobIDNum);
		reduceFile.append("_r_");
		reduceFile.append(String.format("%06d", partition));
		reduceFile.append(suffix);
		System.out.println("file " + reduceFile);
		return reduceFile.toString();
	}

	@SuppressWarnings("unchecked")
	public SampleProvenanceQueryImpl(Job job, String jobID) {
		try {
			URI uri;
			this.jobIDNum = jobID.substring(4);
			this.conf = job.getConfiguration();
			uri = new URI(HDFS_URI + PROVENANCE_ROOT + jobID);
			this.fs = FileSystem.get(uri, conf);
			this.queryPath = new Path(PROVENANCE_ROOT + jobID);
			partitioner = (Partitioner<KEY, VALUE>) ReflectionUtils
					.newInstance(job.getPartitionerClass(), conf);
			numPartitions = job.getNumReduceTasks();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	/*
	 * key is reduce output key
	 */
	public List<MapProvenance> getMapProvenances(KEY key, VALUE value)
			throws IOException {
		String reduceFile = getReduceFile(key, value,
				ProvenanceConstants.REDUCE_PROVENANCE_KEYS_SUFFIX);

		// get the connection between reduce and map
		MapFile.Reader reader = new MapFile.Reader(fs, reduceFile, conf);
		Text connection = new Text();
		reader.get(key, connection);
		System.out.println("val " + connection);

		// get the map provenance
		List<MapProvenance> mapProvenanceList = new ArrayList<MapProvenance>();
		StringTokenizer itr = new StringTokenizer(connection.toString());
		while (itr.hasMoreTokens()) {
			StringBuilder mapFile = new StringBuilder();
			String token = itr.nextToken();
			mapFile.append(queryPath.toUri().getPath() + Path.SEPARATOR);
			mapFile.append("task_");
			mapFile.append(jobIDNum);
			mapFile.append("_m_");
			mapFile.append(String.format("%06d", Integer.valueOf(token)));

			mapProvenanceList.add(new MapProvenance(new InputStreamReader(fs
					.open(new Path(mapFile.toString()))), mapFile.toString()));
		}
		return mapProvenanceList;
	}

	public ReduceProvenance getReduceProvenance(KEY key, VALUE value)
			throws IOException {
		String reduceFile = getReduceFile(key, value,
				ProvenanceConstants.REDUCE_PROVENANCE_STATUS_SUFFIX);
		return new ReduceProvenance(new InputStreamReader(fs.open(new Path(
				reduceFile))), reduceFile);
	}

	public List<MapProvenance> getAllMapProvenances() throws IOException {
		PathFilter filter = new PathFilter() {
			@Override
			public boolean accept(Path path) {
				if (path.getName().contains("_m_"))
					return true;
				else
					return false;
			}
		};
		FileStatus[] fileStatus = fs.listStatus(queryPath, filter);

		List<MapProvenance> mapProvenances = new ArrayList<MapProvenance>();
		for (FileStatus status : fileStatus) {
			mapProvenances.add(new MapProvenance(new InputStreamReader(fs
					.open(new Path(status.getPath().toUri().getPath()))), status.getPath().getName()));
		}

		return mapProvenances;
	}

	public List<ReduceProvenance> getAllReduceProvenances() throws IOException {
		PathFilter filter = new PathFilter() {
			@Override
			public boolean accept(Path path) {
				if (path.getName().contains("_r_")
					&& path.getName().contains(ProvenanceConstants.REDUCE_PROVENANCE_STATUS_SUFFIX))
					return true;
				else
					return false;
			}
		};
		FileStatus[] fileStatus = fs.listStatus(queryPath, filter);

		List<ReduceProvenance> reduceProvenances = new ArrayList<ReduceProvenance>();
		for (FileStatus status : fileStatus) {
			reduceProvenances.add(new ReduceProvenance(new InputStreamReader(fs
					.open(new Path(status.getPath().toUri().getPath()))), status.getPath().getName()));
		}

		return reduceProvenances;
	}

	// test drive
	public static void main(String[] args) throws IOException {
		String jobID = "job_201112072020_0002";
		Job job = new Job(new Configuration(), "wordcount");
		job.setNumReduceTasks(3);
		SampleProvenanceQueryImpl<Text, Text> query = new SampleProvenanceQueryImpl<Text, Text>(
				job, jobID);

		// Text key = new Text("\"(Lo)cra\"");
		Text key = new Text("\"Absoluti");
		Text value = new Text("\"1490");
		List<MapProvenance> mapProvenances = query
				.getMapProvenances(key, value);
		for (MapProvenance mapProvenance : mapProvenances) {
			// get attributes
			Map<String, String> attrs = mapProvenance.getAttrs();
			Iterator<String> iterator = attrs.keySet().iterator();
			while (iterator.hasNext()) {
				String next = iterator.next();
				System.out.println(next + ", " + attrs.get(next));
			}

			// get data distribution
			TreeMap<Integer, Integer> dataDistribution = mapProvenance
					.getDataDistribution();
			Iterator<Integer> iter = dataDistribution.keySet().iterator();
			while (iter.hasNext()) {
				Integer id = iter.next();
				System.out.println(id + " " + dataDistribution.get(id));
			}
		}

		ReduceProvenance reduceProvenance = query.getReduceProvenance(key,
				value);
		Map<String, String> attrs = reduceProvenance.getAttrs();
		Iterator<String> iterator = attrs.keySet().iterator();
		while (iterator.hasNext()) {
			String next = iterator.next();
			System.out.println(next + ", " + attrs.get(next));
		}

		List<MapProvenance> allMapProvenances = query.getAllMapProvenances();
	}
}
