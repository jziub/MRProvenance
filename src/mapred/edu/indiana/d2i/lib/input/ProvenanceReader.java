package edu.indiana.d2i.lib.input;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import edu.indiana.d2i.lib.ProvenanceConstants;

public abstract class ProvenanceReader {
	protected abstract void initializeFilePatterns();
	
	protected String HDFS_URI = "hdfs://127.0.0.1:54310";
	protected String PROVENANCE_ROOT = "/provenance/";
	protected FileSystem fs = null;
	protected Path queryPath = null;
	protected Configuration conf = null;
	
	protected enum READERNAME {
		KEYDIST, STATISTIC, PARTITION_PLAN
	};
	
	protected Map<READERNAME, List<SequenceFile.Reader>> readers = null;
	protected Map<READERNAME, String> filePatterns = null; 
	
	protected List<SequenceFile.Reader> getReaders(final READERNAME readerName) throws IOException {
		if (!readers.containsKey(readerName)) {
			List<SequenceFile.Reader> readerList = new ArrayList<SequenceFile.Reader>();
			
			PathFilter filter = new PathFilter() {
				@Override
				public boolean accept(Path path) {
					if (path.getName().contains(filePatterns.get(readerName)))
						return true;
					else
						return false;
				}
			};
			FileStatus[] status = fs.listStatus(queryPath, filter);
			for (FileStatus fileStatus : status) {
				String name = fileStatus.getPath().toUri().getPath();
				FileSystem mapkeyfs = FileSystem.get(URI.create(HDFS_URI + name), conf);
				SequenceFile.Reader reader = new SequenceFile.Reader(
						mapkeyfs, new Path(name), conf);
				readerList.add(reader);
			}
			
			readers.put(readerName, readerList);
		}
		
		return readers.get(readerName);
	}
	
	public ProvenanceReader(Configuration conf, String jobID) throws Exception {
		this.conf = conf;
		this.HDFS_URI = conf.get("fs.default.name");
		this.PROVENANCE_ROOT = conf.get(ProvenanceConstants.PROVENANCE_STORE_DIR);
		this.fs = FileSystem.get(new URI(HDFS_URI + PROVENANCE_ROOT + jobID), conf);
		this.queryPath = new Path(PROVENANCE_ROOT + jobID);
		this.filePatterns = new HashMap<READERNAME, String>();
		this.readers = new HashMap<ProvenanceReader.READERNAME, List<Reader>>();
		
		initializeFilePatterns(); // let sub class handle
	}
	
	/** <hash code for key, size of value> */
	public Map<Integer, Integer> readKeyDistribution() throws IOException {			
		Map<Integer, Integer> keyValue = new HashMap<Integer, Integer>();
		
		List<Reader> readerList = getReaders(READERNAME.KEYDIST);
		for (Reader reader : readerList) {
			IntWritable key = (IntWritable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			IntWritable value = (IntWritable) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);
			while (reader.next(key, value)) {
				if (keyValue.containsKey(key.get())) {
					keyValue.put(key.get(), keyValue.get(key.get()) + value.get());
				} else {
					keyValue.put(key.get(), value.get());
				}
			}
		}
		
		return keyValue;
	}
	
	public List<Map<String, String>> readTaskStatistic() throws IOException {
		List<Map<String, String>> taskStatistic = new ArrayList<Map<String,String>>();
		
		Text key = new Text();
		Text value = new Text();
		List<Reader> readerList = getReaders(READERNAME.STATISTIC);
		for (Reader reader : readerList) {
			Map<String, String> keyValue = new HashMap<String, String>();
			while (reader.next(key, value)) {
				keyValue.put(key.toString(), value.toString());
			}
			taskStatistic.add(keyValue);
		}
		
		return taskStatistic;
	}
}
