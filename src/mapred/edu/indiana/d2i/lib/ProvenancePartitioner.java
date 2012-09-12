package edu.indiana.d2i.lib;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Partitioner;

import edu.indiana.d2i.lib.io.ArrayOfListIntWritable;
import edu.indiana.d2i.lib.io.ProvenanceReaderFactory;


public class ProvenancePartitioner<KEY, VALUE> 
	extends Partitioner<KEY, VALUE> implements Configurable {
	private Map<Integer, Integer> plan = new HashMap<Integer, Integer>(); // <hash code for key, reducer>
	private Configuration conf = null;
	
	@Override
	public int getPartition(KEY key, VALUE value, int numPartitions) {	
		return plan.get(key.hashCode());
//		return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

	@Override
	public void setConf(Configuration conf) {		
		this.conf = conf;
		
		SequenceFile.Reader reader = null;
		try {
			Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
			reader = new SequenceFile.Reader(
					localFiles[0].getFileSystem(conf), localFiles[0], conf); 
			IntWritable key = new IntWritable();
			ArrayOfListIntWritable val = new ArrayOfListIntWritable();
			while (reader.next(key, val)) {
				List<IntWritable>[] keys = val.getArrayOfListInt();
				for (int i = 0; i < keys.length; i++) {
					for (IntWritable keyhashcode : keys[i]) {
						plan.put(keyhashcode.get(), i);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
		
//		try {
//			Reader reader = ProvenanceReaderFactory.createReader(
//					conf, new Path(conf.get(ProvenanceConstants.PARTITION_PLAN_LOCATION)));
//			IntWritable key = new IntWritable();
//			ArrayOfListIntWritable val = new ArrayOfListIntWritable();
//			while (reader.next(key, val)) {
//				List<IntWritable>[] keys = val.getArrayOfListInt();
//				for (int i = 0; i < keys.length; i++) {
//					for (IntWritable keyhashcode : keys[i]) {
//						plan.put(keyhashcode.get(), i);
//					}
//				}
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
	}

	@Override
	public Configuration getConf() {
		return conf;
	}
}
