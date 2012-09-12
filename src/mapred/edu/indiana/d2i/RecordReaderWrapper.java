package edu.indiana.d2i;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class RecordReaderWrapper<K,V> extends RecordReader<K,V> {
	private final org.apache.hadoop.mapreduce.RecordReader<K,V> reader;
	private String inputURI;
	
	private static final Log LOG = LogFactory.getLog(RecordReaderWrapper.class.getName());
    
    public RecordReaderWrapper(org.apache.hadoop.mapreduce.RecordReader<K,V> reader) {
    	this.reader = reader;
    	
    	LOG.info("RecordReaderWrapper construct");
    }
    
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		reader.initialize(split, context);
		inputURI = ((FileSplit)split).getPath().getName();
	}
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return reader.nextKeyValue();
	}
	@Override
	public K getCurrentKey() throws IOException, InterruptedException {
		return reader.getCurrentKey();
	}
	@Override
	public V getCurrentValue() throws IOException, InterruptedException {
		return reader.getCurrentValue();
	}
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return reader.getProgress();
	}
	@Override
	public void close() throws IOException {
		reader.close();
		
		LOG.info("RecordReaderWrapper close");
	}
}
