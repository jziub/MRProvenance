package edu.indiana.d2i.lib.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntWritablePair implements Writable {	
	private IntWritable first;
	private IntWritable second;

	public IntWritablePair() {}
	
	public IntWritablePair(IntWritable first, IntWritable second) {
		this.first = first;
		this.second = second;
	}
	
	public IntWritable getFirst() {
		return first;
	}
	
	public IntWritable getSecond() {
		return second;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

}
