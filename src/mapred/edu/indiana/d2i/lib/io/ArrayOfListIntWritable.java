package edu.indiana.d2i.lib.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class ArrayOfListIntWritable implements Writable {

	private static class IntArrayWritable extends ArrayWritable {
		public IntArrayWritable() { 
		    super(IntWritable.class); 
		}	
	}
	
	private IntArrayWritable[] keylist;
	
	public ArrayOfListIntWritable() {}
	
	public ArrayOfListIntWritable(List<IntWritable>[] keys) {
		keylist = new IntArrayWritable[keys.length];
		for (int i = 0; i < keys.length; i++) {
			Writable[] values = keys[i].toArray(new Writable[0]);
			keylist[i] = new IntArrayWritable();
			keylist[i].set(values);
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(keylist.length);
		for (int i = 0; i < keylist.length; i++) {
			keylist[i].write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int tmp = in.readInt();
		keylist = new IntArrayWritable[tmp];
		for (int i = 0; i < keylist.length; i++) {
			keylist[i] = new IntArrayWritable();
			keylist[i].readFields(in);
		}
	}
	
	@SuppressWarnings("unchecked")
	public List<IntWritable>[] getArrayOfListInt() {
		List<IntWritable>[] results = (List<IntWritable>[]) new ArrayList[keylist.length];
		for (int i = 0; i < keylist.length; i++) {
			List<Writable> list = Arrays.asList(keylist[i].get());
			List<IntWritable> lst = new ArrayList<IntWritable>();
			for (Writable writable : list) {
				lst.add((IntWritable) writable);
			}
			results[i] = lst;
		}
		
		return results;
	}
}
