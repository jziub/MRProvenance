package edu.indiana.d2i.lib.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import edu.indiana.d2i.lib.ProvenanceConstants;


@SuppressWarnings("rawtypes")
public class ProvenanceMapOutputValue<T extends Writable> implements Writable, Configurable {
	private IntWritable marker; //
	private T instance; //
	private Configuration conf = null;
	
	/**
	 * Set the instance that is wrapped.
	 * 
	 * @param obj
	 */
	public void set(T obj, IntWritable marker) {
		this.instance = obj;
		this.marker = marker;
	}

	/**
	 * Return the wrapped instance.
	 */
	public T get() {
		return instance;
	}

	public IntWritable getMarker() {
		return marker;
	}
	
	public String toString() {
		return "GW["
				+ (instance != null ? ("class=" + instance.getClass().getName()
						+ ",value=" + instance.toString()) : "(null)") + "]";
	}

	@SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException {		
		// read inputURI
		marker = new IntWritable();
		marker.readFields(in);

		// read instance
		if (conf == null) System.out.println("!!!! conf is null !!!!");
		if (conf.getClass(ProvenanceConstants.USER_MAP_OUTPUT, Writable.class) == null)
			System.out.println("!!! getclass is null");
		
		instance = (T) ReflectionUtils.newInstance
			(conf.getClass(ProvenanceConstants.USER_MAP_OUTPUT, Writable.class), conf);
//		instance = (T) ReflectionUtils.newInstance
//			(Text.class, conf);
		instance.readFields(in);
	}

	public void write(DataOutput out) throws IOException {	
		// write inputURI
		marker.write(out);

		// write instance
		instance.write(out);
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	/* test drive */
	
	public static byte[] serialize(Writable writable) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		writable.write(dataOut);
		dataOut.close();
		return out.toByteArray();
	}
	
	public static void deserialize(Writable writable, byte[] bytes) 
		throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);
		writable.readFields(dataIn);
		dataIn.close();
	}
	
	public static void main(String[] args) {
		 // map output value
		 Text value = new Text("hello world");;
		 IntWritable inputURI = new IntWritable(1);
		
		 // mapper's side
		 Configuration conf = new Configuration();
		 conf.setClass(ProvenanceConstants.USER_MAP_OUTPUT, Text.class, Writable.class);
		 
		 ProvenanceMapOutputValue<Text> wrapper = new ProvenanceMapOutputValue<Text>();
		 wrapper.set(value, inputURI);
		 wrapper.setConf(conf);
		 System.out.println(wrapper.get().getClass().getName());
		 
		 try {
			byte[] bytes = serialize(wrapper);
			
			/* shuffle and sort */
			
			// reducer's side
			ProvenanceMapOutputValue dewrapper = new ProvenanceMapOutputValue();
			dewrapper.setConf(conf);
			deserialize(dewrapper, bytes);
			
			// test
			IntWritable uri = dewrapper.getMarker();
			Text writable = (Text) dewrapper.get();
			System.out.println("inputURI " + uri);
			System.out.println("writable " + writable);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}