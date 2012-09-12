package edu.indiana.d2i.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 
 * 
 * 
 * @author root
 * 
 * @param <V>
 */
@SuppressWarnings("rawtypes")
public class MapOutputValueWrapper implements Writable {
	private static Class[] CLASSES = {
		IntWritable.class,
		LongWritable.class,
		FloatWritable.class,
		DoubleWritable.class,
		ByteWritable.class,
		BytesWritable.class,
		BooleanWritable.class,
		Text.class,
		VIntWritable.class,
		VLongWritable.class,
		ArrayWritable.class,
		TwoDArrayWritable.class,
	};
	private String inputURI; //
	private Writable instance = null; //
	private static final byte NOT_SET = -1;
	private byte type = NOT_SET;
	private Configuration conf = null;

	/**
	 * Set the instance that is wrapped.
	 * 
	 * @param obj
	 */
	public void set(Writable obj, String inputURI) {
		this.instance = obj;
		this.inputURI = inputURI;
		Class<? extends Writable> instanceClazz = instance.getClass();
		for (int i = 0; i < CLASSES.length; i++) {
			Class<? extends Writable> clazz = CLASSES[i];
			if (clazz.equals(instanceClazz)) {
				this.type = (byte) i;
				return;
			}
			
		}
		throw new RuntimeException("The type of instance is: "
                + instance.getClass() + ", which is NOT registered.");
	}

	/**
	 * Return the wrapped instance.
	 */
	public Writable get() {
		return instance;
	}

	public String getInputURI() {
		return inputURI;
	}
	
	public String toString() {
		return "GW["
				+ (instance != null ? ("class=" + instance.getClass().getName()
						+ ",value=" + instance.toString()) : "(null)") + "]";
	}

	public void readFields(DataInput in) throws IOException {
		// read type
		this.type = in.readByte();
		Class<? extends Writable> clazz = CLASSES[type & 0xff];
		
		// read inputURI
		inputURI = in.readLine();

		// read instance
		try {
			instance = ReflectionUtils.newInstance(clazz, conf);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException("Cannot initialize the class: "
					+ clazz);
		}
		instance.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		if (type == NOT_SET || instance == null)
		      throw new IOException("The MapOutputValueWrapper has NOT been set correctly. type="
		                            + type + ", instance=" + instance);
		
		// write type
		out.writeByte(type);
		
		// write inputURI
		out.writeBytes(this.inputURI + "\n");

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
		 IntWritable value = new IntWritable(1);
		 String inputURI = "file01";
		
		 // mapper's side
		 Configuration conf = new Configuration();
		 MapOutputValueWrapper wrapper = new MapOutputValueWrapper();
		 wrapper.set(value, inputURI);
		 wrapper.setConf(conf);
		 System.out.println(wrapper.get().getClass().getName());
		 
		 try {
			byte[] bytes = serialize(wrapper);
			
			/* shuffle and sort */
			
			// reducer's side
			MapOutputValueWrapper dewrapper = new MapOutputValueWrapper();
			dewrapper.setConf(conf);
			deserialize(dewrapper, bytes);
			
			// test
			String uri = dewrapper.getInputURI();
			IntWritable writable = (IntWritable)dewrapper.get();
			System.out.println("inputURI " + uri);
			System.out.println("writable.get() " + writable.get());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}