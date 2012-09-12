package edu.indiana.d2i.lib.collector;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.indiana.d2i.lib.ProvenanceConstants;
import edu.indiana.d2i.lib.io.ProvenanceMapOutputValue;
import edu.indiana.d2i.lib.io.ProvenanceWriterFactory;

@SuppressWarnings("rawtypes")
public class SampleReduceProvenanceCollector<INKEY extends WritableComparable, INVALUE extends Writable, OUTKEY extends Writable, OUTVALUE extends Writable>
		implements ProvenanceCollector<INKEY, INVALUE, OUTKEY, OUTVALUE> {
	protected org.apache.hadoop.mapreduce.Reducer.Context context = null;
	/* <key, <task_id ...>> */
	protected TreeMap<Integer, ArrayList<Integer>> keys = null;
	protected long inputDataSize = 0; 
	protected ByteArrayOutputStream out = null;
	
	protected long startTime = -1;
	protected long endTime = -1;
	protected long overheadTime = 0;

	protected boolean captureMapReduceConnection = false;
	protected boolean captureReduceStatistic = false;

	public SampleReduceProvenanceCollector(
			org.apache.hadoop.mapreduce.Reducer.Context context) {
		this.context = context;
		this.keys = new TreeMap<Integer, ArrayList<Integer>>();
		this.out = new ByteArrayOutputStream();

		captureMapReduceConnection = context.getConfiguration().getBoolean(
				ProvenanceConstants.CAPTURE_MAP_REDUCE_CONNECTION, false);
		captureReduceStatistic = context.getConfiguration().getBoolean(
				ProvenanceConstants.CAPTURE_REDUCE_STATISTIC, false);

		// kick off time recorder
		startTime = System.nanoTime();
	}

	private String getProvenanceFileName(String pattern) {
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append(context.getConfiguration().get(
				ProvenanceConstants.PROVENANCE_STORE_DIR));
		strBuilder.append(context.getJobID().toString()
				+ java.io.File.separator);
		strBuilder.append(pattern);
		strBuilder.append(context.getTaskAttemptID().getTaskID().getId());

		return strBuilder.toString();
	}
	
	public void setOverheadTime(long overheadTime) {
		this.overheadTime = overheadTime;
	}

	@Override
	public void close() throws IOException {
		// end time recorder
		endTime = System.nanoTime();

		// write <key, <task_id ...>>
		if (captureMapReduceConnection) {
//			Writer keydistWriter = ProvenanceWriterFactory.createWriter(
//					context.getConfiguration(),
//					getProvenanceFileName("reduce_keydist"), IntWritable.class,
//					Text.class);
//			IntWritable keyHashCode = new IntWritable();
//			Text textVal = new Text();
//			Iterator<Integer> iterator = keys.keySet().iterator();
//			while (iterator.hasNext()) {
//				int key = iterator.next();
//				ArrayList<Integer> list = keys.get(key);
//				StringBuilder strBuilder = new StringBuilder();
//				for (Integer value : list) {
//					strBuilder.append(value);
//					strBuilder.append(ProvenanceConstants.TEXT_DELIMITOR);
//				}
//
//				keyHashCode.set(key);
//				textVal.set(strBuilder.toString());
//				// writer.writeKeyDistribution(keyHashCode, textVal);
//				keydistWriter.append(keyHashCode, textVal);
//			}
//			keydistWriter.close();
		}

		// write reduce statistic
		if (captureReduceStatistic) {
			Text textKey = new Text("ElapsedTime");
			Text textVal = new Text();
			textVal.set(String.valueOf((endTime - startTime - overheadTime) / TIME_SCALE));			
			
			Writer statisticWriter = ProvenanceWriterFactory.createWriter(
					context.getConfiguration(),
					getProvenanceFileName("reduce_statistic"), Text.class,
					Text.class);
			statisticWriter.append(textKey, textVal);
			
//			textKey.set("InputDataSize");
//			textVal.set(String.valueOf(inputDataSize));
//			statisticWriter.append(textKey, textVal);
			
			statisticWriter.close();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void collectOutput(OUTKEY key, OUTVALUE value) throws IOException {		
		Writable realValue = value;
		
		if (captureMapReduceConnection) {	
			realValue = ((ProvenanceMapOutputValue<Writable>)value).get();
			
			int hashcode = key.hashCode();
			IntWritable marker = ((ProvenanceMapOutputValue<Writable>)value).getMarker();
			if (keys.containsKey(hashcode)) {
				keys.get(hashcode).add(marker.get());
			} else {
				ArrayList<Integer> array = new ArrayList<Integer>();
				array.add(marker.get());
				keys.put(hashcode, array);
			}
		}
		
		if (captureReduceStatistic) {			
			out.reset(); // reuse the buffer
			DataOutputStream dataIn = new DataOutputStream(out);
			realValue.write(dataIn);
//			key.write(dataIn);
			inputDataSize += dataIn.size();
			dataIn.close();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void collectInput(INKEY key, INVALUE value) throws IOException {
		Writable realValue = value;
		
		if (captureMapReduceConnection) {	
			realValue = ((ProvenanceMapOutputValue<Writable>)value).get();
			
			int hashcode = key.hashCode();
			IntWritable marker = ((ProvenanceMapOutputValue<Writable>)value).getMarker();
			if (keys.containsKey(hashcode)) {
				keys.get(hashcode).add(marker.get());
			} else {
				ArrayList<Integer> array = new ArrayList<Integer>();
				array.add(marker.get());
				keys.put(hashcode, array);
			}
		}
		
		if (captureReduceStatistic) {			
			out.reset(); // reuse the buffer
			DataOutputStream dataIn = new DataOutputStream(out);
			realValue.write(dataIn);
//			key.write(dataIn);
			inputDataSize += dataIn.size();
			dataIn.close();
		}
	}
}
