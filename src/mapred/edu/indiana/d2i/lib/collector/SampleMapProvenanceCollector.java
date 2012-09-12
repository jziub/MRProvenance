package edu.indiana.d2i.lib.collector;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.MapContext;

import edu.indiana.d2i.lib.ProvenanceConstants;
import edu.indiana.d2i.lib.io.IntWritablePair;
import edu.indiana.d2i.lib.io.ProvenanceWriterFactory;

@SuppressWarnings("rawtypes")
public class SampleMapProvenanceCollector<INKEY extends WritableComparable, INVALUE extends Writable, OUTKEY extends WritableComparable, OUTVALUE extends Writable>
		implements ProvenanceCollector<INKEY, INVALUE, OUTKEY, OUTVALUE> {
	protected org.apache.hadoop.mapreduce.TaskInputOutputContext context = null;

	// <provenance name, provenance value>
	protected Map<String, String> provenance = null;
	// <hashcode of key, size of value>
	protected Map<IntWritable, IntWritable> outkeydist = null;
	protected Map<IntWritable, IntWritable> inkeydist = null; // <key, insize>
	protected long startTime = -1;
	protected long endTime = -1;
	protected long overhead = -1;

	protected ByteArrayOutputStream out = null;

	protected boolean captureMapOutputKey = false;
	protected boolean captureMapStatistic = false;
	protected boolean captureMapInputKey = false;

	private void initialize() {
		out = new ByteArrayOutputStream();

		provenance = new HashMap<String, String>();
		outkeydist = new HashMap<IntWritable, IntWritable>();
		inkeydist = new HashMap<IntWritable, IntWritable>();

		// get the entire path for the input file
		InputSplit split = ((MapContext) context).getInputSplit();
		Path fileName = ((FileSplit) split).getPath();
		provenance.put("InputFileName", fileName.toUri().getPath());
		try {
			provenance.put("InputFileSize", String.valueOf(split.getLength()));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		captureMapOutputKey = context.getConfiguration().getBoolean(
				ProvenanceConstants.CAPTURE_MAP_OUTPUTKEY, false);
		captureMapStatistic = context.getConfiguration().getBoolean(
				ProvenanceConstants.CAPTURE_MAP_STATISTIC, false);
		captureMapInputKey = context.getConfiguration().getBoolean(
				ProvenanceConstants.CAPTURE_MAP_INPUTKEY, false);

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

	public SampleMapProvenanceCollector(
			org.apache.hadoop.mapreduce.Mapper.Context context) {
		this.context = context;
		initialize();
	}

	@Override
	public void close() throws IOException {
		// end time recorder
		endTime = System.nanoTime();

		// write key distribution
		if (captureMapOutputKey) {
			Writer keydistWriter = ProvenanceWriterFactory.createWriter(
					context.getConfiguration(),
					getProvenanceFileName("map_keydist"), IntWritable.class,
					IntWritable.class);
			
			Iterator<IntWritable> keyIter = outkeydist.keySet().iterator();
			while (keyIter.hasNext()) {
				IntWritable key = keyIter.next();
				keydistWriter.append(key, outkeydist.get(key));
			}
			keydistWriter.close();
		}
		
		//
		if (captureMapInputKey) {
			Writer keydistWriter = ProvenanceWriterFactory.createWriter(
					context.getConfiguration(),
					getProvenanceFileName("map_inkeydist"), IntWritable.class,
					IntWritable.class);
			
			Iterator<IntWritable> keyIter = inkeydist.keySet().iterator();
			while (keyIter.hasNext()) {
				IntWritable key = keyIter.next();
				keydistWriter.append(key, inkeydist.get(key));
			}
			keydistWriter.close();
		}

		// write task statistic
		if (captureMapStatistic) {
			provenance.put("ElapsedTime",
					String.valueOf((endTime - startTime) / TIME_SCALE));

			Writer statisticWriter = ProvenanceWriterFactory.createWriter(
					context.getConfiguration(),
					getProvenanceFileName("map_statistic"), Text.class,
					Text.class);
			Text provenanceName = new Text();
			Text provenanceVal = new Text();
			Iterator<String> iterator = provenance.keySet().iterator();
			while (iterator.hasNext()) {
				String name = iterator.next();
				provenanceName.set(name);
				provenanceVal.set(provenance.get(name));
				statisticWriter.append(provenanceName, provenanceVal);
			}
			statisticWriter.close();
		}
	}

	public void collectOutput(OUTKEY key, OUTVALUE value) throws IOException {
		if (captureMapOutputKey == false)
			return;

		out.reset(); // reuse the buffer
		DataOutputStream dataOut = new DataOutputStream(out);

		// key distribution
		if (captureMapOutputKey) {
			value.write(dataOut);
			IntWritable hashcode = new IntWritable(key.hashCode());
			
			if (outkeydist.containsKey(hashcode)) {
				outkeydist.get(hashcode).set(outkeydist.get(hashcode).get() + dataOut.size());
			} else {
				outkeydist.put(hashcode, new IntWritable(dataOut.size()));
			}
		}

		// data distributed among tasks
		dataOut.close();
	}

	@Override
	public void collectInput(INKEY key, INVALUE value) throws IOException {
		if (!captureMapInputKey) return;
		
		out.reset(); // reuse the buffer
		DataOutputStream dataOut = new DataOutputStream(out);
		value.write(dataOut);
		
		IntWritable hashcode = new IntWritable(key.hashCode());
		if (inkeydist.containsKey(hashcode)) {
			inkeydist.get(hashcode).set(inkeydist.get(hashcode).get()+dataOut.size());
		} else {
			inkeydist.put(hashcode, new IntWritable(dataOut.size()));
		}
		dataOut.close();
	}
}
