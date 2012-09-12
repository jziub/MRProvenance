package edu.indiana.d2i.lib;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import edu.indiana.d2i.lib.collector.ProvenanceCollector;
import edu.indiana.d2i.lib.collector.SampleMapProvenanceCollector;
import edu.indiana.d2i.lib.io.ProvenanceMapOutputValue;

@SuppressWarnings("rawtypes")
public class ProvenanceMapper<INKEY extends WritableComparable, INVALUE extends Writable, OUTKEY extends WritableComparable, OUTVALUE extends Writable>
		extends Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE> {
	private Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE> mapper = null; // ?
	private Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context mappercontext = null;
	private Context outer = null;
	private SampleMapProvenanceCollector<INKEY, INVALUE, OUTKEY, OUTVALUE> mapProvenanceCollector = null;
	private IntWritable taskConnection = null;

	// optimization
	private ProvenanceMapOutputValue<Writable> valueWrapper = new ProvenanceMapOutputValue<Writable>();

	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		mapper.run(mappercontext);
		cleanup(context);
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		if (outer.getNumReduceTasks() > 0) {
			mapProvenanceCollector.close();
		}
	}

	@SuppressWarnings("unchecked")
	protected void setup(Context context) throws IOException,
			InterruptedException {
		outer = context;
		taskConnection = new IntWritable();
		taskConnection.set(context.getTaskAttemptID().getTaskID().getId());
		Class<SampleMapProvenanceCollector> provenanceCollectorClass = (Class<SampleMapProvenanceCollector>) context
				.getConfiguration().getClass(
						ProvenanceConstants.MAP_PROVENANCE_COLLECTOR,
						ProvenanceCollector.class);
		try {
			if (provenanceCollectorClass == null) {
				mapProvenanceCollector = new SampleMapProvenanceCollector<INKEY, INVALUE, OUTKEY, OUTVALUE>(context);
			} else {
				Constructor<SampleMapProvenanceCollector> constructor = provenanceCollectorClass
						.getConstructor(Context.class);
				mapProvenanceCollector = constructor.newInstance(context);
			}
			
			// set up original mapper
			Class<Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>> mapperClass = (Class<Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>>) context
					.getConfiguration().getClass(
							ProvenanceConstants.USER_MAPPER, Mapper.class);
			mapper = ReflectionUtils.newInstance(mapperClass,
					context.getConfiguration());

			// set up original context for original mapper
			boolean capMRConnection = context.getConfiguration().getBoolean(ProvenanceConstants.CAPTURE_MAP_REDUCE_CONNECTION, false);
			if (context.getNumReduceTasks() > 0 && capMRConnection) {
				mappercontext = new Context(context.getConfiguration(),
						context.getTaskAttemptID(), new MapperRecordReader(),
						new MapperRecordWriter(), context.getOutputCommitter(),
						new MapperStatusReporter(), context.getInputSplit());
			}
			else {
				mappercontext = new Context(context.getConfiguration(),
						context.getTaskAttemptID(), new MapperRecordReader(),
						new MapperDirectRecordWriter(), context.getOutputCommitter(),
						new MapperStatusReporter(), context.getInputSplit());
			}
			
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}

	private class MapperRecordReader extends RecordReader<INKEY, INVALUE> {
		private INKEY key;
		private INVALUE value;

		@Override
		public void close() throws IOException {
		}
		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (!outer.nextKeyValue()) {
				return false;
			}
			key = (INKEY) outer.getCurrentKey();
			value = (INVALUE) outer.getCurrentValue();
			mapProvenanceCollector.collectInput(key, value);
			return true;
		}

		public INKEY getCurrentKey() {
			return key;
		}

		@Override
		public INVALUE getCurrentValue() {
			return value;
		}
	}

	@SuppressWarnings("unchecked")
	private class MapperRecordWriter extends RecordWriter<OUTKEY, OUTVALUE> {

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {

		}

		@Override
		public void write(OUTKEY key, OUTVALUE value) throws IOException,
				InterruptedException {
			// collect provenance
			mapProvenanceCollector.collectOutput(key, value);

			// mark connection between mapper and reducer			
			valueWrapper.set((Writable) value, taskConnection);
			valueWrapper.setConf(outer.getConfiguration());

			outer.write((OUTKEY) key, (OUTVALUE) valueWrapper);
		}
	}
	
	private class MapperDirectRecordWriter extends RecordWriter<OUTKEY, OUTVALUE> {
		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {

		}

		@Override
		public void write(OUTKEY key, OUTVALUE value) throws IOException,
				InterruptedException {
			// collect provenance
			mapProvenanceCollector.collectOutput(key, value);
			outer.write(key, value);
		}
	}

	private class MapperStatusReporter extends StatusReporter {

		@Override
		public Counter getCounter(Enum<?> name) {
			return outer.getCounter(name);
		}

		@Override
		public Counter getCounter(String group, String name) {
			return outer.getCounter(group, name);
		}

		@Override
		public void progress() {
			outer.progress();
		}

		@Override
		public void setStatus(String status) {
			outer.setStatus(status);
		}
	}
}
