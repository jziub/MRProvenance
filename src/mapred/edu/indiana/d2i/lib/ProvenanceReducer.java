package edu.indiana.d2i.lib;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;

import edu.indiana.d2i.lib.collector.ProvenanceCollector;
import edu.indiana.d2i.lib.collector.SampleReduceProvenanceCollector;
import edu.indiana.d2i.lib.io.ProvenanceMapOutputValue;

@SuppressWarnings({ "deprecation", "rawtypes" })
public class ProvenanceReducer<KEYIN extends WritableComparable, VALUEIN extends Writable, KEYOUT extends Writable, VALUEOUT extends Writable>
		extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	private Context outer = null;
	private Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer = null;
	private Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context reducerContext = null;
	private SampleReduceProvenanceCollector<KEYIN, Writable, KEYOUT, VALUEOUT> reduceProvenanceCollector = null;

	//
	private long overheadTime = 0;
	
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		reducer.run(reducerContext);
		cleanup(context);
	}

	public void cleanup(Context context) throws IOException,
			InterruptedException {
		reduceProvenanceCollector.setOverheadTime(overheadTime);
		reduceProvenanceCollector.close();
	}

	@SuppressWarnings("unchecked")
	protected void setup(Context context) throws IOException,
			InterruptedException {
		outer = context;
		Class<SampleReduceProvenanceCollector> provenanceCollectorClass = (Class<SampleReduceProvenanceCollector>) context
				.getConfiguration().getClass(
						ProvenanceConstants.REDUCE_PROVENANCE_COLLECTOR,
						ProvenanceCollector.class);

		try {
			if (provenanceCollectorClass == null) {
				reduceProvenanceCollector = new SampleReduceProvenanceCollector<KEYIN, Writable, KEYOUT, VALUEOUT>(
						context);
			} else {
				Constructor<SampleReduceProvenanceCollector> constructor = provenanceCollectorClass
						.getConstructor(Context.class);
				reduceProvenanceCollector = constructor.newInstance(context);
			}

			Configuration jobConf = context.getConfiguration();
			reducer = ReflectionUtils.newInstance(jobConf.getClass(
					ProvenanceConstants.USER_REDUCER, null, Reducer.class),
					jobConf);

			// set useless arguments for Reduce context
			JobConf job = (JobConf) context.getConfiguration();
			RawComparator comparator = job.getOutputValueGroupingComparator();
			RawKeyValueIterator riter = new RawKeyValueIterator() {
				public void close() throws IOException {
				}

				public DataInputBuffer getKey() throws IOException {
					return null;
				}

				public Progress getProgress() {
					return null;
				}

				public DataInputBuffer getValue() throws IOException {
					return null;
				}

				public boolean next() throws IOException {
					return true;
				}
			};

			// set up a wrapper for reducer context
			if (context.getConfiguration().getBoolean(ProvenanceConstants.CAPTURE_MAP_REDUCE_CONNECTION, false)) {
				reducerContext = new ProvenanceReducerContext(context.getConfiguration(),
						context.getTaskAttemptID(), riter, null, null,
						new ReducerRecordWriter(), context.getOutputCommitter(),
						new ReducerStatusReporter(), comparator,
						(Class<KEYIN>) context.getMapOutputKeyClass(),
						(Class<VALUEIN>) context.getMapOutputValueClass());
			} else {
				System.out.println("!!! OriginalReducerContext");
				reducerContext = new OriginalReducerContext(context.getConfiguration(),
						context.getTaskAttemptID(), riter, null, null,
						new ReducerRecordWriter(), context.getOutputCommitter(),
						new ReducerStatusReporter(), comparator,
						(Class<KEYIN>) context.getMapOutputKeyClass(),
						(Class<VALUEIN>) context.getMapOutputValueClass());
			}
			
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}

	private class ProvenanceReducerContext extends
			Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {
		public ProvenanceReducerContext(Configuration conf, TaskAttemptID taskid,
				RawKeyValueIterator input, Counter inputKeyCounter,
				Counter inputValueCounter,
				RecordWriter<KEYOUT, VALUEOUT> output,
				OutputCommitter committer, StatusReporter reporter,
				RawComparator<KEYIN> comparator, Class<KEYIN> keyClass,
				Class<VALUEIN> valueClass) throws IOException,
				InterruptedException {
			super(conf, taskid, input, inputKeyCounter, inputValueCounter,
					output, committer, reporter, comparator, keyClass,
					valueClass);
		}

		/** Start processing next unique key. */
		public boolean nextKey() throws IOException, InterruptedException {
			return outer.nextKey();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return outer.nextKeyValue();
		}

		public KEYIN getCurrentKey() {
			return outer.getCurrentKey();
		}

		@SuppressWarnings("unchecked")
		@Override
		public VALUEIN getCurrentValue() {
			return (VALUEIN) ((ProvenanceMapOutputValue<Writable>) outer
					.getCurrentValue()).get();
		}

		@SuppressWarnings("unchecked")
		public Iterable<VALUEIN> getValues() throws IOException,
				InterruptedException {
			long start = System.nanoTime();
			
			List<VALUEIN> values = new ArrayList<VALUEIN>();
			Iterable<ProvenanceMapOutputValue<Writable>> valuesWrapped = (Iterable<ProvenanceMapOutputValue<Writable>>) outer
					.getValues();

			/* build a list like <key, <src1, src2, ... srcN>> */
			for (ProvenanceMapOutputValue<Writable> object : valuesWrapped) {
				values.add((VALUEIN) object.get());
				reduceProvenanceCollector.collectInput(outer.getCurrentKey(), object);
			}
			
			overheadTime += System.nanoTime() - start;

			return values;
		}
	}

	private class OriginalReducerContext extends
			Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {
		public OriginalReducerContext(Configuration conf, TaskAttemptID taskid,
				RawKeyValueIterator input, Counter inputKeyCounter,
				Counter inputValueCounter,
				RecordWriter<KEYOUT, VALUEOUT> output,
				OutputCommitter committer, StatusReporter reporter,
				RawComparator<KEYIN> comparator, Class<KEYIN> keyClass,
				Class<VALUEIN> valueClass) throws IOException,
				InterruptedException {
			super(conf, taskid, input, inputKeyCounter, inputValueCounter,
					output, committer, reporter, comparator, keyClass,
					valueClass);
		}

		/** Start processing next unique key. */
		public boolean nextKey() throws IOException, InterruptedException {
			return outer.nextKey();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return outer.nextKeyValue();
		}

		public KEYIN getCurrentKey() {
			return outer.getCurrentKey();
		}

		@Override
		public VALUEIN getCurrentValue() {
			return outer.getCurrentValue();
		}

		/* can't collect any thing here due to an unknown bug */
		public Iterable<VALUEIN> getValues() throws IOException,
				InterruptedException {			
			return outer.getValues();
		}
	}

	private class ReducerRecordWriter extends RecordWriter<KEYOUT, VALUEOUT> {
		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
		}

		@Override
		public void write(KEYOUT key, VALUEOUT value) throws IOException,
				InterruptedException {
			outer.write(key, value);
		}
	}

	private class ReducerStatusReporter extends StatusReporter {
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
