package edu.indiana.d2i.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;

import edu.indiana.d2i.lib.io.ProvenanceMapOutputValue;

public class ProvenanceCombiner<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends
		Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	private Context outer = null;
	private Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> combiner = null;
	private Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context combinerContext = null;
	private IntWritable mapProvenanceStr = null;

	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		combiner.run(combinerContext);
	}

	@SuppressWarnings("unchecked")
	protected void setup(Context context) throws IOException,
			InterruptedException {
		outer = context;

		try {
			Configuration jobConf = context.getConfiguration();
			combiner = ReflectionUtils.newInstance(jobConf.getClass(
					ProvenanceConstants.USER_COMBINER, null, Reducer.class),
					jobConf);

			// set useless arguments for combiner context
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
			if (context.getConfiguration().getBoolean(
					ProvenanceConstants.CAPTURE_MAP_REDUCE_CONNECTION, false)) {
				combinerContext = new ProvenanceCombinerContext(
						context.getConfiguration(), context.getTaskAttemptID(),
						riter, null, null, new CombinerRecordWriter(),
						context.getOutputCommitter(),
						new ReducerStatusReporter(), comparator,
						(Class<KEYIN>) context.getMapOutputKeyClass(),
						(Class<VALUEIN>) context.getMapOutputValueClass());
			} else {
				combinerContext = new OriginalCombinerContext(
						context.getConfiguration(), context.getTaskAttemptID(),
						riter, null, null, new CombinerRecordWriter(),
						context.getOutputCommitter(),
						new ReducerStatusReporter(), comparator,
						(Class<KEYIN>) context.getMapOutputKeyClass(),
						(Class<VALUEIN>) context.getMapOutputValueClass());
			}

		} catch (SecurityException e) {
			e.printStackTrace();
		}
	}

	private class ProvenanceCombinerContext extends
			Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {
		public ProvenanceCombinerContext(Configuration conf,
				TaskAttemptID taskid, RawKeyValueIterator input,
				Counter inputKeyCounter, Counter inputValueCounter,
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

		/**
		 * Advance to the next key/value pair.
		 */
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
			List<VALUEIN> values = new ArrayList<VALUEIN>();
			Iterable<ProvenanceMapOutputValue<Writable>> valuesWrapped = (Iterable<ProvenanceMapOutputValue<Writable>>) outer
					.getValues();
			for (ProvenanceMapOutputValue<Writable> object : valuesWrapped) {
				values.add((VALUEIN) object.get());
				if (mapProvenanceStr == null) {
					mapProvenanceStr = object.getMarker();
					break;
				}
			}
			return values;
		}
	}

	private class OriginalCombinerContext extends
			Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {
		public OriginalCombinerContext(Configuration conf,
				TaskAttemptID taskid, RawKeyValueIterator input,
				Counter inputKeyCounter, Counter inputValueCounter,
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

		/**
		 * Advance to the next key/value pair.
		 */
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

		public Iterable<VALUEIN> getValues() throws IOException,
				InterruptedException {
			return outer.getValues();
		}
	}

	private class CombinerRecordWriter extends RecordWriter<KEYOUT, VALUEOUT> {
		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
		}

		@Override
		public void write(KEYOUT key, VALUEOUT value) throws IOException,
				InterruptedException {
			// wrap
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