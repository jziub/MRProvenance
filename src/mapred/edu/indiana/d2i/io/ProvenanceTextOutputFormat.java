package edu.indiana.d2i.io;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import edu.indiana.d2i.Constants;
import edu.indiana.d2i.Provenance;
import edu.indiana.d2i.util.ProvenanceWritter;

public class ProvenanceTextOutputFormat<K, V> extends TextOutputFormat<K, V> {
	private Provenance.TYPE type;
	
	private static final Log LOG = LogFactory.getLog(ProvenanceTextOutputFormat.class.getName());
	
	/* set the output path for provenance collection */
	private synchronized Path getProvenanceDirectory(TaskAttemptContext context,
			String extension) throws IOException {
		FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
		
		StringBuilder result = new StringBuilder();
		// directory name
		result.append(Constants.PROVENANCE_DIR);
		result.append(context.getJobID().toString() + "/");
		// file name
		TaskID taskId = context.getTaskAttemptID().getTaskID();
	    int partition = taskId.getId();
	    result.append("provenance");
	    result.append('-');
	    result.append(type == Provenance.TYPE.MAP_PROVENANCE ? 'm' : 'r');
	    result.append('-');
	    result.append(partition);
	    result.append(extension);
		
//		LOG.info(result.toString());
		
//		return new Path(committer.getWorkPath(), result.toString());
		return new Path("." + result.toString());
	}

	public ProvenanceTextOutputFormat(Provenance.TYPE type) {
		super();
		this.type = type;
	}
	
	/* overwrite it to set a particular output path */
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		boolean isCompressed = getCompressOutput(job);
		String keyValueSeparator = conf.get(
				"mapred.textoutputformat.separator", "\t");
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
					job, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
					conf);
			extension = codec.getDefaultExtension();
		}

		// Path file = getDefaultWorkFile(job, extension);
		Path file = getProvenanceDirectory(job, extension);
		LOG.info("provenance file name " + file.getName());

		FileSystem fs = file.getFileSystem(conf);
		if (!isCompressed) {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
		} else {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new LineRecordWriter<K, V>(new DataOutputStream(
					codec.createOutputStream(fileOut)), keyValueSeparator);
		}
	}
}
