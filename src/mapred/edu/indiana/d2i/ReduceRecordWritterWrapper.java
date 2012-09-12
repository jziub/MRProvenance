package edu.indiana.d2i;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import edu.indiana.d2i.util.NetworkAddress;
import edu.indiana.d2i.util.ProvenanceController;
import edu.indiana.d2i.util.ProvenanceWritter;

public class ReduceRecordWritterWrapper<OUTKEY,OUTVALUE> 
	extends RecordWriter<OUTKEY,OUTVALUE> {
	private final RecordWriter<OUTKEY,OUTVALUE> writter;
	private ProvenanceWritter provenanceWritter;
	private String outputFileName = "";
	private String ipAddr = "";
//	private final TaskAttemptContext taskContext;
//	private final Configuration conf;
	
	private static final Log LOG = LogFactory.getLog(ReduceRecordWritterWrapper.class.getName());
	
	public ReduceRecordWritterWrapper(RecordWriter<OUTKEY,OUTVALUE> writter, 
			TaskAttemptContext taskContext, String outputFileName) {
		this.writter = writter;
		this.outputFileName = outputFileName;
//		this.ipAddr = NetworkAddress.getIP("wlan0");
//		this.conf = taskContext.getConfiguration();
//		this.taskContext = taskContext;
		provenanceWritter = new ProvenanceWritter(taskContext, Provenance.TYPE.REDUCE_PROVENANCE);
	}
	
	
	@Override
	/* reduce provenance: <reduce-out-key, reduce-output-file> */
	public void write(OUTKEY key, OUTVALUE value) throws IOException,
			InterruptedException {
		writter.write(key, value);
		
		/*** capture reduce provenance <key, output> ***/
//		String output = this.ipAddr + "\t" + this.outputFileName;
		String output = 
			ProvenanceController.ProvenanceFormatter.getFileID(ipAddr, outputFileName);
		Provenance provenance = new Provenance(Provenance.TYPE.REDUCE_PROVENANCE, 
				key.toString(), output);
		provenanceWritter.write(provenance);
		
//		LOG.info("RecordWritterWrapper write");
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		writter.close(context);
		provenanceWritter.close();
//		LOG.info("RecordWritterWrapper close");
	}

}
