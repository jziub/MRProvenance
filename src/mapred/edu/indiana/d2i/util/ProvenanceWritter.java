package edu.indiana.d2i.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.indiana.d2i.Provenance;
import edu.indiana.d2i.io.ProvenanceTextOutputFormat;

public class ProvenanceWritter {
	private RecordWriter<Text, Text> provenanceWritter; 
	private Text provenanceKey = new Text(); 
	private Text provenanceVal = new Text();
	private TaskAttemptContext taskContext;
	
	private static final Log LOG = LogFactory.getLog(ProvenanceWritter.class.getName()); 
	
	public ProvenanceWritter(TaskAttemptContext taskContext, Provenance.TYPE type) {
		this.taskContext = taskContext;
		try {
			provenanceWritter = 
				new ProvenanceTextOutputFormat<Text, Text>(type).getRecordWriter(taskContext);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
		LOG.info("ProvenanceWritter init");
	}
	
	public void write(Provenance provenance) throws IOException, InterruptedException {
		LOG.info("ProvenanceWritter write");
		
		List<HashMap<String, String>> provenances = provenance.getProvenances();
		for (HashMap<String, String> keyVal : provenances) {
			String strKey = keyVal.keySet().iterator().next();
			String strVal = keyVal.get(strKey);			
			provenanceKey.set(strKey);
			provenanceVal.set(strVal);
			
//			LOG.info("strKey " + strKey);
//			LOG.info("strVal " + strVal);
//			LOG.info("provenanceKey " + provenanceKey.toString());
//			LOG.info("provenanceVal " + provenanceVal.toString());
			
			provenanceWritter.write(provenanceKey, provenanceVal);
		}
	}
	
	/* just use close() ??? */
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		provenanceWritter.close(context);
	}
	
	public void close() throws IOException, InterruptedException {
		provenanceWritter.close(this.taskContext);
	}
}
