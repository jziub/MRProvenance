package edu.indiana.d2i.algorithm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.servlet.jsp.JspWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ProvenanceTracking { 
	private final String PROVENANCE_DIR = "/user/hadoop/provenance/";
	private String uri;
	private String jobid;
	private Configuration conf;
	private JspWriter out;
	
	private static final Log LOG = LogFactory.getLog(ProvenanceTracking.class.getName());
	
	/**
	 * uri right now is the key in map provenance file
	 */
	public ProvenanceTracking(String jobid, String uri, Configuration conf, JspWriter out) {
		this.uri = uri;
		this.jobid = jobid;
		this.conf = conf;
		this.out = out;
	}
	
	/**
	 * a very naive backtrack algorithm, just search the map provenance linearly
	 */
	public void backTrack() {		
		try {
			// open the map provenance file
			FileSystem fs = FileSystem.get(conf);
			StringBuilder strB = new StringBuilder();
			strB.append(PROVENANCE_DIR);
			strB.append("/");
			strB.append(jobid);
			strB.append("/");
			strB.append("provenance-m-0");
			Path inFile = new Path(strB.toString());

			// linearly scan the whole file
			FSDataInputStream in = fs.open(inFile);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String line;
			while ((line = reader.readLine()) != null) {
				String[] splits = line.split("\t");
				if (splits[0].equals(uri)) {
					// read provenance
					Path provenancePath = new Path(splits[1]);
					FSDataInputStream provenanceFile = fs.open(provenancePath);
					BufferedReader provenanceReader = new BufferedReader(new InputStreamReader(in));
					
					// dump the provenance to the web page
					int len = 512;
//					char[] cbuf = new char[len];
//					provenanceReader.read(cbuf, Integer.valueOf(splits[2]), len);
					byte[] buf = new byte[len];
					int actualRead = provenanceFile.read(Integer.valueOf(splits[2]), buf, 0, len);
//					provenanceFile.readFully(buf, Integer.valueOf(splits[2]), len);
					
					// format the output
					String output = new String(buf);
					out.println("<p>" + output + "</p>");
					out.println("it is in damp tracking!!!");
					
					
//					out.println(splits[1]);
//					out.println(splits[2]);
				}
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
