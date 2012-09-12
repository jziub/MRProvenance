package edu.indiana.d2i.util;

import java.io.File;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ProvenanceInforGenerator {
	private final String delimitor = "\t";
	
	private org.apache.hadoop.mapreduce.Mapper.Context context = null; 
	
	private InputSplit split;
	private RecordReader reader;
	private String fileEntirePath = "";
	
	private void initialize() {
		// get file name
		Path fileName = ((FileSplit) split).getPath(); 
		
		// get the entire path for that file
		StringBuilder strB = new StringBuilder();
		while (fileName != null) {
			strB.insert(0, fileName.getName());		
			fileName = fileName.getParent();
			if (fileName != null)
				strB.insert(0, File.separator);;
		}
		fileEntirePath = strB.toString();
	}
	
	public ProvenanceInforGenerator(InputSplit split, RecordReader reader) {
		this.split = split;
		this.reader = reader;
		initialize();
	}
	
	public ProvenanceInforGenerator(org.apache.hadoop.mapreduce.Mapper.Context context) {
		this.context = context;
		this.split = context.getInputSplit();
		initialize();
	}
	
	public String getProvenanceInfo() {
//		return fileEntirePath + delimitor + reader.getCurrentOffset();
		return fileEntirePath;
	}
}
