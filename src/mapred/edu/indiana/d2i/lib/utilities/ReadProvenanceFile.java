package edu.indiana.d2i.lib.utilities;

import java.io.File;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.indiana.d2i.lib.io.ProvenanceReaderFactory;

public class ReadProvenanceFile extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		System.out.println("Read Single Provenance File");
		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", args[0]);
		Path path = new Path(args[1]);
		FileWriter writer = new FileWriter(new File(args[2]));
		
		Reader reader = ProvenanceReaderFactory.createReader(conf, path);
		Writable key = (Writable) reader.getKeyClass().newInstance();
		Writable val = (Writable) reader.getValueClass().newInstance();
		while (reader.next(key, val)) {
			writer.append(key.toString() + "\t" + val.toString() + "\n");
		}
		reader.close();
		writer.close();
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new RetrieveProvenance(), args);
		System.exit(0);
	}
}
