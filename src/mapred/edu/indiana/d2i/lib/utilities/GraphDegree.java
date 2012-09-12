package edu.indiana.d2i.lib.utilities;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GraphDegree extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String input = args[0];
		String output = args[1];
		
		FileWriter writer = new FileWriter(new File(output));		
		FileSystem fs = FileSystem.get(getConf());
		DataInputStream d = new DataInputStream(fs.open(new Path(input)));
		BufferedReader reader = new BufferedReader(new InputStreamReader(d));
		String line = "";
		while ((line = reader.readLine()) != null){
			String[] splits = line.split("\\s+");
			writer.write((splits.length-1) + "\n");
			writer.flush();
		}
		reader.close();
		writer.close();
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new GraphDegree(), args);
		System.exit(0);
	}
}
