package edu.indiana.d2i.lib.utilities;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Clue2OutGraph extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String input = args[0];
		String output = args[1];
		
//		FileWriter writer = new FileWriter(new File(output));		
		FileSystem fs = FileSystem.get(getConf());
		DataInputStream d = new DataInputStream(fs.open(new Path(input)));
		BufferedReader reader = new BufferedReader(new InputStreamReader(d));
		BufferedWriter writer = new BufferedWriter(
				new OutputStreamWriter(fs.create(new Path(output), true)));
		
		String line = reader.readLine();
		long totalNode = Long.valueOf(line);
		long count = 0;
		while ((line = reader.readLine()) != null){
			if (line.equals(" ")) {
				writer.write(count + "\n");
			} else {
				String[] splits = line.split("\\s+");
				writer.write(count + " ");
				for (int i = 0; i < splits.length; i++) {
					writer.write(splits[i] + " ");
				}
				writer.write("\n");
			}
			count++;
//			writer.flush();
		}
		reader.close();
		writer.close();
		
		System.out.println("totalNode " + totalNode);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new Clue2OutGraph(), args);
		System.exit(0);
	}
}
