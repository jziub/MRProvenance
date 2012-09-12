package edu.indiana.d2i.lib.io;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

public class ProvenanceWriterFactory {
//	private static Map<String, SequenceFile.Writer> writers = new HashMap<String, SequenceFile.Writer>();

	public static SequenceFile.Writer createWriter(
			Configuration conf, String fileName, Class<? extends Writable> keyClass, Class<? extends Writable> valueClass) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(fileName), conf);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs,
				conf, new Path(fileName), keyClass, valueClass, SequenceFile.CompressionType.BLOCK);
//		SequenceFile.Writer writer = SequenceFile.createWriter(fs,
//				conf, new Path(fileName), keyClass, valueClass);
		return writer;
		
//		if (!writers.containsKey(fileName)) {
//			FileSystem fs = FileSystem.get(URI.create(fileName), conf);
//			SequenceFile.Writer writer = SequenceFile.createWriter(fs,
//					conf, new Path(fileName), keyClass, valueClass, SequenceFile.CompressionType.BLOCK);
//			writers.put(fileName, writer);
//		}
//		return writers.get(fileName);
	}
}
