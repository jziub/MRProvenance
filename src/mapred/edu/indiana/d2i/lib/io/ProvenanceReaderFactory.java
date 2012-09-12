package edu.indiana.d2i.lib.io;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

/** avoid creating duplicate objects */
public class ProvenanceReaderFactory {	
	public static SequenceFile.Reader createReader(Configuration conf, Path file) throws Exception {
		FileSystem fs = FileSystem.get(URI.create(conf.get("fs.default.name") + file.toUri().getPath()), conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
		return reader;
	}
	
	public static List<SequenceFile.Reader> createReader(
			Configuration conf, Path dir, final String filePattern) throws Exception {	
		List<SequenceFile.Reader> readerList = new ArrayList<SequenceFile.Reader>();
		
		PathFilter filter = new PathFilter() {
			@Override
			public boolean accept(Path path) {
				if (path.getName().contains(filePattern))
					return true;
				else
					return false;
			}
		};
		FileSystem fs = FileSystem.get(new URI(conf.get("fs.default.name") + dir.toUri().getPath()), conf);
		FileStatus[] status = fs.listStatus(dir, filter);
		for (FileStatus fileStatus : status) {
			String name = fileStatus.getPath().toUri().getPath();
			FileSystem mapkeyfs = FileSystem.get(URI.create(conf.get("fs.default.name") + name), conf);
			SequenceFile.Reader reader = new SequenceFile.Reader(
					mapkeyfs, new Path(name), conf);
			readerList.add(reader);
		}
		return readerList;
		
//		if (!readers.containsKey(filePattern)) {
//			List<SequenceFile.Reader> readerList = new ArrayList<SequenceFile.Reader>();
//			
//			PathFilter filter = new PathFilter() {
//				@Override
//				public boolean accept(Path path) {
//					if (path.getName().contains(filePattern))
//						return true;
//					else
//						return false;
//				}
//			};
//			FileSystem fs = FileSystem.get(new URI(conf.get("fs.default.name") + dir.toUri().getPath()), conf);
//			FileStatus[] status = fs.listStatus(dir, filter);
//			for (FileStatus fileStatus : status) {
//				String name = fileStatus.getPath().toUri().getPath();
//				FileSystem mapkeyfs = FileSystem.get(URI.create(conf.get("fs.default.name") + name), conf);
//				SequenceFile.Reader reader = new SequenceFile.Reader(
//						mapkeyfs, new Path(name), conf);
//				readerList.add(reader);
//			}
//			
//			readers.put(filePattern, readerList);
//		}
//		return readers.get(filePattern);
	}
}
