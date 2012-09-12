package edu.indiana.d2i.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import edu.indiana.d2i.Constants;
import edu.indiana.d2i.Provenance;


public class ProvenanceController {
	private static final Log LOG = LogFactory.getLog(ProvenanceController.class.getName());
	
	public static class ProvenanceFormatter {
		private static String delimitor = "\t";
		private static String fileidDelimitor = "#";
		
		/**
		 * set the format as ip "\t" filename "\t" offset 
		 * fileURI = filename "\t" offset 
		 */
//		public static String getFileID(String ipAddr, String fileName /*String offset*/) {		
////			StringBuilder strB = new StringBuilder();
////			strB.append(ipAddr);
////			strB.append(deliminator);
////			strB.append(fileName);
////			strB.append(deliminator);
////			strB.append(offset);
////			return strB.toString();
//			return ipAddr + deliminator + fileName;
//		}
		
		public static String getFileID(Path fileName, String offset) {					
			StringBuilder strB = new StringBuilder();

			// get the entire path for that file
			while (fileName != null) {
				strB.insert(0, fileName.getName());		
				fileName = fileName.getParent();
				if (fileName != null)
					strB.insert(0, File.separator);;
			}
			
//			strB.append(fileName);
			strB.append(delimitor);
			strB.append(offset);
			return strB.toString();
		}
		
		public static String getFileID(String fileName, String offset) {					
			StringBuilder strB = new StringBuilder();

			strB.append(fileName);
			strB.append(delimitor);
			strB.append(offset);
			return strB.toString();
		}
		
		/**
		 * strlist = <file \t offset, ..., file \t offset>
		 * return <file \t offset #...# file \t offset> 
		 */
		public static String convertFileIDSet2String(java.util.HashSet<String> fileIDSet) {
			StringBuilder strB = new StringBuilder();
			Iterator<String> iter = fileIDSet.iterator();
			while (iter.hasNext()) {
				strB.append(iter.next());
				strB.append(fileidDelimitor);
			}
			return strB.toString();
		}
		
		/**
		 * str = <file \t offset #...# file \t offset>
		 * return <file \t offset, ..., file \t offset> 
		 */
		public static List<String> convertString2FileIDList(String str) {
			String[] split = str.split(fileidDelimitor);
			List<String> fileidList = Arrays.asList(split);
			return fileidList;
		}
		
		public static String provenanceFileName(Provenance.TYPE type) {
			String filename = ""; 
			
			if (type == Provenance.TYPE.MAP_PROVENANCE) {
				filename = Constants.MAP_PROVENANCE_PREFIX;
			}
			else if (type == Provenance.TYPE.REDUCE_PROVENANCE) {
				filename = Constants.REDUCE_PROVENANCE_PREFIX;
			}
			
			return filename;
		}
	}
}
