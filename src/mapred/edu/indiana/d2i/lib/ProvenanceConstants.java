package edu.indiana.d2i.lib;

public class ProvenanceConstants {
	public static String USER_MAPPER = "edu.indiana.d2i.lib.usermapper";
	public static String USER_REDUCER = "edu.indiana.d2i.lib.userreducer";
	public static String USER_COMBINER = "edu.indiana.d2i.lib.usercombiner";
	public static String USER_MAP_OUTPUT = "edu.indiana.d2i.lib.usermapout";
	public static String MAP_PROVENANCE_COLLECTOR = "edu.indiana.d2i.lib.mapprovenance.collector";
	public static String REDUCE_PROVENANCE_COLLECTOR = "edu.indiana.d2i.lib.reduceprovenance.collector";
	
	public static String PROVENANCE_STORE_DIR = "edu.indiana.d2i.lib.provenance.dir";
	public static String PROVENANCE_STORE_DIR_DEFAULTNAME = "/provenance/";
	public static String PARTITION_PLAN_LOCATION = "edu.indiana.d2i.lib.partition";
	public static String PARTITION_PLAN_DEFAULT_NAME = "partitionplan";
	
	public static String CAPTURE_MAP_REDUCE_CONNECTION = "edu.indiana.d2i.lib.capturereduceoutput";
	public static String CAPTURE_REDUCE_STATISTIC = "edu.indiana.d2i.lib.capturereducestatistic";
	public static String CAPTURE_MAP_OUTPUTKEY = "edu.indiana.d2i.lib.capturemapoutput";
	public static String CAPTURE_MAP_INPUTKEY = "edu.indiana.d2i.lib.capturemapinput";
	public static String CAPTURE_MAP_STATISTIC = "edu.indiana.d2i.lib.capturemapstatistic";
	public static String ALPHA_MAP_INOUT_TRADEOFF = "edu.indiana.d2i.lib.capturemapstatistic";
	
	public static String REDUCE_PROVENANCE_INFO_KEY = "edu.indiana.d21.lib.reduceprovenence.info";
	public static String TEXT_DELIMITOR = "\t";
	public static String LINE_BREAK = "\n";
	public static String REDUCE_PROVENANCE_STATUS_SUFFIX = "_status";
	public static String REDUCE_PROVENANCE_KEYS_SUFFIX = "_keys";
}
