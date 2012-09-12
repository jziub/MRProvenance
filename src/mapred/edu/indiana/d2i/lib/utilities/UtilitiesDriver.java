package edu.indiana.d2i.lib.utilities;

import org.apache.hadoop.util.ProgramDriver;

public class UtilitiesDriver {	
	public static void main(String[] args) {
		int exitCode = -1;
	    ProgramDriver pgd = new ProgramDriver();
	    try {
	    	pgd.addClass("retrieveprov", RetrieveProvenance.class, "retrieve provenance");
	    	pgd.addClass("graphgen", GraphGenerator.class, "generate graph");
	    	pgd.addClass("viewseq", MapInOutDebugger.class, "view sequence file");
	    	pgd.addClass("graphdeg", GraphDegree.class, "graph degree");
	    	pgd.addClass("graphreverse", GraphReverse.class, "graph reverse");
	    	pgd.addClass("clue2outgraph", Clue2OutGraph.class, "clue graph");
	    	pgd.addClass("debugger", Debugger.class, "debugger");
	    	pgd.addClass("mapiodebug", MapInOutDebugger.class, "mapiodebug");
	    	pgd.driver(args);
	    	// Success
	        exitCode = 0;
	    } catch(Throwable e){
	        e.printStackTrace();
	    }
	    
	    System.exit(exitCode);
	}
}
