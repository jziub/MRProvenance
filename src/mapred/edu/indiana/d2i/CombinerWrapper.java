package edu.indiana.d2i;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;

import edu.indiana.d2i.io.MapOutputValueWrapper;

public class CombinerWrapper <INKEY, INVALUE, OUTKEY, OUTVALUE> extends
		Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> {
	private final Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer;
	private static final Log LOG = LogFactory.getLog(CombinerWrapper.class.getName());
	
	public CombinerWrapper(Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer) {
		this.reducer = reducer;
	}
	
	/**
	 * Advanced application writers can use the
	 * {@link #run(org.apache.hadoop.mapreduce.Reducer.Context)} method to
	 * control how the reduce task works.
	 */
	@SuppressWarnings("unchecked")
	public void run(Context context) throws IOException, InterruptedException {
//		reducer.run(context);
		
		LOG.info("CombinerWrapper run");

//		reducer.setup(context);
		while (context.nextKey()) {
			// extract the real values from the wrapper
			Iterable<MapOutputValueWrapper> valuesWrapped = 
				(Iterable<MapOutputValueWrapper>) context.getValues();
			List<INVALUE> values = new ArrayList<INVALUE>();
			for (MapOutputValueWrapper wrapped : valuesWrapped) {
				values.add((INVALUE)wrapped.get());
			}
			
			/*** don't capture map provenance in combiner ***/
			
			// feed the reduce function provided by users
			reducer.reduce(context.getCurrentKey(), values, context);
		}
		reducer.cleanup(context);
		
		LOG.info("CombinerWrapper finish");
	}
}
