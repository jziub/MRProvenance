package edu.indiana.d2i;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.indiana.d2i.io.MapOutputValueWrapper;
import edu.indiana.d2i.util.ProvenanceController;
import edu.indiana.d2i.util.ProvenanceWritter;

public class ReducerWrapper<INKEY, INVALUE, OUTKEY, OUTVALUE> extends
		Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> {
	private final Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer;
	private ProvenanceWritter provenanceWritter;
	private static final Log LOG = LogFactory.getLog(ReducerWrapper.class
			.getName());

	public ReducerWrapper(Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE> reducer,
			TaskAttemptContext taskContext) {
		this.reducer = reducer;
		provenanceWritter = new ProvenanceWritter(taskContext,
				Provenance.TYPE.MAP_PROVENANCE);
		// LOG.info("ReducerWrapper construct");
	}

	/**
	 * Advanced application writers can use the
	 * {@link #run(org.apache.hadoop.mapreduce.Reducer.Context)} method to
	 * control how the reduce task works.
	 */
	@SuppressWarnings("unchecked")
	public void run(Context context) throws IOException, InterruptedException {
		// reducer.run(context);

		LOG.info("updated ReducerWrapper run");

//		reducer.setup(context);
		while (context.nextKey()) {
			// extract the real values from the wrapper
			Iterable<MapOutputValueWrapper> valuesWrapped = (Iterable<MapOutputValueWrapper>) context
					.getValues();
			List<INVALUE> values = new ArrayList<INVALUE>();
			java.util.Set<String> inputURIs = new HashSet<String>();
			for (MapOutputValueWrapper wrapped : valuesWrapped) {
				String inputURI = wrapped.getInputURI();
				List<String> fileidList = ProvenanceController.ProvenanceFormatter
						.convertString2FileIDList(wrapped.getInputURI());
//				inputURIs.add(inputURI);
				inputURIs.addAll(fileidList);
				values.add((INVALUE) wrapped.get());
			}

			/*** capture map provenance <key, inputURI-1, ..., inputURI-k> ***/
			Provenance provenance = new Provenance(
					Provenance.TYPE.MAP_PROVENANCE, context.getCurrentKey()
							.toString(), inputURIs);
			provenanceWritter.write(provenance);

			// feed the reduce function provided by users
			reducer.reduce(context.getCurrentKey(), values, context);
		}
		reducer.cleanup(context);

		LOG.info("ReducerWrapper finish");
	}
}
