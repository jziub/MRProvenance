package edu.indiana.d2i.lib.input;

import org.apache.hadoop.conf.Configuration;

import edu.indiana.d2i.lib.ProvenanceConstants;

public class PartitionPlanProvenanceReader extends ProvenanceReader {
	public PartitionPlanProvenanceReader(Configuration conf, String jobID)
			throws Exception {
		super(conf, jobID);
	}

	@Override
	protected void initializeFilePatterns() {
		filePatterns.put(READERNAME.PARTITION_PLAN, conf.get(ProvenanceConstants.PARTITION_PLAN_LOCATION));
	}
}
