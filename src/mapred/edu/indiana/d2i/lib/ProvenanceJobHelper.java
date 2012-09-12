package edu.indiana.d2i.lib;

import java.net.URI;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import edu.indiana.d2i.lib.collector.ProvenanceCollector;
import edu.indiana.d2i.lib.collector.SampleMapProvenanceCollector;
import edu.indiana.d2i.lib.collector.SampleReduceProvenanceCollector;
import edu.indiana.d2i.lib.io.ArrayOfListIntWritable;
import edu.indiana.d2i.lib.io.ProvenanceMapOutputValue;
import edu.indiana.d2i.lib.io.ProvenanceWriterFactory;
import edu.indiana.d2i.lib.planner.GreedyBinPackPlanner;
import edu.indiana.d2i.lib.planner.Planner;
import edu.indiana.d2i.lib.planner.PartitionPlanDriver;

@SuppressWarnings("deprecation")
public class ProvenanceJobHelper {
	private Job job = null;
	private String lastJobID = null;
	private static final Log LOG = LogFactory.getLog(JobClient.class);
	private PartitionPlanDriver.PlanType planType = PartitionPlanDriver.PlanType.NOPLAN;

	private void internalSetting() throws Exception {
		/* approach 1 */
		Configuration conf = job.getConfiguration();

		boolean connection = conf.getBoolean(
				ProvenanceConstants.CAPTURE_MAP_REDUCE_CONNECTION, false);
		boolean reducestatistic = conf.getBoolean(
				ProvenanceConstants.CAPTURE_REDUCE_STATISTIC, false);
		boolean mapstatistic = conf.getBoolean(
				ProvenanceConstants.CAPTURE_MAP_STATISTIC, false);
		boolean mapoutput = conf.getBoolean(
				ProvenanceConstants.CAPTURE_MAP_OUTPUTKEY, false);
		boolean mapinput = conf.getBoolean(
				ProvenanceConstants.CAPTURE_MAP_INPUTKEY, false);

		// change to be more accurate
		if (connection || reducestatistic || mapstatistic || mapoutput
				|| mapinput) {
			LOG.info("!!! use wrapper");

			// replace MapOutput class
			if (conf.getBoolean(
					ProvenanceConstants.CAPTURE_MAP_REDUCE_CONNECTION, false)) {
				conf.setClass(ProvenanceConstants.USER_MAP_OUTPUT,
						job.getMapOutputValueClass(), Writable.class);
				job.setMapOutputValueClass(ProvenanceMapOutputValue.class);
			}

			// set user mapper
			conf.setClass(ProvenanceConstants.USER_MAPPER,
					job.getMapperClass(), Mapper.class);

			// set new mapper
			job.setMapperClass(ProvenanceMapper.class);

			// set user reducer
			conf.setClass(ProvenanceConstants.USER_REDUCER,
					job.getReducerClass(), Reducer.class);

			// set new reducer
			job.setReducerClass(ProvenanceReducer.class);

			// set user combiner
			if (job.getCombinerClass() != null) {
				conf.setClass(ProvenanceConstants.USER_COMBINER,
						job.getCombinerClass(), Reducer.class);
				job.setCombinerClass(ProvenanceCombiner.class);
			}

			// set provenance collector
			conf.setClass(ProvenanceConstants.MAP_PROVENANCE_COLLECTOR,
					SampleMapProvenanceCollector.class,
					ProvenanceCollector.class);
			conf.setClass(ProvenanceConstants.REDUCE_PROVENANCE_COLLECTOR,
					SampleReduceProvenanceCollector.class,
					ProvenanceCollector.class);

			// set provenance directory
			if (conf.get(ProvenanceConstants.PROVENANCE_STORE_DIR) == null) {
				conf.set(ProvenanceConstants.PROVENANCE_STORE_DIR,
						ProvenanceConstants.PROVENANCE_STORE_DIR_DEFAULTNAME);
			}
		}

		// set partition plan
		if (lastJobID != null) {
			// job.setPartitionerClass(ProvenancePartitioner.class);
			// conf.set(ProvenanceConstants.PARTITION_PLAN_LOCATION,
			// conf.get(ProvenanceConstants.PROVENANCE_STORE_DIR,
			// ProvenanceConstants.PROVENANCE_STORE_DIR_DEFAULTNAME) + lastJobID
			// + "/" + "partition_plan");
			String filename = conf.get(
					ProvenanceConstants.PROVENANCE_STORE_DIR,
					ProvenanceConstants.PROVENANCE_STORE_DIR_DEFAULTNAME)
					+ lastJobID + "/" + "partition_plan";
			DistributedCache.addCacheFile(new URI(filename), conf);
		}
	}

	public ProvenanceJobHelper(Job job) throws ClassNotFoundException {
		this.job = job;
	}

	public ProvenanceJobHelper setProvenanceDir(String rootDir) {
		// validate dir format???
		job.getConfiguration().set(ProvenanceConstants.PROVENANCE_STORE_DIR,
				rootDir);
		return this;
	}

	public ProvenanceJobHelper setProvenanceBasedPartitioner(String lastJobID) {
		this.lastJobID = lastJobID;
		return this;
	}

	/*
	 * alpha [0, 1] -- 0 collect no map input, 1 collect no map output, other
	 * value means no plan is generated
	 */
	public ProvenanceJobHelper planPartition(float alpha) {
		if (alpha > 1.0 || alpha < 0.0)
			return this;

		job.getConfiguration().setFloat(
				ProvenanceConstants.ALPHA_MAP_INOUT_TRADEOFF, alpha);
		if (alpha == 0.0) {
			captureMapperOutputKey(true);
			planType = PartitionPlanDriver.PlanType.MAPOUT;
		} else if (alpha == 1.0) {
			captureMapperInputKey(true);
			planType = PartitionPlanDriver.PlanType.MAPIN;
		} else {
			captureMapperInputKey(true);
			captureMapperOutputKey(true);
			planType = PartitionPlanDriver.PlanType.MAPINOUT;
		}
		return this;
	}

	public ProvenanceJobHelper captureMapReduceConnection(boolean flag) {
		job.getConfiguration().setBoolean(
				ProvenanceConstants.CAPTURE_MAP_REDUCE_CONNECTION, flag);
		return this;
	}

	public ProvenanceJobHelper captureReducerStatistic(boolean flag) {
		job.getConfiguration().setBoolean(
				ProvenanceConstants.CAPTURE_REDUCE_STATISTIC, flag);
		return this;
	}

	public ProvenanceJobHelper captureMapperOutputKey(boolean flag) {
		job.getConfiguration().setBoolean(
				ProvenanceConstants.CAPTURE_MAP_OUTPUTKEY, flag);
		return this;
	}

	public ProvenanceJobHelper captureMapperInputKey(boolean flag) {
		job.getConfiguration().setBoolean(
				ProvenanceConstants.CAPTURE_MAP_INPUTKEY, flag);
		return this;
	}

	public ProvenanceJobHelper captureMapperStatistic(boolean flag) {
		job.getConfiguration().setBoolean(
				ProvenanceConstants.CAPTURE_MAP_STATISTIC, flag);
		return this;
	}

	public boolean submitJob(boolean verbose) throws Exception {
		internalSetting();
		return job.waitForCompletion(verbose);
	}

	/** */
	public String submitJobAndReturnJobID(boolean verbose) throws Exception {
		internalSetting();

		Configuration conf = job.getConfiguration();
		String oldMapperClass = "mapred.mapper.class";
		String oldReduceClass = "mapred.reducer.class";
		conf.setBooleanIfUnset("mapred.mapper.new-api",
				conf.get(oldMapperClass) == null);
		conf.setBooleanIfUnset("mapred.reducer.new-api",
				conf.get(oldReduceClass) == null);

		JobClient jobClient = new JobClient((JobConf) conf);
		RunningJob runningJob = jobClient.submitJobInternal(new JobConf(conf));
		LOG.info("Running job: " + runningJob.getID());
		String lastReport = "";
		while (!runningJob.isComplete()) {
			Thread.sleep(1000);
			String report = (" map "
					+ StringUtils.formatPercent(runningJob.mapProgress(), 0)
					+ " reduce " + StringUtils.formatPercent(
					runningJob.reduceProgress(), 0));
			if (!report.equals(lastReport)) {
				LOG.info(report);
				lastReport = report;
			}
		}

		// use a thread, synchronization issue!!!!!!
		if (planType != PartitionPlanDriver.PlanType.NOPLAN) {
			if (runningJob.mapProgress() >= 1.0) {
				Planner planner = new GreedyBinPackPlanner();
				PartitionPlanDriver plan = new PartitionPlanDriver(conf,
						runningJob.getID().toString(), job.getNumReduceTasks(),
						planner, planType);
				List<IntWritable>[] partitionPlan = plan.getPartitionPlan();
				ArrayOfListIntWritable array = new ArrayOfListIntWritable(
						partitionPlan);

				String filename = conf
						.get(ProvenanceConstants.PROVENANCE_STORE_DIR)
						+ runningJob.getID() + "/partition_plan";
				Writer writer = ProvenanceWriterFactory.createWriter(conf,
						filename, IntWritable.class,
						ArrayOfListIntWritable.class);
				writer.append(new IntWritable(0), array);
				writer.close();
			}
		}

		return runningJob.getID().toString();
	}
}
