package org.apache.hive.hcatalog.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

public class HCatMultiInputFormat extends HCatBaseInputFormat {
	public static void setInput(Job job, ArrayList<InputJobInfo> inputJobInfoList)
		throws IOException {
		try {
			InitializeInput.setInput(job, inputJobInfoList);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
	
	private static HCatMultiSplit castToHMultiCatSplit(InputSplit split) throws IOException{
		if (split instanceof HCatMultiSplit) {
			return (HCatMultiSplit) split;
		} else {
			throw new IOException("Split must be " + HCatMultiSplit.class.getName()
					+ " but found " + split.getClass().getName());
		}
	}
	
	private static HCatSchema getTableSchema(Configuration conf, String tableName) throws IOException {
		List<InputJobInfo> list = getJobInfoList(conf);
		InputJobInfo inputJobInfo = null;
		for (InputJobInfo jobInfo : list) {
			if (tableName.equals(getTableFullName(jobInfo))) {
				inputJobInfo = jobInfo;
				break;
			}
		}
		if (inputJobInfo == null) {
			throw new IOException("no job information found for table " + tableName);
		}

		HCatSchema allCols = new HCatSchema(new LinkedList<HCatFieldSchema>());
		for (HCatFieldSchema field : inputJobInfo.getTableInfo().getDataColumns().getFields()) {
			allCols.append(field);
		}
		for (HCatFieldSchema field : inputJobInfo.getTableInfo().getPartitionColumns().getFields()) {
			allCols.append(field);
		}
		return allCols;
	}

	private static List<InputJobInfo> getJobInfoList(Configuration conf) throws IOException {
		String jobListString = conf.get(
				HCatConstants.HCAT_KEY_MULTI_INPUT_JOBS_INFO);
	    if (jobListString == null) {
	        throw new IOException("job information list not found in configuration."
	          + " HCatInputFormat.setInput() not called?");
	      }
	    
	    return (ArrayList<InputJobInfo>) HCatUtil.deserialize(jobListString);
	}
	
	private static String getTableFullName(InputJobInfo jobInfo) {
		return jobInfo.getDatabaseName() + "." + jobInfo.getTableName();
	}
	
	@Override
	public RecordReader<WritableComparable, HCatRecord> createRecordReader(InputSplit split,
			TaskAttemptContext taskContext) throws IOException, InterruptedException {
		
		HCatMultiSplit hcatSplit = castToHMultiCatSplit(split);
		
		PartInfo partitionInfo = hcatSplit.getPartitionInfo();
		JobContext jobContext = taskContext;
		Configuration conf = jobContext.getConfiguration();
		
		HiveStorageHandler storageHandler = HCatUtil.getStorageHandler(
				conf, partitionInfo);
		
		JobConf jobConf = HCatUtil.getJobConfFromContext(jobContext);
		Map<String, String> jobProperties = partitionInfo.getJobProperties();
		HCatUtil.copyJobPropertiesToJobConf(jobProperties, jobConf);
		
		Map<String, Object> valuesNotInDataCols = getColValsNotInDataColumns(
				getTableSchema(conf, hcatSplit.getTableName()),
				partitionInfo);
		
		return new HCatRecordReader(storageHandler, valuesNotInDataCols);
	}
	
	@Override
	public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
		Configuration conf = jobContext.getConfiguration();
		
		List<InputJobInfo> inputJobInfoList = getJobInfoList(conf);
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for (InputJobInfo inputJobInfo : inputJobInfoList) {
			List<InputSplit> oneTableSplits = getSplits(jobContext, inputJobInfo);
			String tableName = getTableFullName(inputJobInfo);
			for (InputSplit split : oneTableSplits) {
				HCatSplit hCatSplit = (HCatSplit) split;
				HCatMultiSplit multiSplit = new HCatMultiSplit(hCatSplit, tableName);
				splits.add(multiSplit);
			}
		}
		return splits;
	}
}
