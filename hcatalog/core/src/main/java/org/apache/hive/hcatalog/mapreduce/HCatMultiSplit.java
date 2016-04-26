package org.apache.hive.hcatalog.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

public class HCatMultiSplit extends HCatSplit {

	/** The table name of HCatTable, format: [db name].[table name] */
	private String tableName;

	public HCatMultiSplit() {
	}

	public HCatMultiSplit(HCatSplit split, String tableName) {
		super(split.getPartitionInfo(), split.getBaseSplit());
		this.tableName = tableName;
	}

	public String getTableName() {
		return tableName;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		tableName = WritableUtils.readString(input);
		super.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		WritableUtils.writeString(output, tableName);
		super.write(output);
	}
}
