package com.dataflair.tr.retail.sales.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Sales breakdown by store
 * Handle bad records (use multipleOutputs APIs)
 */

public final class StoreSalesDriver
{
	public final static void main(final String[] args) throws Exception
	{
		final Configuration conf = new Configuration();

		final Job job = new Job(conf, "Store Sales Analysis");
		job.setJarByClass(StoreSalesDriver.class);
		job.setMapperClass(StoreSalesMapper.class);
		job.setReducerClass(StoreSalesReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		MultipleOutputs.addNamedOutput(job, "ParsedRecords", TextOutputFormat.class , Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "BadRecords", TextOutputFormat.class , Text.class, Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}