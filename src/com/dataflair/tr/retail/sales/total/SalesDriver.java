package com.dataflair.tr.retail.sales.total;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Find the total sales value across all the stores, and the total number of sales.
 * 4138476       1034457953.26 
 */


public final class SalesDriver
{
	public final static void main(final String[] args) throws Exception
	{
		final Configuration conf = new Configuration();

		final Job job = new Job(conf, "P1Q3");
		job.setJarByClass(SalesDriver.class);
		job.setMapperClass(SalesMapper.class);
		job.setReducerClass(SalesReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}