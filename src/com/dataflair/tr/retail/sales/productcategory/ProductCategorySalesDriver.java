package com.dataflair.tr.retail.sales.productcategory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Sales breakdown by product category across all the stores, (all the cities).
 */

public final class ProductCategorySalesDriver
{
	public final static void main(final String[] args) throws Exception
	{
		final Configuration conf = new Configuration();

		final Job job = new Job(conf, "Product Category MR");
		job.setJarByClass(ProductCategorySalesDriver.class);
		job.setMapperClass(ProductCategorySalesMapper.class);
		job.setReducerClass(ProductCategorySalesReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}