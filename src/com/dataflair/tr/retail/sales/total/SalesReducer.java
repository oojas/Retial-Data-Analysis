package com.dataflair.tr.retail.sales.total;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesReducer extends Reducer<Text, DoubleWritable, IntWritable, Text>
{
	public final void reduce(final Text key, final Iterable<DoubleWritable> values, final Context context) throws IOException, InterruptedException
	{
		double sum = 0.0;
		int count = 0;
//		Key => ConstantKey  Value => 214.05,100.00,200.00,300.00.....
		for (final DoubleWritable val : values)
		{
			count++;
			sum += val.get();
		}
		DecimalFormat df = new DecimalFormat("#.##");			//so that output will not come in 16.3E34 format
		context.write(new IntWritable(count), new Text(df.format(sum)));
//		(Total-Transaction, Total-Sales)
	}
}
