package com.dataflair.tr.retail.sales.productcategory;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public final class ProductCategorySalesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
	public final void reduce(final Text key, final Iterable<DoubleWritable> values, final Context context) throws IOException, InterruptedException
	{
		//Input will be Men's Clothing--> 214.05,100.00,200.00,300.00
		double sum = 0.0;
		for (final DoubleWritable val : values)
		{
			sum += val.get();
		}
		context.write(key, new DoubleWritable(sum));	//(men's clothing,1000.00)
	}
}
