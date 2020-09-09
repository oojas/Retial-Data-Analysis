package com.dataflair.tr.retail.sales.store;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public final class StoreSalesMapper extends Mapper<LongWritable, Text, Text, Text>
{
	public final void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
//		"2012-01-01      09:00   San Jose        Men's Clothing  214.05  Amex"
		
		final String[] data = line.trim().split("\t"); 
		//data[0] = 2012-01-01 
		//data[1] = 09:00
		//data[2] = San Jose
		//data[3] = Men's Clothing
		//data[4] = 214.05
		//data[5] = Amex
		if (data.length == 6)
		{
			String store = data[2];	//San Jose
//			final double sales = Double.parseDouble(data[4]); 	//214.05
			String sales = data[4];
			context.write(new Text(store), new Text(sales));
//			(San Jose,214.05) output to reducer
		}
		else
		{
			context.write(new Text("bad-record-key"), value);
		}
	}
}