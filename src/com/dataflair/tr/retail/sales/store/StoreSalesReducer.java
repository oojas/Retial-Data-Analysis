package com.dataflair.tr.retail.sales.store;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;

public final class StoreSalesReducer extends Reducer<Text, Text, Text, Text>
{
	MultipleOutputs<Text, Text> mos = null;

	public void setup(Context context)
    {
         mos = new MultipleOutputs <Text, Text>(context);
    }
	
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
        mos.close();
	}

	public void reduce(Text key, Iterable<Text> values, final Context context) throws IOException, InterruptedException
	{
		double totalSale = 0.0;

		if (key.toString().equals("bad-record-key"))
		{
//			Input will be (key => bad-record-key, value => input bad record)
			for (Text val: values)
				mos.write("BadRecords", val, new Text());
		}
		else
		{
//			Input will be (key => San Jose and value => 214.05, 100.00, 200.00, 300.00....)
			for (Text val : values)
			{
				String valString = val.toString();
				totalSale += Double.parseDouble(valString);
			}
			mos.write("ParsedRecords", key, new Text(""+totalSale));
		}
	}
}