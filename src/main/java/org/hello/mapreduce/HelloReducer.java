package org.hello.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * 
 * @author Sathish Kumar Parthasarathy
 *
 * Sep 5, 2016 8:24:02 PM
 *
 */
public class HelloReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
	private IntWritable result = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	{
		System.out.println("Inside Reducer ==========================> Start");
				int sum = 0;
		for (IntWritable value : values){
			sum +=  value.get();
		}
		result.set(sum);
		System.out.println("Inside Reducer ==========================> End");
		context.write(key, result);
	}
}
