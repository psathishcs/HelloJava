package org.hello.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 
 * @author Sathish Kumar Parthasarathy
 *
 * Sep 5, 2016 9:10:14 PM
 *
 */
public class HelloMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	private Text word = new Text();
	private final IntWritable one = new IntWritable(1);
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		System.out.println("Inside Mapper ==========================> Start");
		StringTokenizer itr = new StringTokenizer(value.toString());
		
		while (itr.hasMoreTokens()){
			word.set(itr.nextToken());
			context.write(word, one);
		}
		System.out.println("Inside Mapper ==========================> End");
	}

}
