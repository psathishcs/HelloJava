package org.hello.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 * @author Sathish Kumar Parthasarathy
 *
 * Sep 5, 2016, 8:21:01 PM
 */
public class HelloWorldCount {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("HelloWorldCount==============================> Start");
		System.out.println("Input Folder  : " + args[0]);
		System.out.println("Output Folder : " + args[1]);
		Configuration conf = new Configuration();
		Job job = new Job(conf, "HelloWorldCount");
		job.setJarByClass(HelloWorldCount.class);
		job.setMapperClass(HelloMapper.class);
		job.setCombinerClass(HelloReducer.class);
		job.setReducerClass(HelloReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		System.out.println("HelloWorldCount==============================> End");
		System.exit(result ? 0 : 1);
		
	}
	
}
