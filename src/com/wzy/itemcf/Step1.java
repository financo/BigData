package com.wzy.itemcf;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step1 {
	public static boolean run (Configuration conf, Map<String, String> paths) {
		try {
			conf.set("fs.defaultFS", "hdfs://slave100:9000");
			conf.set("yarn.resourcemanager.hostname", "slave100");
//			conf.set("mapreduce.app-submission.cross-platform", "true");
//			conf.set("mapred.jar", "C:\\Users\\Mr.w\\Desktop\\itemcf.jar");
			
			FileSystem fs = FileSystem.get(conf);
			Job job = Job.getInstance(conf);
			
			job.setJobName("step1");
			job.setJarByClass(StartRun.class);
			job.setMapperClass(Step1_Mapper.class);
			job.setReducerClass(Step1_Reducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(paths.get("Step1Input")));
			Path outpath = new Path(paths.get("Step1Output"));
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);

			boolean flag = job.waitForCompletion(true);
			return flag;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	static class Step1_Mapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			if (key.get() != 0) {
				context.write(value, NullWritable.get());
			}
		}
	}
	
	
	static class Step1_Reducer extends Reducer<Text, IntWritable, Text, NullWritable>{
		@Override
		protected void reduce(Text text, Iterable<IntWritable> iterable, Context context) 
				throws IOException, InterruptedException {
			context.write(text, NullWritable.get());
		}
	}
	
}
