package com.wzy.itemcf;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step3 {
	
	private static final Text K = new Text();
	private static final IntWritable V = new IntWritable(1);
	
	public static boolean run (Configuration conf, Map<String, String> paths) {
		try {
			conf.set("fs.defaultFS", "hdfs://slave100:9000");
			conf.set("yarn.resourcemanager.hostname", "slave100");
//			conf.set("mapreduce.app-submission.cross-platform", "true");
//			conf.set("mapred.jar", "C:\\Users\\Mr.w\\Desktop\\tq.jar");
			
			FileSystem fs = FileSystem.get(conf);
			Job job = Job.getInstance(conf);
			
			job.setJobName("step3");
			job.setJarByClass(StartRun.class);
			job.setMapperClass(Step3_Mapper.class);
			job.setReducerClass(Step3_Reducer.class);
			job.setCombinerClass(Step3_Reducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(paths.get("Step3Input")));
			
			Path outpath = new Path(paths.get("Step3Output"));

			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}

			FileOutputFormat.setOutputPath(job, outpath);

			boolean flag = job.waitForCompletion(true);
			if (flag) {
				System.out.println("job success!");
			}
			return flag;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	static class Step3_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			String[] items = tokens[1].split(",");
			for (int i = 0; i < items.length; i++) {
				String itemA = items[i].split(":")[0];
				for (int j = 0; j < items.length; j++) {
					String itemB = items[j].split(":")[0];
					K.set(itemA + ":" + itemB);
					context.write(K, V);
				}
			}
		}
	}
	
	
	static class Step3_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text text, Iterable<IntWritable> iterable, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable i : iterable) {
				sum = sum + i.get();
			}
			V.set(sum);
			context.write(text, V);
		}
	}
}
