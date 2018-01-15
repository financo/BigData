package com.wzy.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
 * 排序
 * 输入数据格式：每行一个整数
 */
public class Sort {
	
	public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {
		private static IntWritable data = new IntWritable();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			data.set(Integer.parseInt(line));
			context.write(data, new IntWritable(1));
		}
	}

	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			for (IntWritable v : values) {
				context.write(key, new Text(""));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

//		conf.set("fs.defaultFS", "hdfs://slave101:8020");
//		conf.set("yarn.resourcemanager.hostname", "slave101");
//		conf.set("mapred.job.tracker", "192.168.1.100:9000");
		conf.set("mapred.jar", "C:\\Users\\Mr.w\\Desktop\\tq.jar");
		conf.set("mapreduce.app-submission.cross-platform", "true");
		
//		if (args.length != 2) {
//			System.err.println("Usage: Data Sort <in> <out>");
//			System.exit(2);
//		}
//		System.out.println(args[0]);
//		System.out.println(args[1]);

		Job job = Job.getInstance(conf, "Data Sort");
		job.setJarByClass(Sort.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path("/user/sort/input"));
		FileOutputFormat.setOutputPath(job, new Path("/user/sort/output"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
