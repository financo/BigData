package com.wzy.itemcf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step2 {
	public static boolean run (Configuration conf, Map<String, String> paths) {
		try {
			conf.set("fs.defaultFS", "hdfs://slave100:9000");
			conf.set("yarn.resourcemanager.hostname", "slave100");
//			conf.set("mapreduce.app-submission.cross-platform", "true");
//			conf.set("mapred.jar", "C:\\Users\\Mr.w\\Desktop\\tq.jar");
			
			FileSystem fs = FileSystem.get(conf);
			Job job = Job.getInstance(conf);
			
			job.setJobName("step2");
			job.setJarByClass(StartRun.class);
			job.setMapperClass(Step2_Mapper.class);
			job.setReducerClass(Step2_Reducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(paths.get("Step2Input")));
			Path outpath = new Path(paths.get("Step2Output"));
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
	
	static class Step2_Mapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			String item = tokens[1];
			String user = tokens[0];
			String action = tokens[2];
			Text k = new Text(user);
//			Integer rv = StartRun.R.get(action);
			Integer rv =  Integer.parseInt(action);
			
			Text v = new Text(item + ":" + rv.intValue());
			context.write(k, v);
		}
	}
	
	
	static class Step2_Reducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text text, Iterable<Text> iterable, Context context) 
				throws IOException, InterruptedException {
			Map<String, Integer> r = new HashMap<String, Integer>();
			
			for (Text i : iterable) {
				String[] vs = i.toString().split(":");
				String item = vs[0];
				Integer action = Integer.parseInt(vs[1]);
				action = ((Integer) (r.get(item) == null ? 0 : r.get(item))).intValue() + action;
				r.put(item, action);
			}
			StringBuffer sb = new StringBuffer();
			for (Entry<String, Integer> entry : r.entrySet()) {
				sb.append(entry.getKey() + ":" + entry.getValue().intValue() + ",");
			}
			context.write(text, new Text(sb.toString()));
 		}
	}
}
