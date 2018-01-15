package com.wzy.itemcf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

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


public class Step5 {
	private static final Text K = new Text();
	private static final Text V = new Text();
	
	public static boolean run (Configuration conf, Map<String, String> paths) {
		try {
//			conf.set("fs.defaultFS", "hdfs://slave101:8020");
//			conf.set("yarn.resourcemanager.hostname", "slave101");
			conf.set("mapreduce.app-submission.cross-platform", "true");
			conf.set("mapred.jar", "C:\\Users\\Mr.w\\Desktop\\tq.jar");
			
			FileSystem fs = FileSystem.get(conf);
			Job job = Job.getInstance(conf);
			
			job.setJobName("step5");
			job.setJarByClass(StartRun.class);
			job.setMapperClass(Step5_Mapper.class);
			job.setReducerClass(Step5_Reducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(paths.get("Step5Input")));
			
			Path outpath = new Path(paths.get("Step5Output"));

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
	
	static class Step5_Mapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = Pattern.compile("[\t,]").split(value.toString());
			Text k = new Text(tokens[0]);
			Text v = new Text(tokens[1] + "," + tokens[2]);
			context.write(k, v);
		}
	}
	
	
	static class Step5_Reducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text text, Iterable<Text> iterable, Context context) 
				throws IOException, InterruptedException {
			Map<String, Double> map = new HashMap<String, Double>();
			for (Text line : iterable) {
				String[] tokens = line.toString().split(",");
				String itemID = tokens[0];
				Double score = Double.parseDouble(tokens[1]);
				if (map.containsKey(itemID)) {
					map.put(itemID, map.get(itemID) + score);
				}else {
					map.put(itemID, score);
				}
			}
			
			Iterator<String> iter = map.keySet().iterator();
			while (iter.hasNext()) {
				String itenID = (String) iter.next();
				double score = map.get(itenID);
				Text v = new Text(itenID + "," + score);
				context.write(text, v);
			}
		}
	}
}
