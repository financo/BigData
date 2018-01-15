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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step4 {
	public static boolean run (Configuration conf, Map<String, String> paths) {
		try {
//			conf.set("fs.defaultFS", "hdfs://slave101:8020");
//			conf.set("yarn.resourcemanager.hostname", "slave101");
			conf.set("mapreduce.app-submission.cross-platform", "true");
			conf.set("mapred.jar", "C:\\Users\\Mr.w\\Desktop\\tq.jar");
			
			FileSystem fs = FileSystem.get(conf);
			Job job = Job.getInstance(conf);
			
			job.setJobName("step4");
			job.setJarByClass(StartRun.class);
			job.setMapperClass(Step4_Mapper.class);
			job.setReducerClass(Step4_Reducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(job, 
					new Path[] { new Path(paths.get("Step4Input1")), new Path(paths.get("Step4Input2"))});
			
			Path outpath = new Path(paths.get("Step4Output"));

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
	
	static class Step4_Mapper extends Mapper<LongWritable, Text, Text, Text>{
		private String flag;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			flag = split.getPath().getParent().getName();
			System.out.println(flag + "**********************************");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = Pattern.compile("[\t,]").split(value.toString());
			
			if (flag.equals("step3")) {
				String[] v1 = tokens[0].split(":");
				String itemTD1 = v1[0];
				String itemTD2 = v1[1];
				String num = tokens[1];
				
				Text k = new Text(itemTD1);
				Text v = new Text("A:" + itemTD2 + "," + num);
				
				context.write(k, v);
			}else if (flag.equals("step2")) {
				String userID = tokens[0];
				for (int i = 1; i < tokens.length; i++) {
					String[] vector = tokens[1].split(":");
					String itemID = vector[0];
					String pref = vector[1];
					
					Text k = new Text(itemID);
					Text v = new Text("B:" + userID + "," + pref);
					context.write(k, v);
				}
			}
		}
	}
	
	
	static class Step4_Reducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text text, Iterable<Text> iterable, Context context) 
				throws IOException, InterruptedException {
			Map<String, Integer> mapA = new HashMap<String, Integer>();
			Map<String, Integer> mapB = new HashMap<String, Integer>();
			
			for (Text i : iterable) {
				String val = i.toString();
				if (val.startsWith("A:")) {
					String[] kv = Pattern.compile("[\t,]").split(val.substring(2));
					try {
						mapA.put(kv[0], Integer.parseInt(kv[1]));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}else if (val.startsWith("B:")) {
					String[] kv = Pattern.compile("[\t,]").split(val.substring(2));
					try {
						mapB.put(kv[0], Integer.parseInt(kv[1]));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			
			double result = 0;
			Iterator<String> iter = mapA.keySet().iterator();
			while (iter.hasNext()) {
				String mapk = iter.next();
				int num = mapA.get(mapk).intValue();
				Iterator<String> iterb = mapB.keySet().iterator();
				while (iterb.hasNext()) {
					String mapkb = iterb.next();
					int pref = mapB.get(mapkb).intValue();
					result = num * pref;
					
					Text k = new Text(mapkb);
					Text v = new Text(mapk + "," + result);
					context.write(k, v);
				}
			}
		}
	}
}
