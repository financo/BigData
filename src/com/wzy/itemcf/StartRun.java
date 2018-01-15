package com.wzy.itemcf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
//数据模板
//user_id,item_id,behavior_type,user_geohash,item_category,time
//98047837,232431562,1,,4245,2014-12-06 02
//97726136,383583590,1,,5894,2014-12-09 20
//98607707,64749712,1,,2883,2014-12-18 11
//98662432,320593836,1,96nn52n,6562,2014-12-06 10

public class StartRun {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://slave100:9000");
//		conf.set("yarn.resourcemanager.hostname", "slave100");
//		conf.set("mapreduce.app-submission.cross-platform", "true");
//		conf.set("mapred.jar", "C:\\Users\\Mr.w\\Desktop\\wc.jar");
		
		Map<String, String> paths = new HashMap<String, String>();
		paths.put("Step1Input", "/user/itemcf/input/tianchi_mobile_recommend_train_user.csv");
		paths.put("Step1Output", "/user/itemcf/output/step1");
		
		paths.put("Step2Input", paths.get("Step1Output"));
		paths.put("Step2Output", "/user/itemcf/output/step2");
		
		paths.put("Step3Input", paths.get("Step2Output"));
		paths.put("Step3Output", "/user/itemcf/output/step3");
		
		paths.put("Step4Input1", paths.get("Step2Output"));
		paths.put("Step4Input2", paths.get("Step3Output"));
		paths.put("Step4Output", "/user/itemcf/output/step4");
		
		paths.put("Step5Input", paths.get("Step4Output"));
		paths.put("Step5Output", "/user/itemcf/output/step5");
		
		paths.put("Step6Input", paths.get("Step5Output"));
		paths.put("Step6Output", "/user/itemcf/output/step6");
		
//		Step1.run(conf, paths);
//		Step2.run(conf, paths);
		Step3.run(conf, paths);
//		Step4.run(conf, paths);
//		Step5.run(conf, paths);
//		Step6.run(conf, paths);
	}
	
	public static Map<String, Integer> R = new HashMap<String, Integer>();
	
	static {
		R.put("click", 1);
		R.put("collect", 2);
		R.put("cart", 3);
		R.put("alipay", 4);
	}
}
