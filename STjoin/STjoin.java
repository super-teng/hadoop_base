package com.baiduwaimai.hadoop.STjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class STjoin extends Configured implements Tool {
	// 用于输出第一行标记的
	public static int time = 0;

	public static class Map extends Mapper<Object, Text, Text, Text> {
		// map 方法对其内容进行自表连接
		public void map(Object obj, Text text, Context context) throws IOException, InterruptedException {
			String[] data = text.toString().split(" ");
			// 如果当前行不是第一行 child parent这行的话进行操作
			if (data[0].compareTo("child") != 0) {
				// 这样两个键值均为中间值父亲那块，“1”标识表示为孙子
				// “2”标识表示为爷爷
				context.write(new Text(data[1]), new Text("1" + data[0]));
				context.write(new Text(data[0]), new Text("2" + data[1]));
			}
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			// 第一行输出标记
			if (time == 0) {
				context.write(new Text("grandChild"), new Text("grandParent"));
				time++;
			}
			List<Text> grandChild = new ArrayList<Text>();
			List<Text> grandParent = new ArrayList<Text>();
			for (Text val : value) {
				String oper = val.toString();
				// 根据表示分为添加到我们的两个孙子和爷爷的集合中去
				if (oper.charAt(0) == '1') {
					grandChild.add(new Text(oper.substring(1,oper.length())));
				} else if (oper.charAt(0) == '2') {
					grandParent.add(new Text(oper.substring(1,oper.length())));
				}
			}
			for (int i = 0; i < grandChild.size(); i++) {
				for (int j = 0; j < grandParent.size(); j++) {
					context.write(grandChild.get(i), grandParent.get(j));
				}
			}
		}
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("args error");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		Job job = new Job(conf, "STjoin");
		job.setJarByClass(STjoin.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCodec = ToolRunner.run(new STjoin(), args);
		System.exit(exitCodec);
	}
}
