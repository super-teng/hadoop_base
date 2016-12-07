package com.baiduwaimai.hadoop.deduplication;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Dedup {
	public static class Map extends Mapper <Object,Text,Text,Text>{
		public void map(Object obj,Text text,Context context) throws IOException, InterruptedException{
			context.write(text, new Text(""));
		}
	}
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			context.write(key, new Text(""));
		}
	}
	public static void main(String[]args) throws IOException, ClassNotFoundException, InterruptedException{
		//第一步获取配置文件信息，检测参数的正确性，创建JOB工作，指定JAR包，指定map和reduece类
		//指定输出文件格式，指定文件读取的具体位置
		//等待执行退出
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.out.println("args was wrong");
			System.exit(2);
		}
		Job job = new Job(conf,"Deduplication");
		job.setJarByClass(Dedup.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
