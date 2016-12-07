package com.baiduwaimai.hadoop.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Sort extends Configured implements Tool{
	public static class Map extends Mapper<Object,Text,IntWritable,IntWritable>{
		//将当中的值直接写出
		public void map(Object obj,Text val,Context context) throws IOException, InterruptedException{
			context.write(new IntWritable(Integer.parseInt(val.toString())), new IntWritable(1));
		}
	}
	
	public static class Reduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		//当前的行标号
		private static IntWritable lineNumber = new IntWritable(1);
		
		public void reduce(IntWritable key,Iterable<IntWritable> val,Context context) throws IOException, InterruptedException{
			for(IntWritable i : val){
				//把当前元素写出
				context.write(lineNumber, key);
				//行号进行++操作
				lineNumber.set(lineNumber.get()+1);
			}
		}
		
	}
	
	//分割的类，当map函数存储到本地硬盘的时候通过partition函数对其进行分区处理，每个区域传给一个reduce处理
	//这样做完，reduce中编号由小到大中存储的也是组间有序的状态
	public static class Partition extends Partitioner<IntWritable,IntWritable>{

		//参数是map输出的结果对和reduce的数量，输出则是reduce的编号。
		@Override
		public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
			//当前传入元素中最大的元素
			int MaxNumber = 65223;
			//保证最大的元素也可以在这个范围中去
			int bounds = MaxNumber / numPartitions + 1 ;
			//当前传入的元素值
			int keyNumber = key.get();
			//看当前传入的元素值在哪个reduce中
			for(int i = 0;i < numPartitions;i++){
				//找出当前元素所在的分区范围
				if(keyNumber < bounds* (i+1)  && keyNumber >= bounds*i){
					return i;
				}
			}
			return -1;
		}
		
	}
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if(args.length != 2){
			System.out.println("args was wrong");
			System.exit(2);
		}
		Job job = new Job(conf,"sort");
		job.setJarByClass(Sort.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(Partition.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCodec = ToolRunner.run(new Sort(), args);
		System.exit(exitCodec);
	}

	
	
}
