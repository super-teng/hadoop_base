package com.baiduwaimai.hadoop.MTjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Super腾 on 2016/12/10.
 */
/*
    对两个表进行联合处理
    第一个表                第二个表              联合处理为
    hagongda 1              1 haerbin           hagongda haerbin
    hagongcheng 1           2 beijing           hagongcheng haerbin
    linda 1                                     linda haerbin
    qinghua 2                                   qinghua beijing
    beida 2                                     beida beijing
 */
public class MTjoin extends Configured implements Tool {
    public static int time = 0;
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if(line.contains("factoryName")){
                return ;
            }
            //判断当前元素是属于左表还是右表
            //说明当前是右表
            if (line.charAt(0) >= '0' && line.charAt(0) <= '9') {
                //将地址ID作为键，后面作为值并加上标记2
                context.write(new Text(String.valueOf(line.charAt(0))), new Text("2" + line.substring(2, line.length())));
            } else {
                //说明当前是左表
                context.write(new Text(String.valueOf(line.charAt(line.length() - 1))), new Text("1" + line.substring(0, line.length() - 2)));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if(time == 0){
                context.write(new Text("factoryName"),new Text("addressName"));
                time++;
            }
            List<String> factoryName = new ArrayList<String>();
            List<String> addressName = new ArrayList<String>();
            //轮询当前属于同一地址ID的内容，对其通过第一个标记进行分组
            for (Text val : values) {
                String temp = val.toString();
                //说明是工厂名左表中的元素
                if (temp.charAt(0) == '1') {
                    factoryName.add(temp.substring(1, temp.length()));
                } else {
                    //说明是地址名是右表中的元素
                    addressName.add(temp.substring(1, temp.length()));
                }
            }
            //将二者的笛卡尔积进行输出
            for (int i = 0; i < factoryName.size(); i++) {
                for (int j = 0; j < addressName.size(); j++) {
                    context.write(new Text(factoryName.get(i)), new Text(addressName.get(j)));
                }
            }
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("args was wrong");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = new Job(conf, "MTjoin");
        job.setJarByClass(MTjoin.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MTjoin(),args);
        System.exit(exitCode);
    }
}
