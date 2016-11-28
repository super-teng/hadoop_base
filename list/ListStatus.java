package com.baiduwaimai.hadoop.hdfs.hdfs.list;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ListStatus {
	public static void main(String[] args) throws Exception{
		String uri = args[0];
		Configuration conf = new Configuration();
		//获取到当前的文件系统API
		FileSystem fs = FileSystem.get(URI.create(uri),conf);
		//得到当前所有要得到路径的地址存到包装类Path中
		Path[] path = new Path[args.length];
		for(int i =0;i<path.length;i++){
			path[i] = new Path(args[i]);
		}
		//通过文件流获得每个路径下的所有路径并保存到filestatus包装类中
		FileStatus[] list = fs.listStatus(path);
		//调取文件工具类把文件状态转化为路径
		Path[] p = FileUtil.stat2Paths(list);
		//输出
		for(Path pp: p){
			System.out.println(pp);
		}
	}
}
