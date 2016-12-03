package com.baiduwaimai.hadoop.sequenceFile;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileReader {
	public static void main(String[] args) throws Exception{
		//要读取的文件路径
		String uri = args[0];
		//获取到当前的配置文件信息
		Configuration conf = new Configuration();
		//获取到当前的文件流信息
		FileSystem fs = FileSystem.get(URI.create(uri),conf);
		SequenceFile.Reader reader = null;
		Path url = new Path(uri);
		try{
			//创建顺序读取流对象
			reader = new SequenceFile.Reader(fs,url,conf);
			//通过配置文件和键的类创建文件的键对象
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			//获取值对象类
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			//如果当前的reader中还有值和键的组合的话
			while(reader.next(key, value)){
				//输出当前的值和键
				System.out.println(key+" "+value);
			}
		}finally{
			IOUtils.closeStream(reader);
		}
	}
}
