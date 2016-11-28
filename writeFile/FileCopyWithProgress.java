package com.baiduwaimai.hadoop.hdfs.hdfs.writeFile;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileCopyWithProgress {
	public static void main(String[] args) throws Exception{
		String localSrc = args[0]; //源路径
		String targetSrc = args[1];//目标路径
		//对源文件创建文件输入流
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		//对目标文件创立hadoop文件系统
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(targetSrc),conf);
		OutputStream out =  null;
		try{
			//建立一个输出流，输出同时创建一个匿名类，重写progress方法表示执行过程中依次输出小数点作为标记
			out = fs.create(new Path(targetSrc),new Progressable(){
				public void progress(){
					System.out.print(".");
				}
			});
			//把输入中的数据输出到输出流当中
			IOUtils.copyBytes(in,out,4096,false);
		}finally{
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
	}
	
}
