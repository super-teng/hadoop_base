package com.baiduwaimai.hadoop.io.compression;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class FileDecompressor {
	public static void main(String[] args) throws Exception{
		String uri = args[0];
		String targetUrl = args[1];
		Configuration conf = new Configuration();
		Path inputUrl = new Path(uri);
		//创建压缩文件工厂
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		//通过压缩工厂来获取当前的压缩类,通过后缀名来自动判断压缩格式
		CompressionCodec codec = factory.getCodec(inputUrl);
		//创建文件系统
		FileSystem fs = FileSystem.get(URI.create(uri),conf);
		InputStream in = null;
		OutputStream out = null;
		try{
			//对输入流进行解压缩
			in = codec.createInputStream(fs.open(inputUrl));
			//创建新的输出流
			out = fs.create(new Path(targetUrl));
			IOUtils.copyBytes(in, out, 4096,false);
		}finally{
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
	}
}
