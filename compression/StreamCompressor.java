package com.baiduwaimai.hadoop.io.compression;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

//读取某一文件信息进行压缩之后输出到控制台中去
public class StreamCompressor {
	public static void main(String[] args) throws Exception {
		// 获取参数中的压缩类形式，因为有众多的压缩类
		String codeClassName = args[0];
		//当前要压缩的文件路径
		String url = args[1];
		//输出的压缩路径
		String targeturl = args[2];
		// 通过反射字符串找到当前的具体执行类
		Class<?> codeClass = Class.forName(codeClassName);
		// 操作配置文件的包装类
		Configuration conf = new Configuration();
		// 通过压缩执行类与配置文件包装类反射工具创建出执行压缩的实体类
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codeClass, conf);
		//通过当前路径与配置文件获取到文件的操作流对象
		FileSystem fs = FileSystem.get(URI.create(url), conf);
		//创建普通的输入流
		InputStream in = null;
		//创建压缩输出流
		CompressionOutputStream out = null;
		try{
			//打开输入流
			in = fs.open(new Path(url));
			//对输出流进行压缩
			out = codec.createOutputStream(fs.create(new Path(targeturl)));
			//对其进行复制
			IOUtils.copyBytes(in, out, 4096,false);
			//压缩完成后并不关闭这个管道流
			out.finish();
		}finally{
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
	}
}
