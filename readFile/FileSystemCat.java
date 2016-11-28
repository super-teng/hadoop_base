package com.baiduwaimai.hadoop.hdfs.hdfs.readFile;
import java.io.InputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemCat {
	// 主类向上层抛出异常，防止外层不知道底层异常
	public static void main(String[] args) throws Exception {
		String uri = args[0]; // 获取打开文件的具体路径
		Configuration conf = new Configuration(); // 封装了客户端或服务器的配置
		// 通过目标文件的uri以及配置文件信息获得文件文件系统的API
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		InputStream in = null;
		try {
			in = fs.open(new Path(uri));// 通过文件系统打开读取流
			//将文件数据流中的数据输出到屏幕中去，缓存是4096，且输出完毕后不关闭数据流
			IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(in);
		}	
	}
}
