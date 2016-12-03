package com.baiduwaimai.hadoop.sequenceFile;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.common.IOUtils;

public class SequenceFileWriteDemo {
	
	private static final String[] DATA ={
		"A","B","C","D","E"
	};
	public static void main(String[] args) throws IOException{
		String uri =  args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri),conf);
		//键的数据类型
		IntWritable key = new IntWritable();
		//值的数据类型
		Text value = new Text();
		//构建成path类型
		Path path = new Path(uri);
		//构建顺序文件的写对象方法
		SequenceFile.Writer writer = null;
		try{
			//顺序文件常见写数据流，里面需要当前的文件系统API，写入的路径，键对象类型，值对象类型
			//打开这个对象
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			//对这个对象进行具体的赋值
			for(int i=0;i<100;i++){
				//键每次写入序号
				key.set(100 - i);
				//值每次写入数组中所给内容
				value.set(DATA[i%DATA.length]);
				//添加到我们的对象中去
				writer.append(key, value);
			}
		}finally{
			//把这个写入文件的数据流给关闭了
			IOUtils.closeStream(writer);
		}
	}
}
