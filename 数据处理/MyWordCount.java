package com.huike;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
   * 统计文本中单词出现的次数,以文件形式进行输出
 */
public class MyWordCount {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		//1.设置conf对象,配置hdfs路径
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.101.135:9000");
		
		//2.设置job对象 在job对象中指定 jar map reduce的输出类型   输入格式化方式 输出的路径
		Job job = Job.getInstance(conf,"wordcount");  //设置job对象
		job.setJarByClass(MyWordCount.class);   //指定 jar
		job.setMapperClass(MyMapper.class);  //指定mapper
		job.setReducerClass(MyReducer.class); //指定Reducer
		
		job.setOutputKeyClass(Text.class); //指定Mapper的输出类型中key的类型
		job.setOutputValueClass(IntWritable.class); //指定Mapper的输出类型中value的类型
		
		job.setInputFormatClass(TextInputFormat.class);  //指定输入的格式化方式
		
		//将要分析的原始数据读入进来
		FileInputFormat.addInputPath(job, new Path("/pro/mywordcount/input/upload.txt")); //输入路径
		Path outPath = new Path("/pro/mywordcount/output/");  //新建输出路径
		FileSystem.get(conf).delete(outPath, true);  //如果路径存在,则删除原有路径
		FileOutputFormat.setOutputPath(job, outPath);  //指定输出路径
		
		System.exit(job.waitForCompletion(true)?0:1);  //如果程序完成,系统正常退出
		
	}
	
	
	/**
	 * 新建一个Mapper类
	 */
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String[] words = value.toString().strip(str, "[]").strip(str, "\'").split("\\,s+");  //将每一行的文本内容删除[]'三个字符，并转换成字符串并按照空格分割
			
			IntWritable one = new IntWritable(1);
			Text text = new Text();
			
			for (String word : words) {
				text.set(word);
				context.write(text, one);
			}
			
			
		}
		
	}

	
	/**
	 * 新建一个Reducer类
	 */
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text value, Iterable<IntWritable> iterable,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum = 0;
			IntWritable i = new IntWritable();
			
			for (IntWritable intWritable : iterable) {
				sum += intWritable.get();
			}
			
			i.set(sum);
			context.write(value, i);
		}
		
		
	}
	
	
}




