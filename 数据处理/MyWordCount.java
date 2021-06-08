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
   * ͳ���ı��е��ʳ��ֵĴ���,���ļ���ʽ�������
 */
public class MyWordCount {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		//1.����conf����,����hdfs·��
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.101.135:9000");
		
		//2.����job���� ��job������ָ�� jar map reduce���������   �����ʽ����ʽ �����·��
		Job job = Job.getInstance(conf,"wordcount");  //����job����
		job.setJarByClass(MyWordCount.class);   //ָ�� jar
		job.setMapperClass(MyMapper.class);  //ָ��mapper
		job.setReducerClass(MyReducer.class); //ָ��Reducer
		
		job.setOutputKeyClass(Text.class); //ָ��Mapper�����������key������
		job.setOutputValueClass(IntWritable.class); //ָ��Mapper�����������value������
		
		job.setInputFormatClass(TextInputFormat.class);  //ָ������ĸ�ʽ����ʽ
		
		//��Ҫ������ԭʼ���ݶ������
		FileInputFormat.addInputPath(job, new Path("/pro/mywordcount/input/upload.txt")); //����·��
		Path outPath = new Path("/pro/mywordcount/output/");  //�½����·��
		FileSystem.get(conf).delete(outPath, true);  //���·������,��ɾ��ԭ��·��
		FileOutputFormat.setOutputPath(job, outPath);  //ָ�����·��
		
		System.exit(job.waitForCompletion(true)?0:1);  //����������,ϵͳ�����˳�
		
	}
	
	
	/**
	 * �½�һ��Mapper��
	 */
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String[] words = value.toString().strip(str, "[]").strip(str, "\'").split("\\,s+");  //��ÿһ�е��ı�����ɾ��[]'�����ַ�����ת�����ַ��������տո�ָ�
			
			IntWritable one = new IntWritable(1);
			Text text = new Text();
			
			for (String word : words) {
				text.set(word);
				context.write(text, one);
			}
			
			
		}
		
	}

	
	/**
	 * �½�һ��Reducer��
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




