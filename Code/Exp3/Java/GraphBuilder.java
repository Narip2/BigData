package exp3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;

public class GraphBuilder {
	//Step 1
		public static class GraphBuilderMapper extends Mapper<LongWritable,Text,Text,Text>
		{
			public void map(LongWritable key, Text value, Context context) {
				String line = value.toString();
				String [] tuple = line.split("\t");
				try {
					context.write(new Text(tuple[0]), new Text("1.0\t"+tuple[1]));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	public static void main(String args[]) {
		try {	Configuration conf = new Configuration();
		Job job = new Job(conf, "GraphBuilder");
//		String[] otherArgs = new String[] {"txt_input","output"};
		job.setJarByClass(GraphBuilder.class);
//		job.setJar("Job2.jar");
		
		job.setMapperClass(GraphBuilderMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
	       } catch (Exception e) {      e.printStackTrace();                              }
	}
}
