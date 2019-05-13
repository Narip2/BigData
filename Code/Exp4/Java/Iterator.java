package exp4;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
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


public class Iterator {
	public static double distance(double x1, double y1, double x2, double y2) {
		return Math.sqrt((x1 - x2)*(x1 - x2) + (y1 - y2)*(y1 - y2));
	}
	public static class Itermap extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
		public void map(LongWritable key, Text value, Context context){
			double d;
			int tag = 0;
			for(int i = 0; i < Driver.count; i++) {
				d = Double.POSITIVE_INFINITY;
				for(int j = 0; j < Driver.k; j++) {
					if(distance(Driver.points[i][0],Driver.points[i][1],Driver.centers[j][0],Driver.centers[j][1]) < d) {
						d = distance(Driver.points[i][0],Driver.points[i][1],Driver.centers[j][0],Driver.centers[j][1]);
						tag = j;
					}
				}
				//<centers_id, points_id>
				try {
					context.write(new IntWritable(tag), new IntWritable(i));
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}
		}	
	}
	public static class Iterreduce extends Reducer<IntWritable,IntWritable,Text,Text>{
		public void reduce(IntWritable key, Iterable<IntWritable> values,Context context){
			double sum_x = 0;
			double sum_y = 0;
			int count = 0;
			for(IntWritable value:values) {
				sum_x += Driver.points[value.get()][0];
				sum_y += Driver.points[value.get()][1];
				count++;
			}
			//sum_x and sum_y stands for new centers now
			sum_x = sum_x/count;
			sum_y = sum_y/count;
			if((Driver.centers[key.get()][0]!=sum_x)||(Driver.centers[key.get()][1]!=sum_y)) {
				Driver.centers[key.get()][0] = sum_x;
				Driver.centers[key.get()][1] = sum_y;
			}
		}
	}
	public static void main(String args[]) {
	try {	Configuration conf = new Configuration();
	Job job = new Job(conf, "Iterator");
//	String[] otherArgs = new String[] {"txt_input","output"};
	job.setJarByClass(Iterator.class);
//	job.setJar("Job2.jar");
	
	job.setMapperClass(Itermap.class);
	job.setReducerClass(Iterreduce.class);
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(IntWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.waitForCompletion(true);
//	System.exit(job.waitForCompletion(true) ? 0 : 1);
       } catch (Exception e) {      e.printStackTrace();                              }
}
}
