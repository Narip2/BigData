
package exp5;
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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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

public class assess_mapreduce {
	public static double result;
	public static int count;
	public static class Map extends Mapper<Object,Text,Text,Text> {
		public void setup(Context context) {
			result = 0;
			count = 0;
		}
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] str = line.split(",");
			if(str.length == 4) {
				if(Integer.parseInt(str[0]) == assess.userid) {
					context.write(new Text(str[1]), new Text(str[2]));
				}
			} else {
				str = line.split("\t");
				context.write(new Text(str[0]), new Text(str[1]));
			}
		}	
	}
	public static class Reduce extends Reducer<Text,Text,Text,NullWritable>{
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			double tmp = 0;
			int cnt = 0;
			for(Text value:values) {
				cnt++;
				tmp = Double.parseDouble(value.toString()) - tmp;
			}
			if(cnt == 2) {
				count++;
				result += Math.abs(tmp);
//				result += tmp*tmp;
			}
			context.write(new Text(tmp+""), NullWritable.get());
		}
		public void cleanup(Context context) {
			result = result/count;
		}
	}
	public static void main(String args[]) {
		try {	Configuration conf = new Configuration();
		Job job = new Job(conf, "invert index");
//		String[] otherArgs = new String[] {"/home/narip/Code/Python/Big_Data/ml-latest-small/movies.csv","/home/narip/Code/Python/Big_Data/ml-latest-small/ratings.csv","/home/narip/Code/Python/Big_Data/test_output/ttt"};
		job.setJarByClass(assess_mapreduce.class);
//		job.setJar("Job2.jar");
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
	       } catch (Exception e) {      e.printStackTrace();                              }
}
}
