package exp5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import exp5.Inverse_index.Map;
import exp5.Inverse_index.Reduce;

public class sort2 {
	public static class Map extends Mapper<Object,Text,DoubleWritable,Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] line = value.toString().split(",");
			if(most_common.common_list.contains(line[0])) {
				context.write(new DoubleWritable(Double.parseDouble(line[2])), new Text(line[1]));
			}
		}
	}
	private static class IntWritableDecreasingComparator extends DoubleWritable.Comparator {
//	       public int compare(WritableComparable a, WritableComparable b) {
//	         return -super.compare(a, b);
//	       }
	       
	       public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	           return -super.compare(b1, s1, l1, b2, s2, l2);
	       }
	   }
	public static class Reduce extends Reducer<DoubleWritable,Text,DoubleWritable,Text>{
		public void reduce(DoubleWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			for(Text value:values) {
				context.write(key, value);	
			}
		}
	}
	public static void main(String args[]) {
		try {	Configuration conf = new Configuration();
		Job job = new Job(conf, "sort");
//		String[] otherArgs = new String[] {"txt_input","output"};
		job.setJarByClass(sort2.class);
//		job.setJar("Job2.jar");
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		job.setSortComparatorClass(IntWritableDecreasingComparator.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
	       } catch (Exception e) {      e.printStackTrace();                              }
	}
}
