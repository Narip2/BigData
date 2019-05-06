package exp3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import org.apache.hadoop.mapreduce.Partitioner;

public class PageRankViewer {
	public static class PageRankViewerMapper extends Mapper<LongWritable, Text,DoubleWritable,Text>
	{
		public void map(LongWritable key, Text value, Context context) {
			String line = value.toString();
			String[] tuple = line.split("\t");
			String pagename = tuple[0];
			double pagerank = Double.parseDouble(tuple[1]);
			try {
				context.write(new DoubleWritable(pagerank), new Text(pagename));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public static class PageRankViewerReduce extends Reducer<DoubleWritable,Text,NullWritable,Text>
	{
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
		{
			try {
				for(Text value:values) {
					DecimalFormat df = new DecimalFormat("0.0000000000");
					double keyout = key.get();
					context.write(NullWritable.get(), new Text("("+value+","+df.format(keyout)+")"));
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public static class NewPartitioner extends HashPartitioner<DoubleWritable,Text>{
		public int getPartition(DoubleWritable key,Text value, int numReduceTasks) {
			return super.getPartition(key, value, numReduceTasks);
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

	public static void main(String args[]) {
		try {	Configuration conf = new Configuration();
		Job job = new Job(conf, "PageRankViewer");
//		String[] otherArgs = new String[] {"txt_input","output"};
		job.setJarByClass(PageRankViewer.class);
//		job.setJar("Job2.jar");
		
		job.setMapperClass(PageRankViewerMapper.class);
		job.setReducerClass(PageRankViewerReduce.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setSortComparatorClass(IntWritableDecreasingComparator.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	       } catch (Exception e) {      e.printStackTrace();                              }
	}
}
