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



public class PageRankItr {
	private static double d = 0.85;
	public static class PRIterMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key, Text value, Context context) {
			String line = value.toString();
			String[] tuple = line.split("\t");
			String pagename = tuple[0];
			Double pagerank = Double.parseDouble(tuple[1]);
			
			
			String links = tuple[2];
			if(tuple.length > 2) {//Have out edges
				String[] linkpage = links.split(",");
				String pagevalue = String.valueOf(pagerank/linkpage.length);
				for(String link:linkpage) {
					try {
						context.write(new Text(link), new Text(pagevalue));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				try {
					context.write(new Text(pagename), new Text("#"+links));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	public static class PRIterReducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) {
			double pagerank = 0;
			String links = "";
			for(Text value:values) {
				if(value.toString().startsWith("#")) {
//					links += value.toString().substring(1);
					links = value.toString().substring(value.toString().indexOf("#")+1);
					continue;
				}
				pagerank += Double.parseDouble(value.toString());
			}
			pagerank = (double)(1-d) + d*pagerank;
			try {
				context.write(new Text(key), new Text(pagerank+"\t" + links));
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
		Job job = new Job(conf, "PageRankItr");
//		String[] otherArgs = new String[] {"txt_input","output"};
		job.setJarByClass(PageRankItr.class);
		
		job.setMapperClass(PRIterMapper.class);
		job.setReducerClass(PRIterReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	       } catch (Exception e) {      e.printStackTrace();                              }
	}
}
