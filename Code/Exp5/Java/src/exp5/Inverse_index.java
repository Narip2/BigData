
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

public class Inverse_index {
	public static Double[] distance = mapreduce.distance;
	public static int[] movielist = eulerdistance.movielist;
	public static class Map extends Mapper<Object,Text,Text,Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] str = line.split(",");
			// movieId, userId
			context.write(new Text(str[1]), new Text(str[0] +","+ str[2]));
		}	
	}
	public static class Reduce extends Reducer<Text,Text,Text,DoubleWritable>{
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			//Sum all movieId which are in 10 movies
//			StringBuilder result = new StringBuilder();
			double sumdistance = 0;
			double sumscore = 0;
			int flag = 0;
			//movies are in the movielist
			for(int i = 0; i < 10; i++) {
				if(Integer.parseInt(key.toString()) == movielist[i]) {
					flag = 1;
				}
			}
			//Drop the movielist
			if(flag == 1)return;
			int count = 0;
			for(Text value:values) {
				count++;
				String[] str = value.toString().split(",");
				sumscore += (1/distance[Integer.parseInt(str[0])])*Double.parseDouble(str[1]);
				sumdistance += 1/distance[Integer.parseInt(str[0])];
//				result.append(value+",");
			}
			sumscore = sumscore/sumdistance;
			//delete the last comma
//			result.deleteCharAt(result.length()-1);
			//MovieId, score
			if(count >= 3) context.write(key, new DoubleWritable(sumscore));
		}
//		public void cleanup(Context context) {
//			for(int i = 0; i < 611; i++) {
//				System.out.println(i + "  " + distance[i]);
//			}
//		}
	}
	public static void main(String args[]) {
		try {	Configuration conf = new Configuration();
		Job job = new Job(conf, "invert index");
//		String[] otherArgs = new String[] {"txt_input","output"};
		job.setJarByClass(Inverse_index.class);
//		job.setJar("Job2.jar");
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
	       } catch (Exception e) {      e.printStackTrace();                              }
}
}
