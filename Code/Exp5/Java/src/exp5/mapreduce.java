
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

public class mapreduce {
	static int usernumber = 611;
	static Double[] distance = new Double[usernumber];
	static int[] movielist;
	static Double[] movie_score;
	public static class Map extends Mapper<Object,Text,Text,Text> {
		public void setup(Context context) {
		if(menu.shell_num == 1) {
			movielist = assess.movielist;
			movie_score = assess.movie_score;
		} else if(menu.shell_num == 2) {
			movielist = eulerdistance.movielist;
			movie_score = eulerdistance.movie_score;
		}
		//Initialize the distance to a value like inf
		for(int i = 0; i < 10; i++) {
			System.out.println(movielist[i]);
		}
		for(int i = 0; i < usernumber; i++)
		{
			distance[i] = 25.0;
		}
	}
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] str = line.split(",");
			//context content: userId, (movieId,rating)
			context.write(new Text(str[0]),new Text(str[1]+","+str[2]));
		}	
	}
	public static class Reduce extends Reducer<Text,Text,Text,DoubleWritable>{
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			double dist = 0;
			int count = 0;
				for(Text value:values) {
					for(int i = 0; i < eulerdistance.recommend_number; i++) {
						if(Integer.parseInt(value.toString().split(",")[0]) == movielist[i]) {
							count++;
							dist += Math.pow(movie_score[i] - (Double.parseDouble(value.toString().split(",")[1])), 2);
						}
					}
				}
			if(dist != 0) {
				distance[Integer.parseInt(key.toString())] = dist/count;
				context.write(key, new DoubleWritable(dist));
			}
		}
	}
	public static void main(String args[]) {
		try {	Configuration conf = new Configuration();
		Job job = new Job(conf, "invert index");
//		String[] otherArgs = new String[] {"txt_input","output"};
		job.setJarByClass(mapreduce.class);
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
