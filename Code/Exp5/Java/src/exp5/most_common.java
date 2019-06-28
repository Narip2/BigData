package exp5;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import exp5.sort.Map;
import exp5.sort.Reduce;

public class most_common {
	static Vector common_list = new Vector();
	public static int common_number = 0;
	static int[] movielist = test.movielist;
	public static class Map extends Mapper<Object,Text,Text,Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] line = value.toString().split(",");
			context.write(new Text(line[0]), new Text(line[1]+","+line[2]));
		}
	}
	public static class Reduce extends Reducer<Text,Text,Text,NullWritable>{
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int count = 0;
			String[] str;
			for(Text value:values) {
				str = value.toString().split(",");
				for(int i = 0; i < 10; i++) {
					if((Integer.parseInt(str[0]) == movielist[i])&&(Double.parseDouble(str[1]) >= 3.5)) {
						count++;
						break;
					}
				}
			}
			if(count > common_number) {
				common_number = count;
				common_list.clear();
				common_list.add(key.toString());
			} else if(count == common_number) {
				common_list.add(key.toString());
			}
		}
		public void cleanup(Context context) throws IOException, InterruptedException {
			for(Object element:common_list) {
				context.write(new Text(element.toString()),NullWritable.get());
			}
		}
	}
		
	public static void main(String args[]) {
		try {	Configuration conf = new Configuration();
		Job job = new Job(conf, "most_common");
//		String[] otherArgs = new String[] {"txt_input","output"};
		job.setJarByClass(most_common.class);
//		job.setJar("Job2.jar");
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
	       } catch (Exception e) {      e.printStackTrace();                              }
	}
}
