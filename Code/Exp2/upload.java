
package exp2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;

public class upload {
	static private String pattern ="[^a-zA-Z0-9]";
	public static class Map extends Mapper<Object,Text,Text,IntWritable> {
		private Set<String> stopwords;
		String temp;
//		private String localFiles = "/home/narip/hadoop_exp/stop_words_eng.txt";
		private Path[] localFiles;

		public void setup(Context context) throws IOException {
			stopwords = new TreeSet<String>();
			Configuration conf = context.getConfiguration();
		localFiles = DistributedCache.getLocalCacheFiles(conf);//get stop_wrods_eng.txt
		for(int i = 0; i < localFiles.length; i++) {
			String line;
			BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));
			while ((line = br.readLine()) != null) {
				StringTokenizer itr = new StringTokenizer(line);
				while (itr.hasMoreTokens()) {
					stopwords.add(itr.nextToken());
				}
			}
		}
		}

		public void map(Object key, Text value, Context context){
			//get filename
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			//Split the words
			//才开始读进来是一行一行的
			String line = value.toString();
			line = line.replaceAll(pattern, " ");
			line = line.toLowerCase();
			StringTokenizer itr = new StringTokenizer(line);
			//Map the term and the filename
			while(itr.hasMoreTokens()) {
					temp = itr.nextToken();
						try {
							if(!stopwords.contains(temp)) {
								context.write(new Text(temp+"#"+fileName), new IntWritable(1));	
							}
						} catch (IOException | InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
			}
		}	
	}
	public static class Combiner extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val:values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	//<term,doc> 按照term reduce
	public static class NewPartitioner extends HashPartitioner<Text,IntWritable>{
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			System.out.println("111");
			String term = new String();
			System.out.println(key.toString());
			
			term = key.toString().split(",")[0];
			return super.getPartition(new Text(term), value, numReduceTasks);
		}
	}
	public static class Reduce extends Reducer<Text,IntWritable,Text,Text>{
		String preview = " ";
		List<String> list = new ArrayList<String>();
		String term;String word;
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val : values) {
				sum += val.get();
			}
			term = key.toString().split("#")[0];
			word = "<"+key.toString().split("#")[1]+","+sum+">;";
			if(!preview.equals(term)&&!preview.equals(" ")) {
				int count = 0;
				StringBuilder out = new StringBuilder();
				for(String l:list) {
					count += Integer.parseInt(l.substring(l.indexOf(",")+1, l.indexOf(">")));
					out.append(l);
				}
				out.append("<total,"+count+">.");
				context.write(new Text(preview), new Text(out.toString()));	
				list = new ArrayList<String>();
			}
			preview = term;
			list.add(word);
		}
		
		public void cleanup(Context context) {
			int count = 0;
			StringBuilder out = new StringBuilder();
			for(String l:list) {
				count += Integer.parseInt(l.substring(l.indexOf(",")+1, l.indexOf(">")));
				out.append(l);
			}
			out.append("<total,"+count+">.");
			try {
				context.write(new Text(preview), new Text(out.toString()));
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
		try {	
			Configuration conf = new Configuration();
			DistributedCache.addCacheFile(new URI("hdfs://master:9000/stop_words/stop_words_eng.txt"), conf);
		Job job = new Job(conf, "invert index");
		String[] otherArgs = new String[] {"txt_input","output"};
		//job.setJarByClass(upload.class);
		job.setJar("upload.jar");
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combiner.class);
		job.setPartitionerClass(NewPartitioner.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	       } catch (Exception e) {      e.printStackTrace();                              }
}
}
