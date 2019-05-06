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

public class PageRankDriver {
	private static int times = 10;

	public static void main(String args[])
	{ 
		String[] path = new String[]{"input","output"};
		String[] forGB = {path[0],path[1]+"/Data0"};
		GraphBuilder.main(forGB);
		String[] forItr = {"",""};
		for(int i = 0; i < times; i++) {
			forItr[0] = path[1] + "/Data" + i;
			forItr[1] = path[1] + "/Data" + String.valueOf(i+1);
			PageRankItr.main(forItr);
		}
		String[] forRV = {path[1] + "/Data" +String.valueOf(times), path[1] + "/FinalRank"};
		PageRankViewer.main(forRV);
	}
}
