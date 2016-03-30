package com.wisdomledger.deltarecords;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Controller {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length < 4) {
			System.out.println("parameters error! ");
			System.exit(1);
		}
		
		String rowKeyColumnIndex = "";
		String fileName = otherArgs[0];
		String input = otherArgs[1];
		String output = otherArgs[2];
		
		input= input+"//" + fileName;
		
		String columnIndexParam = otherArgs[3];
		String[] indexs = columnIndexParam.split(",");
		
		for (int i = 0; i < indexs.length; i++) {
			rowKeyColumnIndex += indexs[i] + ",";
		}
		
		String primaryKeyIndex = rowKeyColumnIndex.substring(0, rowKeyColumnIndex.length()-1);
		conf.setStrings("rowkeyColIndex", primaryKeyIndex);
//		conf.set("file.pattern", ".*WRKING_PNL_ADJ_F_DEL_20150613_183023.DAT");
		
		Job job = new Job(conf, "ehub");
		job.setJarByClass(Controller.class);
		job.setMapperClass(ExtractHubMap.class);
//		job.setMapperClass(MergeFileMap.class);
		job.setReducerClass(ExtractHubReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		//set the numbers of map
		FileInputFormat.setMinInputSplitSize(job, 1073741824);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
