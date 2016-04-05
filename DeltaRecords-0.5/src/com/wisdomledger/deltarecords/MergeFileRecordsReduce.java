package com.wisdomledger.deltarecords;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class MergeFileRecordsReduce extends Reducer<Text, Text, NullWritable, Text>{
	
	NullWritable nw = NullWritable.get();
	
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		String val_timestamp = "";
		String val_string    = "";
		
//		int is_duplication=0;
		
		for(Text val: values){
			
//			is_duplication++;
			
			String[] arrs = val.toString().split("\\|##\\|",-1);
			
			if(arrs[0].compareTo(val_timestamp)>0){
				val_timestamp = arrs[0];
				val_string = val_timestamp+":"+arrs[1];
			}
			
		}
		
		//Merge the file - output the records without duplicated data
		Text v = new Text(val_string);
		context.write(nw, v);
		
		//output the delta data
		/*if(is_duplication <= 1){

			Text v = new Text(val_string);
			context.write(nw, v);
		}*/
		
	}

}
