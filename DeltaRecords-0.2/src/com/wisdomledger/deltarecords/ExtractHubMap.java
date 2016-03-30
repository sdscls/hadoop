package com.wisdomledger.deltarecords;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ExtractHubMap extends
		Mapper<LongWritable, Text, Text, Text> {
	
	private String[] rowkeyColIndexs={};
	
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		rowkeyColIndexs = conf.getStrings("rowkeyColIndex");
	}
	
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		//get file's timestamp
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String fileName = fileSplit.getPath().getName();
		String timeStamp = fileName.substring(fileName.length()-19, fileName.length()-4);
		
		//get the data's primary key
		String lineString = value.toString();
		String[] arr = lineString.split("\034", -1);
		String rowkey_ = "";
		for(int i=0; i<rowkeyColIndexs.length;i++){
			rowkey_ += arr[Integer.parseInt(rowkeyColIndexs[i])]+":";
		}
		String rowkey = rowkey_.substring(0, rowkey_.length()-1);
System.out.println("Timestamp: " + timeStamp);
System.out.println("Primary Key, " + rowkey);
		
		//map's value is composed of timeStamp and the data's record
		String value_ = timeStamp+"|##|"+lineString;

		Text k = new Text(rowkey);
		Text v = new Text(value_);
		context.write(k, v);
		
	}

}
