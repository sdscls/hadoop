package com.wisdomledger.deltarecords;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Controller {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length < 1) {
			System.out.println("parameters error! ");
			System.exit(1);
		}

		Reducer reducer = null;

		Properties props = new Properties();
		FileInputStream inputStream = new FileInputStream(otherArgs[0]);
		props.load(inputStream);
		// props.load(Controller.class.getClassLoader().getResourceAsStream(
		// "com/wisdomledger/deltarecords/extracthub.conf"));

		String reducerType = props.getProperty("reducer");
		String fileFormat = props.getProperty("format");
		String inputPath = props.getProperty("input");
		String outputPath = props.getProperty("output");
		String pkIndex = props.getProperty("pkindex");

		String[] inds = pkIndex.split(",");
		System.out.println(inds.length);

		System.out.println("fileFormat:" + fileFormat + ", inputPath:"
				+ inputPath + ", outputPath:" + outputPath + ", pkIndex:"
				+ pkIndex);

		if (fileFormat == null || fileFormat.trim().equals("")) {
			System.out.println("format is not correct!");
			System.exit(1);
		} else if (inputPath == null || inputPath.trim().equals("")) {
			System.out.println("Please don't forget to config the input path!");
			System.exit(1);
		} else if (outputPath == null || outputPath.equals("")) {
			System.out
					.println("Please don't forget to config the output path!");
			System.exit(1);
		} else if (pkIndex == null || pkIndex.equals("")
				|| (!pkIndex.contains(",") && (!(inds.length == 1)))) {

			System.out
					.println("Please don't forget to config the primary key index! And the PK index delimiter should be ','");
			System.exit(1);

		} else {

			Object o = Class.forName(reducerType).newInstance();
			reducer = (Reducer) o;

			System.out.println(reducer.getClass());

			String rowKeyColumnIndex = "";
			// String fileName = otherArgs[0];
			// String input = otherArgs[1];
			// String output = otherArgs[2];

			// input = input + "//" + fileName;
			inputPath = inputPath + "//" + fileFormat;

			// String columnIndexParam = otherArgs[3];

			String[] indexs = pkIndex.split(",");

			for (int i = 0; i < indexs.length; i++) {
				rowKeyColumnIndex += indexs[i] + ",";
			}

			String primaryKeyIndex = rowKeyColumnIndex.substring(0,
					rowKeyColumnIndex.length() - 1);
			conf.setStrings("rowkeyColIndex", primaryKeyIndex);
			// conf.set("file.pattern",
			// ".*WRKING_PNL_ADJ_F_DEL_20150613_183023.DAT");

			Job job = new Job(conf, "ehub");
			job.setJarByClass(Controller.class);
			job.setMapperClass(ExtractHubMap.class);

			job.setReducerClass(reducer.getClass());
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);

			// set the numbers of map
			FileInputFormat.setMinInputSplitSize(job, 1073741824);
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

	}

}
