//StudentMapper.class

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class StudentMapper extends Mapper<LongWritable,Text,Text,Text>{
	Text key=new Text();
	Text value=new Text();
	@Override
	public void map(LongWritable k,Text i,Context c) throws IOException,InterruptedException
	{
		String[] records=i.toString().split(",");
		
		key.set(records[1].toUpperCase());
		value.set(records[2]+","+records[3]+","+records[0]);
		c.write(key, value);
	}
}

//StudentPartitioner.class

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class StudentPartitioner extends Partitioner<Text,Text>{
	@Override
	public int getPartition(Text key,Text value,int numOfReducers)
	{
		String[] records=value.toString().split(",");
		int age=Integer.parseInt(records[0]);
		if(age<20)
		{
			return 0;
		}
		else if(age>=20&&age<30)
		{
			return 1%numOfReducers;
		}
		else{
			return 2%numOfReducers;			
		}
	}

}

//StudentReducer.class

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StudentReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> value, Context c)
			throws IOException, InterruptedException {
		int highest = 0;
		String name="";
		String age="";
		for (Text data : value) {
			String[] line=data.toString().split(",");
			int score = Integer.parseInt(line[1]);
			if (score > highest) {
				highest = score;
				name=line[2];
				age=line[0];
			}
		}
		c.write(key, new Text(name+"-"+highest+"-"+age));
	}
}

//StudentDriver.class with ToolRunner

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class StudentDriver extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new StudentDriver(), args);
		System.exit(status);
	}
	
	@Override
	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Job job=Job.getInstance(getConf(), "Student Toppers");
		job.setJarByClass(StudentDriver.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(StudentMapper.class);
		job.setPartitionerClass(StudentPartitioner.class);
		job.setReducerClass(StudentReducer.class);
		
//		job.setNumReduceTasks(3);
		
		
		return job.waitForCompletion(true) ? 0 : 1;	
	}
}

//Sampledata(student.txt)

Swathi,Female,20,90
Seetha,Female,42,80
Ram,Male,30,89
Priya,Female,27,91
Raj,Male,27,93
Vijay,Male,45,95
Kavi,Female,32,82
Vinay,Male,19,100
Swetha,Female,18,82

//Problem Statement

In the Student dataset divide the result set into 3 files based on age where  age<20 and age>=20&&age <30 and  age >=30.
Find out the max marks in each gender  in these 3 groups.

//Running the Driver class
hadoop jar /home/cloudera/Desktop/StudentTopper.jar StudentDriver -D mapred.reduce.tasks=3 student.txt studenttoppers
