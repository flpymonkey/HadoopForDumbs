import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Program{
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		//Arraylist to store common words
		private ArrayList<String> commonWords = new ArrayList<String>();
		
		public void map(LongWritable Key, Text value, Context content) throws InterruptedException, IOException{
			IntWritable voutput = new IntWritable(1);
			String newValue = value.toString();

			String[] sArray = newValue.split("\\s+");
			for(int i = 0; i< sArray.length; i++){
				// create variable outside of loop and set to check if addable to elements
				Text newText = new Text(sArray[i].replaceAll("^[^a-zA-Z0-9-'\\s]+|[^a-zA-Z0-9-'\\s]+$", ""));
				if (!commonWords.contains(newText)){
					content.write(newText, voutput); // voutput is the same everytime so we should create a constant
				}
				//sArray[i].replaceAll to fix the exta characters problem. Add hyphin and single apostrophy
				//http://stackoverflow.com/questions/24967089/java-remove-all-non-alphanumeric-character-from-beginning-and-end-of-string
			
			}
		}
		
		public void setup(Context content) throws FileNotFoundException{
			// Scanner to read in the common words and save to arrayList
			File inputF = new File("comWordList.txt");
			Scanner linesc = new Scanner(inputF);
			while (linesc.hasNextLine()){
				String word = linesc.nextLine();
				commonWords.add(word);
			}
		}
	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{ // text, intwritable, text, intwritable final output of reducer would be word and final count
		public void reduce(Text key, Iterable<IntWritable> values, Context content) throws IOException, InterruptedException{ // putting, consider adding try catch block
			int intSum = 0;
			Iterator<IntWritable> iterator = values.iterator();
			while(iterator.hasNext()){
				intSum += Integer.parseInt(iterator.next().toString());
			}
			content.write(key, new IntWritable(intSum));
		}
	}
	
//	public static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{ // text, intwritable, text, intwritable final output of reducer would be word and final count
//		public void reduce(Text key, Iterable<IntWritable> values, Context content) throws IOException, InterruptedException{ // putting, consider adding try catch block
//			int intSum = 0; // Just call the reduce class
//			Iterator<IntWritable> iterator = values.iterator();
//			while(iterator.hasNext()){
//				intSum += Integer.parseInt(iterator.next().toString());
//			}
//			content.write(key, new IntWritable(intSum));
//		}
//	}
	
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.set("numReduce", args[3]);
		Job myJob = new Job(conf, "wordCount"); //depricated needs replacement 
		myJob.setJarByClass(Program.class);
		myJob.setMapperClass(MyMapper.class);
		myJob.setCombinerClass(MyReducer.class);
		myJob.setReducerClass(MyReducer.class);
		myJob.setNumReduceTasks(Integer.parseInt(conf.get("numReduce")));
		myJob.setMapOutputKeyClass(Text.class);
		myJob.setMapOutputValueClass(IntWritable.class);
		myJob.setOutputKeyClass(Text.class);
		myJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(myJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myJob,  new Path((args[1])));
		int jobCode = (myJob.waitForCompletion(true) ? 0:1);
		System.exit(jobCode);
	}
}

