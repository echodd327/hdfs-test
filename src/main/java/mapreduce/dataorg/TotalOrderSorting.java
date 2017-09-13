package mapreduce.dataorg;

import java.io.IOException;
import java.util.Map;

import mapreduce.util.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/*
 * 全排序
 */
public class TotalOrderSorting {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Path inPath = new Path(args[0]);
		Path outputStage = new Path(args[1] + "_staging");
		
		Path paritionFile = new Path(args[1] + "_paritions.list");
		Path outputOrder = new Path(args[1]);
		
		Job sampleJob = Job.getInstance(conf);
		sampleJob.setJarByClass(TotalOrderSorting.class);
		
		sampleJob.setMapperClass(LastAccessDataMapper.class);
		sampleJob.setNumReduceTasks(0);

		sampleJob.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(sampleJob, inPath);
		
		sampleJob.setOutputKeyClass(Text.class);
		sampleJob.setOutputValueClass(Text.class);
		sampleJob.setOutputFormatClass(SequenceFileOutputFormat.class); //TODO
		SequenceFileOutputFormat.setOutputPath(sampleJob, outputStage);
		
		int code = sampleJob.waitForCompletion(true) ? 0:1;
		if(code == 0){
			Job orderJob = Job.getInstance();
			orderJob.setJarByClass(TotalOrderSorting.class);
			
			orderJob.setMapperClass(Mapper.class);
			orderJob.setReducerClass(ValueReducer.class);
			orderJob.setNumReduceTasks(10);
			
			orderJob.setPartitionerClass(TotalOrderPartitioner.class);  //TODO
			TotalOrderPartitioner.setPartitionFile(conf, paritionFile); //TODO
			
			orderJob.setOutputKeyClass(Text.class);
			orderJob.setOutputValueClass(NullWritable.class);
			orderJob.setInputFormatClass(SequenceFileInputFormat.class);
			SequenceFileInputFormat.setInputPaths(orderJob, outputStage);
			
			TextOutputFormat.setOutputPath(orderJob, outputOrder);
			orderJob.getConfiguration().set("mapred.textoutputformat.seperator", "");
			
			InputSampler.writePartitionFile(orderJob, new InputSampler.RandomSampler(.001, 10000));//TODO
			
			code = orderJob.waitForCompletion(true)?0:2;
		}
	    
		FileSystem.get(new Configuration()).delete(paritionFile,false);
		FileSystem.get(new Configuration()).delete(outputStage ,false);
		
		System.exit(code);
	}
	
	static class LastAccessDataMapper extends Mapper<Object,Text,Text,Text>{
		
		private Text outkey = new Text();
		
		@Override
		protected void map(Object key, Text value,Context context)
				throws IOException, InterruptedException {
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			
			outkey.set(parsed.get("LastAccessDate"));
			context.write(outkey, value);
		}
	}
	
	static class ValueReducer extends Reducer<Text,Text,Text,NullWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			for(Text t: values){
				context.write(t, NullWritable.get());
			}
		}
	}
}
