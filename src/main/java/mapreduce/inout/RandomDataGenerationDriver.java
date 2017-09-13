package mapreduce.inout;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 生成随机的StackOverFlow评论
 * 将1000个单词列表生成随机导语， 生成随机分数，行ID，用户ID以及随机创建时间。  
 */
public class RandomDataGenerationDriver {
	
	public static final String NUM_MAP_TASKS = "random.generator.map.tasks";
	public static final String NUM_RECORDS_PER_TASK = "random.generator.num.records.per.map.task";
	public static final String RANDOM_WORD_LIST = "random.generator.random.word.list";
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		int numMapTasks = Integer.parseInt(args[0]);
		int numRecordsPerTask = Integer.parseInt(args[1]);
		Path wordList = new Path(args[2]);
		Path outputDir = new Path(args[3]);
		
		Job job = Job.getInstance();
		job.setJobName("RandomDataGenerationDriver");
		job.setJarByClass(RandomDataGenerationDriver.class);
		
		job.setNumReduceTasks(0);
		job.setInputFormatClass(RandomStackOverflowInputFormat.class);
		RandomStackOverflowInputFormat.setNumMapTasks(job, numMapTasks);
		RandomStackOverflowInputFormat.setNumRecordPerTask(job, numRecordsPerTask);
		
		RandomStackOverflowInputFormat.setRandomWordList(job, wordList);
		TextOutputFormat.setOutputPath(job, outputDir);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		System.exit(job.waitForCompletion(true)?0:2);
		
	}
    static class FakeInputSplit extends InputSplit implements Writable{

		@Override
		public void write(DataOutput out) throws IOException {
			
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[0];
		}
	}
	
    static class RandomStackOverflowInputFormat extends InputFormat<Text, NullWritable>{
    	
		@Override
		public List<InputSplit> getSplits(JobContext context)
				throws IOException, InterruptedException {
			 int numSplits = context.getConfiguration().getInt(NUM_MAP_TASKS, -1);
			 
			 ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
			 for(int i = 0; i< splits.size(); i++){
				 splits.add(new FakeInputSplit());
			 }
			 return splits;
		}

		@Override
		public RecordReader<Text, NullWritable> createRecordReader(
				InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			RecordReader rr = new RandomStackOverflowRecordReader();
			rr.initialize(split, context);
			return rr;
		}
		
		public static void setNumMapTasks(Job job, int i){
			job.getConfiguration().setInt(NUM_MAP_TASKS, i);
		}
		public static void setNumRecordPerTask(Job job, int i ){
			job.getConfiguration().setInt(NUM_RECORDS_PER_TASK, i);
		}
		
		public static void setRandomWordList(Job job, Path file){
			DistributedCache.addCacheFile(file.toUri(), job.getConfiguration());
		}
    	
    }
    
    static class RandomStackOverflowRecordReader extends RecordReader<Text, NullWritable>{
    	private int numRecordsToCreate = 0;
    	private int createdRecords = 0;
    	private Text key = new Text();
    	private NullWritable value = NullWritable.get();
    	private Random rdn = new Random();
    	private ArrayList<String> randomWords = new ArrayList<String>();
    	private SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T' HH:mm:ss.SSS");
    	
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			this.numRecordsToCreate = context.getConfiguration().getInt(NUM_RECORDS_PER_TASK, -1);
			
			URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());
			BufferedReader rdr = new BufferedReader(new FileReader(files[0].toString()));
			String line;
			while((line = rdr.readLine())!= null){
				randomWords.add(line);
			}
			rdr.close();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if(createdRecords < numRecordsToCreate){
				int score =  Math.abs(rdn.nextInt() % 15000);
				int rowId = Math.abs(rdn.nextInt() % 100000000);
				int postId = Math.abs(rdn.nextInt() % 1000000);
				int userId =Math.abs(rdn.nextInt() % 1000000);
				
				String creationDate = frmt.format(Math.abs(rdn.nextInt()));
				String text = getRandomText();
				
				String randomRecord = "<row id=\""+rowId+"\" PostId=\""+postId+"\" score=\""+score+"\" Text=\""+text+"\""
						+ " CreationDate=\""+creationDate+"\" UserId=\""+userId+"\" />";
				
				key.set(randomRecord);
				this.createdRecords ++;
				
				return true;
			}else{
				return false;
			}
		}

		private String getRandomText() {
			StringBuffer sbr = new StringBuffer();
			int numWords = Math.abs(rdn.nextInt())%30 + 1;
			for(int i = 0; i < numWords; ++i){
				sbr.append(randomWords.get(Math.abs(rdn.nextInt() % randomWords.size())) + "");
			}
			return sbr.toString();
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			 return key;
		}

		@Override
		public NullWritable getCurrentValue() throws IOException,
				InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public void close() throws IOException {
			
		}
		
		
    	
    }
}
