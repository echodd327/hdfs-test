package mapreduce.inout;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import mapreduce.util.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.google.protobuf.TextFormat;

import redis.clients.jedis.Jedis;

/**
 * 按最后访问日期对Redis进行分区
 */
public class PartitionPruningOutputDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Path inputPath = new Path(args[0]);
		Job job = Job.getInstance();
		job.setJobName("PartitionPrunningDriver");
		job.setJarByClass(PartitionPruningOutputDriver.class);
		
		job.setMapperClass(RedisLastAccessOutputMapper.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, inputPath);
		
		job.setOutputFormatClass(RedisLastAccessOutputFormat.class);
		job.setOutputKeyClass(RedisKey.class);
		job.setOutputValueClass(Text.class);
		
		int code = job.waitForCompletion(true)? 0:2;
		System.exit(code);
	}
	
	static class RedisLastAccessOutputFormat extends OutputFormat<RedisKey, Text>{

		@Override
		public RecordWriter<RedisKey, Text> getRecordWriter(
				TaskAttemptContext context) throws IOException,
				InterruptedException {
			return null;
		}

		@Override
		public void checkOutputSpecs(JobContext context) throws IOException,
				InterruptedException {
			
		}

		@Override
		public OutputCommitter getOutputCommitter(TaskAttemptContext context)
				throws IOException, InterruptedException {
			return (new NullOutputFormat<Text,Text>()).getOutputCommitter(context);
		}
		
	}
	
	static class RedisLastAccessRecordWriter extends RecordWriter<RedisKey, Text>{
		
		private HashMap<Integer,Jedis> jedisMap = new HashMap<Integer,Jedis>();
		
		public RedisLastAccessRecordWriter(){
			int i = 0;
			for(String host: MRDPUtils.REDIS_INSTANCES.split(",")){
				Jedis jedis = new Jedis(host);
				jedis.connect();
				jedisMap.put(i, jedis);
				jedisMap.put(i+1, jedis);
				i+=2;
			}
		}
		
		@Override
		public void write(RedisKey key, Text value) throws IOException,
				InterruptedException {
			Jedis j = jedisMap.get(key.getLastAccessMonth());
//			j.hset(MONTH_FROM_INT, field, value)
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			for(Jedis jedis: jedisMap.values()){
				jedis.disconnect();
			}
		}
	}
	
	static class RedisLastAccessOutputMapper extends Mapper<Object,Text,RedisKey, Text>{
		
		private static final SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T' HH:mm:ss.SSS");
		private RedisKey outkey = new RedisKey();
		private Text outvalue = new Text();
		
		@Override
		protected void map(Object key, Text value,Context context)throws IOException, InterruptedException {
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			
			String userId = parsed.get("Id");
			String reputation = parsed.get("Reputation");
			String strDate = parsed.get("strDate");
			
			Calendar cal = Calendar.getInstance();
			try {
				cal.setTime(frmt.parse(strDate));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			outkey.setLastAccessMonth(cal.get(Calendar.MONTH));
			outkey.setField(new Text(userId));
			outvalue.set(reputation);
			
			context.write(outkey, outvalue);
		}
	}
	
}
