package mapreduce.inout;

import java.io.IOException;
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

import redis.clients.jedis.Jedis;

/**
 * 外部源输出到redis
 *
 */
public class RedisOutputDriver {
	
	public static final String REDIS_HOSTS_CONF = "mapred.redishashoutputformat.hosts";
	public static final String REDIS_HASH_KEY_CONF = "mapred.redishashinputformat.key";
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Path inputPath = new Path(args[0]);
		String hosts = args[1];
		String hashKey = args[2];
		
		Job job = Job.getInstance();
		job.setJobName("RedisOutputDriver");
		job.setJarByClass(RedisOutputDriver.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, inputPath);
		
		job.setOutputFormatClass(RedisHashOutputFormat.class);
		RedisHashOutputFormat.setRedisHashKey(job, hashKey);
		RedisHashOutputFormat.setRedisHosts(job, hosts);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		int code = job.waitForCompletion(true)?0:2;
		System.exit(code);
		
	}
	
	static class RedisHashOutputFormat extends OutputFormat<Text,Text>{
		
		public static void setRedisHosts(Job job,String hosts){
			job.getConfiguration().set(REDIS_HOSTS_CONF, hosts);
		}
		public static void setRedisHashKey(Job job,String hashKey){
			job.getConfiguration().set(REDIS_HASH_KEY_CONF, hashKey);
		}
		
		@Override
		public RecordWriter<Text, Text> getRecordWriter(
				TaskAttemptContext context) throws IOException,
				InterruptedException {
			RedishashRecordWriter writer = new RedishashRecordWriter(
					context.getConfiguration().get(REDIS_HASH_KEY_CONF), 
					context.getConfiguration().get(REDIS_HOSTS_CONF));
			return writer;
		}

		@Override
		public void checkOutputSpecs(JobContext context) throws IOException,
				InterruptedException {
			String hosts = context.getConfiguration().get(REDIS_HOSTS_CONF);
			if(hosts == null || hosts.isEmpty()){
				throw new IOException(REDIS_HOSTS_CONF+" is not set in configuration");
			}
			String hashKey = context.getConfiguration().get(REDIS_HASH_KEY_CONF);
			if(hashKey == null || hashKey.isEmpty()){
				throw new IOException(REDIS_HASH_KEY_CONF+" is not set in configuration");
			}
		}

		@Override
		public OutputCommitter getOutputCommitter(TaskAttemptContext context)
				throws IOException, InterruptedException {
			return (new NullOutputFormat<Text,Text>()).getOutputCommitter(context);
		}
		
	}
	
	static class RedishashRecordWriter extends RecordWriter<Text,Text>{
		private HashMap<Integer,Jedis> jedisMap = new HashMap<Integer,Jedis>();
		private String hashKey = null;
		
		public RedishashRecordWriter(String hashKey,String hosts){
			this.hashKey = hashKey;
			int i = 0;
			for(String host: hosts.split(",")){
				++i;
				Jedis jedis = new Jedis(hosts);
				jedisMap.put(i, jedis);
			}
		}
		
		@Override
		public void write(Text key, Text value) throws IOException,
				InterruptedException {
			Jedis j = jedisMap.get(Math.abs(key.hashCode()%jedisMap.size()));
			j.hset(hashKey, key.toString(), value.toString());
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			for(Jedis jedis: jedisMap.values()){
				 jedis.disconnect();
			}
		}
		
	}
	
	static class RedisOutputMapper extends Mapper<Object,Text,Text,Text>{
		
		private Text outkey = new Text();
		private Text outvalue = new Text();
		
		@Override
		protected void map(Object key, Text value,Context context)throws IOException, InterruptedException {
			 Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			 
			 String userId = parsed.get("Id");
			 String reputation = parsed.get("reputation");
			 
			 outkey.set(userId);
			 outvalue.set(reputation);
			 context.write(outkey, outvalue);
		}
		
	}
	
}
