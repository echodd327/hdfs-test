package mapreduce.inout;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import redis.clients.jedis.Jedis;

/**
 * redis外部源输入
 *
 */
public class RedisInputDriver {
	
	public static final String REDIS_HOSTS_CONF = "mapred.redishashinputformat.hosts";
	public static final String REDIS_HASH_KEY_CONF = "mapred.redishashinputformat.key";
	public static void main(String[] args) {
		
	}
	
	static class RedisHashInputSplit  extends InputSplit implements Writable{
		private String location = null;
		private String hashKey = null;
		
		public RedisHashInputSplit(String location, String hashKey){
			this.location = location;
			this.hashKey = hashKey;
		}
		public String getLocation() {
			return location;
		}

		public void setLocation(String location) {
			this.location = location;
		}

		public String getHashKey() {
			return hashKey;
		}

		public void setHashKey(String hashKey) {
			this.hashKey = hashKey;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(this.location);
			out.writeUTF(this.hashKey);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.location = in.readUTF();
			this.hashKey = in.readUTF();
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[]{location};
		}
	}
	
	static class RedisHashInputFormat extends InputFormat<Text, Text>{
		
		public static void setRedisHosts(Job job,String hosts){
			job.getConfiguration().set(REDIS_HOSTS_CONF, hosts);
		}
		public static void setRedisHashKey(Job job,String hashKey){
			job.getConfiguration().set(REDIS_HASH_KEY_CONF, hashKey);
		}
		@Override
		public List<InputSplit> getSplits(JobContext context)
				throws IOException, InterruptedException {
			String hosts = context.getConfiguration().get(REDIS_HOSTS_CONF);
			if(hosts == null || hosts.isEmpty()){
				throw new IOException(REDIS_HOSTS_CONF+" is not set in configuration");
			}
			String hashKey = context.getConfiguration().get(REDIS_HASH_KEY_CONF);
			if(hashKey == null || hashKey.isEmpty()){
				throw new IOException(REDIS_HASH_KEY_CONF+" is not set in configuration");
			}
			
			List<InputSplit> inputSplits = new ArrayList<InputSplit>();
			for(String host: hosts.split(",")){
				inputSplits.add(new RedisHashInputSplit(host, hashKey));
			}
			return inputSplits;
		}

		@Override
		public RecordReader<Text, Text> createRecordReader(InputSplit split,
				TaskAttemptContext context) throws IOException,
				InterruptedException {
			return new RedisHashRecordReader();
		}
	}
	
	static class RedisHashRecordReader extends RecordReader<Text, Text>{
		
		private Text key = new Text();
		private Text value = new Text();
		private Iterator<Entry<String,String>> keyValueIterator = null;
		private Entry<String,String> currentEntry = null;
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			String host = split.getLocations()[0];
			String hashKey = ((RedisHashInputSplit)split).getHashKey();
			
			Jedis jedis = new Jedis(host);
			jedis.connect();
			jedis.getClient().setTimeoutInfinite();
			
			keyValueIterator = jedis.hgetAll(hashKey).entrySet().iterator();
			jedis.disconnect();
			
		}
		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}
		
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if(keyValueIterator.hasNext()){
				currentEntry = keyValueIterator.next();
				key.set(currentEntry.getKey());
				value.set(currentEntry.getValue());
				return true;
			}else{
				return false;
			}
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
