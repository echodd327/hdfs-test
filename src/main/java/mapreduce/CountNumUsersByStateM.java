package mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import mapreduce.util.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * 计数器实现
 *
 *  用例: 计算每个州的用户数
 */
public class CountNumUsersByStateM {

	static class CountNumUsersByStateMapper extends
			Mapper<Object, Text, NullWritable, NullWritable> {

		public static final String STATE_COUNTER_GROUP = "State";
		public static final String UNKNOWN_COUNTER = "Unknown";
		public static final String NULL_OF_EMPTY_COUNTER = "Null of Empty";

		private String[] statesArray = new String[] { "AL", "AK", "AS", "AZ",
				"AR", "CA", "CO", "CT", "DE", "MS", "FM", "FL", "GA", "GU",
				"HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MH",
				"MD", "MA", "MI", "MN", "MO", "NE", "NV", "NH", "NJ", "NM",
				"NY", "NC", "ND", "MP", "OH", "OK", "OR", "PW", "PA", "PR",
				"RI", "SC", "SD", "MT", "TN", "TX", "UT", "VT", "VI", "VA",
				"WA", "WV", "WI", "WY",

		};
		private HashSet<String> states = new HashSet<String>(Arrays.asList(statesArray));

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			String location = parsed.get("Location");
			
			if(location != null && !location.isEmpty()){
				String[] tokens = location.toLowerCase().split("\\s");
				boolean unkown = true;
				for(String state: tokens){
					if(states.contains(state)){
						context.getCounter(STATE_COUNTER_GROUP, state).increment(1);
						unkown = false;
						break;
					}
				}
				if(unkown){
					context.getCounter(STATE_COUNTER_GROUP, UNKNOWN_COUNTER).increment(1);
				}
			}else{
				context.getCounter(STATE_COUNTER_GROUP, NULL_OF_EMPTY_COUNTER).increment(1);
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf  = new Configuration();
		conf.set("fs.defaultFS", "hdfs://linux.liuhui01.com:8020");
		System.setProperty("hadoop.home.dir", "F:\\software\\hadoop-2.5.0-cdh5.3.6");
		
		Job job = Job.getInstance(conf);
		
		job.setJobName("wiki");
		job.setJarByClass(CountNumUsersByStateM.class);
		
		job.setMapperClass(CountNumUsersByStateMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path("/user/hadoop/mapreduce/input/wc2.input.txt"));
		
		
		boolean success = job.waitForCompletion(true);
		if(success){
			for(Counter counter : job.getCounters().getGroup(CountNumUsersByStateMapper.STATE_COUNTER_GROUP)){
				System.out.println(counter.getDisplayName()+"\t"+counter.getValue());
			}
		}
	}
}
