package mapreduce.metamode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import mapreduce.util.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


/**
 * 作业链
 * 
 * 用例：输出一份用户列表，其中包含用户ID,用户发帖数，声望值， 将用户分为两部分，一部分在平均值以下，一部分在平均值以上
 * 例子中用到4种模式： 数值概要，计数，分箱以及复制连接。
 * 
 *  1. 第一个作业mapper和reducer,计数器计算帖子数和用户数 ，输出（用户ID,帖子数），得出平均帖子数：用户ID数/帖子数
 *  2. 第二个作业mapepr和reducer,用MultipartOutputs对象，分箱输出不同的目录中。
 */
public class JobChainingDriver {
	
public static final String AVERAGE_CALC_GROUP = "";
	
	static class UserIdCountMapper extends Mapper<Object,Text,Text,LongWritable>{
		
		public static final String RECORDS_COUNTER_NAME = "Records";
		private static final LongWritable ONE = new LongWritable(1);
		private Text outkey = new Text();
		
		@Override
		protected void map(Object key, Text value,Context context)
				throws IOException, InterruptedException {
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			
			String userId = parsed.get("OwnerUserId");
			if(userId != null){
				outkey.set(userId);
				context.write(outkey, ONE);
				context.getCounter(AVERAGE_CALC_GROUP, RECORDS_COUNTER_NAME).increment(1);
			}
		}
	}
	
	static class UserIdSumReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		
		public static final String USERS_COUNTER_NAME = "Users";
		
		private LongWritable outvalue = new LongWritable();
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,Context context)
				throws IOException, InterruptedException {
		  
			context.getCounter(AVERAGE_CALC_GROUP, USERS_COUNTER_NAME).increment(1);
			
			long sum = 0;
			for(LongWritable val: values){
				sum += val.get();
			}
			outvalue.set(sum);
			context.write(key, outvalue); //输出 用户ID,帖子数
			 
		}
	}
	
	static class UserIdBinningMapper extends Mapper<Object,Text,Text,Text>{
		
		private MultipleOutputs<Text,Text> mos;
		private Map<String,String> userIdReputation = new HashMap<String,String>();
		private double average = 0.0;
		
		public static final String AVERAGE_POSTS_PER_USER = "avg.post.per.user";
		private static final String MULTIPLE_OUTPUTS_BELOW_NAME = "";
		private static final String MULTIPLE_OUTPUTS_ABOVE_NAME = "";
		
		private Text outkey = new Text();
		private Text outvalue = new Text();
		
		public static void setAveragePostsPerUser(Job job,double avg){
			job.getConfiguration().set(AVERAGE_POSTS_PER_USER, Double.toString(avg));
		}
		public static double getAveragePostsPerUser(Configuration conf){
			return Double.parseDouble(conf.get(AVERAGE_POSTS_PER_USER));
		}
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			average = this.getAveragePostsPerUser(context.getConfiguration());
			
			mos = new MultipleOutputs<Text,Text>(context);
			Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path path: paths){
				BufferedReader br = new BufferedReader(new InputStreamReader(
						new GZIPInputStream(new FileInputStream(new File(path.toString())))));
				String line = "";
				while((line = br.readLine())!= null){
					Map<String,String> parsed = MRDPUtils.transformXmlToMap(line);
					userIdReputation.put(parsed.get("Id"), parsed.get("Reputation"));
				}
				
			}
		}
		// 用户ID  帖子数
		@Override
		protected void map(Object key, Text value,Context context)throws IOException, InterruptedException {
			String tokens[] = value.toString().split("\t");
			String userId = tokens[0];
			int posts = Integer.valueOf(tokens[1]);
			String reputation = userIdReputation.get(userId);
			
			outkey.set(userId);
			outvalue.set(posts+"\t"+reputation);
			if((double)posts > average){
				mos.write(MULTIPLE_OUTPUTS_ABOVE_NAME, outkey, outvalue, MULTIPLE_OUTPUTS_ABOVE_NAME+"/part");
			}else{
				mos.write(MULTIPLE_OUTPUTS_BELOW_NAME, outkey, outvalue, MULTIPLE_OUTPUTS_BELOW_NAME+"/part");
			}
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			mos.close();
		}
	}
	
}
