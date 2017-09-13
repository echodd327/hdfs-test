package mapreduce.dataorg;

import java.io.IOException;
import java.util.Map;

import mapreduce.util.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
 * 
 *  分箱
 *
 *  问题: 给定一组stackOverflow的帖子，按照标签(hadoop,pig,hive和hbase) 将帖子分别放入到4个箱子中去，另外，
 *  在文本或者标题提及的hadoop帖子创建一个单独的箱子
 *  
 *  分析:
 *  帖子标签-->hadoop,pig,hive,hbase
 *  文本、标题 涉及hadooo--> 单独箱子
 *  
 *  入参：帖子
 *  
 */
public class BinningM {
	 
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException  {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		System.setProperty("hadoop.home.dir", "F:\\software\\hadoop-2.5.0-cdh5.3.6");
		
		Job job = Job.getInstance(conf);
		job.setJobName("BinningM");
		job.setJarByClass(BinningM.class);
		
		job.setMapperClass(BinningMapper.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		
		MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, 
				Text.class, NullWritable.class);
		MultipleOutputs.setCountersEnabled(job, true);
		 
		FileInputFormat.addInputPath(job, new Path("/user/hdfs/binning.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/user/hdfs/output"));
		
		boolean isSuccess = job.waitForCompletion(true);
		if(isSuccess){
			System.out.println("任务处理成功");
		}else{
			System.exit(0);
		}
	}
	static  class BinningMapper extends Mapper<Object, Text, Text, NullWritable>{
		
		private MultipleOutputs<Text, NullWritable> mos = null;
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			mos = new MultipleOutputs<>(context);
		}
		@Override
		protected void map(Object key, Text value,Context context)
				throws IOException, InterruptedException {
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			String rawtags = parsed.get("Tags");
			
			String tagTokens[] = rawtags.split("><");
			for(String tagToken : tagTokens){
				String groomed = tagToken.replaceAll(">|<", "");
				
				if(groomed.equalsIgnoreCase("hadoop")){
					mos.write("bins", value, NullWritable.get(), "hadoop-tag");
				}
				if(groomed.equalsIgnoreCase("pig")){
					mos.write("bins", value, NullWritable.get(), "pig-tag");
				}
				if(groomed.equalsIgnoreCase("hive")){
					mos.write("bins", value, NullWritable.get(), "hive-tag");
				}
				if(groomed.equalsIgnoreCase("hbase")){
					mos.write("bins", value, NullWritable.get(), "hbase-tag");
				}
			}
			
			String post = parsed.get("Body");
			if(post.equalsIgnoreCase("hadoop")){
				mos.write("bins", value, NullWritable.get(), "hadoop-post");
			}
		}
		@Override
		protected void cleanup(Context context)throws IOException, InterruptedException {
			mos.close();
		}
	}
}
