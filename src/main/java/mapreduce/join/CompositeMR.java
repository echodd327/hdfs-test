package mapreduce.join;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

/**
 * 组合连接
 */
public class CompositeMR {
	
	public static void main(String[] args) throws Exception {
		Path userPath = new Path(args[0]);
		Path commentPath = new Path(args[1]);
		Path outputDir = new Path(args[2]);
		String joinType = args[3];
		
		JobConf conf = new JobConf("CompositeJoin");
		conf.setJarByClass(CompositeMR.class);
		conf.setMapperClass(CompositeMapper.class);
		conf.setNumReduceTasks(0);
		conf.setInputFormat(CompositeInputFormat.class);
		conf.set("mapred.join.expr", CompositeInputFormat.compose(joinType, 
				KeyValueTextInputFormat.class, userPath, commentPath));
		
		TextOutputFormat.setOutputPath(conf, outputDir);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		RunningJob job = JobClient.runJob(conf);
		while(!job.isComplete()){
			Thread.sleep(1000);
		}
		System.exit(job.isSuccessful()?0:1);
	}
	static class CompositeMapper extends MapReduceBase implements Mapper<Text, TupleWritable, Text, Text>{

		@Override
		public void map(Text key, TupleWritable value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			 output.collect((Text)value.get(0), (Text)value.get(0));
		}
	}
}
