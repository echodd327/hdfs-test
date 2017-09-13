package mapreduce.filter;

import java.io.IOException;
import java.util.Map;

import mapreduce.util.MRDPUtils;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 去重 
 *
 */
public class DistinctUserMR {
	
	static class DistinctUserMapper extends Mapper<Object, Text, Text, NullWritable>{
		
		private Text outUserId = new Text();
		
		@Override
		protected void map(Object key, Text value,Context context)
				throws IOException, InterruptedException {
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			String userId = parsed.get("UserId");
			
			outUserId.set(userId);
			context.write(outUserId, NullWritable.get());
		}
	}
	
	static class DistinctUserReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
}
