package mapreduce.metamode;

import java.io.IOException;
import java.util.Map;

import mapreduce.util.MRDPUtils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 *  链折叠
 *  
 */
public class ChainMapperDriver {
	
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
}
