package mapreduce.dataorg;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import mapreduce.util.MRDPUtils;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 分区模式
 *
 */
public class LastAccessDatePartitionMR {
	
	static class LastAccessDateMapper extends Mapper<Object, Text, IntWritable, Text>{
		
		private static final SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T' HH:mm:ss.SSS");
		
		@Override
		protected void map(Object key, Text value,Context context)throws IOException, InterruptedException {
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			
			String strDate = parsed.get("LastAccessDate");
			
			Calendar cal = Calendar.getInstance();
			try {
				cal.setTime(frmt.parse(strDate));
				int year = cal.get(Calendar.YEAR);
				context.write(new IntWritable(year), value);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
		}
	}
	
	static class LastAccessDatePartitioner extends Partitioner<IntWritable, Text> implements Configurable{

		private Configuration conf = null;
		private int minLastAccessDateYear = 0;
		private static final String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.year";
		
		@Override
		public void setConf(Configuration conf) {
			this.conf = conf;
			minLastAccessDateYear = conf.getInt(MIN_LAST_ACCESS_DATE_YEAR, 0);
		}

		@Override
		public Configuration getConf() {
			return this.conf;
		}

		@Override
		public int getPartition(IntWritable key, Text value, int numPartitions) {
			 return key.get() - minLastAccessDateYear;
		}
		
	}
	
	static class ValueReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			for(Text t: values){
				context.write(t, NullWritable.get());
			}
		}
	}
	
	
}
