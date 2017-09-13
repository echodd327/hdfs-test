package mapreduce.filter;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import mapreduce.util.MRDPUtils;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * TOP 10
 *
 */
public class TopTenMR {
	
	static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text>{
		
		private TreeMap<Integer,Text> repToRecordMap = new TreeMap<Integer,Text>();
		
		@Override
		protected void map(Object key, Text value,Context context)
				throws IOException, InterruptedException {
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			
			String userId = parsed.get("Id");
			String reputation = parsed.get("Reputation");
			
			repToRecordMap.put(new Integer(reputation), new Text(value));
			
			if(repToRecordMap.size()>10){
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}
		
		@Override
		protected void cleanup(Context context)throws IOException, InterruptedException {
			 for(Text t : repToRecordMap.values()){
				 context.write(NullWritable.get(), t);
			 }
		}
	}
	
	static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text>{
		
		private TreeMap<Integer,Text> repToRecordMap = new TreeMap<Integer,Text>();
		
		@Override
		protected void reduce(NullWritable key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			 
			for(Text value : values){
				Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
				
				repToRecordMap.put(new Integer(parsed.get("Reputation")), new Text(value));
				
				if(repToRecordMap.size()>10){
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}
			for(Text t: repToRecordMap.descendingMap().values()){
				context.write(NullWritable.get(), t);
			}
			
		}
	}
	
	
}
