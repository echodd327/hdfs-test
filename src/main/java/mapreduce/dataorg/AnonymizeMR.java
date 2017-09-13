package mapreduce.dataorg;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import mapreduce.util.MRDPUtils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 混排
 * 为了对评论匿名化处理，除去用户ID和行ID，并将日期和时间阶段只有日期，对数据进行混排。
 */
public class AnonymizeMR {
		
	
	static class AnonymizeMapper extends Mapper<Object, Text, IntWritable, Text>{
		
		private IntWritable outKey = new IntWritable();
		private Text outValue = new Text();
		private Random random = new Random();
		
		@Override
		protected void map(Object key, Text value,Context context)throws IOException, InterruptedException {
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			
			if(parsed.size()>0){
				StringBuffer sbr = new StringBuffer();
				sbr.append("<row ");
				
				for(Entry<String, String> entry : parsed.entrySet()){
					if(entry.getKey().equals("UserId") || entry.getKey().equals("Id")){
						
					}else if(entry.getKey().equals("CreationDate")){
						sbr.append(entry.getKey()+"=\""+ entry.getValue().substring(0, entry.getValue().indexOf("T"))+"\" ");	
					}else{
						sbr.append(entry.getKey()+"=\""+ entry.getValue()+"\"");
					}
				}
				sbr.append("/>");
				
				outKey.set(random.nextInt());
				outValue.set(sbr.toString());
				context.write(outKey, outValue);
			}
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
