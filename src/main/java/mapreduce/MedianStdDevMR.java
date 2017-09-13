package mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import mapreduce.util.MRDPUtils;
import mapreduce.util.MedianStdDevTuple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 求数据集中位数，标准差
 *
 */
public class MedianStdDevMR {
	
	
	static class MedianStdDevMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
		
		private IntWritable outHour = new IntWritable();
		private IntWritable outCommentLength = new IntWritable();
		private Calendar cal = Calendar.getInstance();
		private static final SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T' HH:mm:ss.SSS");
		
		@Override
		protected void map(Object key, Text value,Context context)throws IOException, InterruptedException {
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			
			String strDate = parsed.get("CreationDate");
			String text = parsed.get("Text");
			
			try {
				Date creationDate = frmt.parse(strDate);
				cal.setTime(creationDate);
				outHour.set(cal.get(Calendar.HOUR_OF_DAY));
				outCommentLength.set(text.length());
				
				context.write(outHour, outCommentLength);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}
	static class MedianStdDevReducer extends Reducer<IntWritable, IntWritable, IntWritable, MedianStdDevTuple>{
		
		private MedianStdDevTuple result = new MedianStdDevTuple();
		private ArrayList<Float> commentlengths = new ArrayList<Float>();
		
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			
			float sum = 0;
			float count = 0;
			commentlengths.clear();
			for(IntWritable val : values){
				commentlengths.add((float)val.get());
				
				sum+= val.get();
				count++;
			}
			
			Collections.sort(commentlengths);
			
			//计算中位数，如果是奇数则取中间数，如果是偶数，则取中间两位数平均值
			if(count%2 == 0){
				result.setMedian((commentlengths.get((int) (count/2-1)) + commentlengths.get((int) (count/2)))/2.0f);
			}else{
				result.setMedian(commentlengths.get((int) (count/2)));
			}
			
			//求标准差
			float mean = sum/count;
			float sumOfSquares = 0.0f;
			for(Float f: commentlengths){
				sumOfSquares += (f-mean)*(f-mean);
			}
			result.setStdDev((float)Math.sqrt(sumOfSquares/count-1));
			context.write(key, result);
		}
	}
	
	
}
