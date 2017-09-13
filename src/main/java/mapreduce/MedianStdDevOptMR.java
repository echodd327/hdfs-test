package mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import mapreduce.util.MRDPUtils;
import mapreduce.util.MedianStdDevTuple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 *求数据集中位数，标准差优化
 */
public class MedianStdDevOptMR {
	
	
	static class MedianStdDevOptMapper extends Mapper<Object, Text, IntWritable, SortedMapWritable>{
		private static final SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd'T' HH:mm:ss.SSS");
		
		private IntWritable commentLength = new IntWritable();
		private static final LongWritable ONE = new LongWritable(1);
		private IntWritable outHour = new IntWritable();
		private  Calendar cal = Calendar.getInstance();
		@Override
		protected void map(Object key, Text value,Context context)
				throws IOException, InterruptedException {
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			String strDate = parsed.get("CreationDate");
			String text = parsed.get("Text");
			
			try {
				Date creationDate = frmt.parse(strDate);
				cal.setTime(creationDate);
				outHour.set(cal.get(Calendar.HOUR_OF_DAY));
				commentLength.set(text.length());
				
				SortedMapWritable outCommentLength = new SortedMapWritable();
				outCommentLength.put(commentLength, ONE);
				context.write(outHour, outCommentLength);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}
	
	static class MedianStdDevOptReducer extends Reducer<IntWritable, SortedMapWritable, IntWritable, MedianStdDevTuple>{
		
		private MedianStdDevTuple result = new MedianStdDevTuple();
		private TreeMap<Integer,Long> commentLengthCounts = new TreeMap<Integer,Long>();
		
		@Override
		protected void reduce(IntWritable key,Iterable<SortedMapWritable> values,Context context)
				throws IOException, InterruptedException {
			float sum = 0;
			long totalComments = 0;
			commentLengthCounts.clear();
			result.setMedian(0);
			result.setStdDev(0);
			
			for(SortedMapWritable val: values){
				 for(Entry<WritableComparable, Writable> entry: val.entrySet()){
					 int length = ((IntWritable)entry.getKey()).get();
					 long count = ((LongWritable)entry.getValue()).get();
					 
					 totalComments+= count;
					 sum+= length*count;
					 
					 Long storedCount = commentLengthCounts.get(length);
					 if(storedCount == null){
						 commentLengthCounts.put(length, count);
					 }else{
						 commentLengthCounts.put(length, storedCount+count);
					 }
				 }
			}
			
			//计算中位数，如果是奇数则取中间数，如果是偶数，则取中间两位数平均值
			long mediaIndex = totalComments/2L;
			long previousComments = 0;
			int prevKey = 0;
			long comments = 0;
			for(Entry<Integer, Long> entry: commentLengthCounts.entrySet()){
				comments = previousComments + entry.getValue();
				
				if(previousComments <= mediaIndex && mediaIndex < comments){
					if(totalComments %2 == 0 && previousComments == mediaIndex){
						result.setMedian((entry.getKey()+prevKey)/2.0f);
					}else{
						result.setMedian(entry.getKey());
					}
				}
				previousComments = comments;
				prevKey = entry.getKey();
			}
			
			float mean = sum/totalComments;
			float sumOfSquares = 0.0f;
			
			for(Entry<Integer, Long> entry: commentLengthCounts.entrySet()){
				sumOfSquares += (entry.getKey()-mean)*(entry.getKey()-mean)*entry.getValue();
			}
			
			result.setStdDev((float)Math.sqrt(sumOfSquares/(totalComments-1)));
			context.write(key, result);
			
		}
	}
	
	static class MedianStdDevOptCombiner extends Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable>{
		
		
		@Override
		protected void reduce(IntWritable key,Iterable<SortedMapWritable> values,Context context)throws IOException, InterruptedException {
			SortedMapWritable outValue = new SortedMapWritable();
			
			for(SortedMapWritable v: values){
				 for(Entry<WritableComparable, Writable> entry : v.entrySet()){
					 LongWritable count = (LongWritable)outValue.get(entry.getKey());
					 
					 if(count != null){
						 count.set(count.get()+((LongWritable)entry.getValue()).get());
						 outValue.put(entry.getKey(), count);
					 }else{
						 outValue.put(entry.getKey(), new LongWritable(((LongWritable)entry.getValue()).get()));
					 }
				 }
			}
		}
	}
}
