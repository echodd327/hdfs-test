package mapreduce.inout;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * 
 *  按照最后访问时间查询用户声望
 */
public class PartitionPrunningInputDriver {
	
	public static void main(String[] args) {
		
	}
	
	static class RedisLastAccessInputSplit extends InputSplit implements Writable{
		
		private String location = null;
		private List<String> hashKeys = new ArrayList<String>();
		
		@Override
		public void write(DataOutput out) throws IOException {
			
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			
		}
		@Override
		public long getLength() throws IOException, InterruptedException {
			return 0;
		}
		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return null;
		}
	}
}
