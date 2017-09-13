package mapreduce.inout;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class RedisKey implements WritableComparable<RedisKey>{
	
	private int lastAccessMonth = 0;
	private Text field = new Text();
	public int getLastAccessMonth() {
		return lastAccessMonth;
	}

	public void setLastAccessMonth(int lastAccessMonth) {
		this.lastAccessMonth = lastAccessMonth;
	}

	public Text getField() {
		return field;
	}

	public void setField(Text field) {
		this.field = field;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(lastAccessMonth);
		this.field.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.lastAccessMonth = in.readInt();
		 this.field.readFields(in);
	}

	@Override
	public int compareTo(RedisKey o) {
		if(this.lastAccessMonth == o.lastAccessMonth){
			return this.field.compareTo(o.getField());
		}else{
			return this.getLastAccessMonth() > o.getLastAccessMonth() ?1: -1;
		}
	}
	
	
	

}
