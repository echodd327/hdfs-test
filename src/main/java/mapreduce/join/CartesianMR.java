package mapreduce.join;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.CompositeInputSplit;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 笛卡尔积
 *
 */
public class CartesianMR {
	
	public static final String LEFT_INPUT_FORMAT = "cart.left.inputformat";
	public static final String LEFT_INPUT_PATH = "cart.left.path";
	public static final String RIGHT_INPUT_FORMAT = "cart.right.inputformat";
	public static final String RIGHT_INPUT_PATH = "cart.right.path";
	
	static class CartesianInputFormat extends FileInputFormat{

		public static void setLeftInputInfo(JobConf job,Class<? extends FileInputFormat> inputFormat,
				String inputPath){
			job.set(LEFT_INPUT_FORMAT, inputFormat.getCanonicalName());
			job.set(LEFT_INPUT_PATH, inputPath);
		}
		public static void setRightInputInfo(JobConf job,Class<? extends FileInputFormat> inputFormat,
				String inputPath){
			job.set(RIGHT_INPUT_FORMAT, inputFormat.getCanonicalName());
			job.set(RIGHT_INPUT_PATH, inputPath);
		}
		@Override
		public InputSplit[] getSplits(JobConf conf, int numSplits)throws IOException {
			try {
				InputSplit[] leftSplits = getInputSplits(conf, conf.get(LEFT_INPUT_FORMAT), LEFT_INPUT_PATH, numSplits);
				InputSplit[] rightSplits = getInputSplits(conf, conf.get(RIGHT_INPUT_FORMAT), RIGHT_INPUT_PATH, numSplits);
				
				CompositeInputSplit[] returnSplits = new CompositeInputSplit[leftSplits.length + rightSplits.length];
				int i = 0;
				for(InputSplit left: leftSplits){
					for(InputSplit right: rightSplits){
						returnSplits[i] = new CompositeInputSplit(2);
						returnSplits[i].add(left);
						returnSplits[i].add(right);
						++i;
					}
				}
				return returnSplits;
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			return null;
		}
		
		@Override
		public RecordReader getRecordReader(InputSplit split, JobConf job,
				Reporter reporter) throws IOException {
			try {
				return  new CartesianRecordReader((CompositeInputSplit)split,job,reporter);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			return null;
		}
		
		private InputSplit[] getInputSplits(JobConf conf,String InputFormatClass,String inputPath,int numSplits) 
				throws ClassNotFoundException, IOException{
			FileInputFormat inputFormat = (FileInputFormat) ReflectionUtils.newInstance(Class.forName(InputFormatClass), conf);
			
			inputFormat.setInputPaths(conf, inputPath);
			
			return inputFormat.getSplits(conf, numSplits);
		}
		
	}
	
	static class CartesianRecordReader<K1,V1,K2,V2> implements RecordReader<Text, Text>{
		private RecordReader leftRR = null,rightRR = null;
		
		private FileInputFormat rightFIF;
		private JobConf rightConf;
		private InputSplit rightIS;
		private Reporter rightReporter;
		
		private K1 lkey;
		private V1 lvalue;
		private K2 rkey;
		private V2 rvalue;
		private boolean getToNextLeft = true, alldone = false;
		
		public CartesianRecordReader(CompositeInputSplit split,JobConf conf,Reporter reporter) throws ClassNotFoundException, IOException{
			this.rightConf = conf;
			this.rightIS = split.get(1);
			this.rightReporter = reporter;
			
			FileInputFormat leftFIF = (FileInputFormat) ReflectionUtils.
					newInstance(Class.forName(conf.get(LEFT_INPUT_FORMAT)), conf);
			
			leftRR = leftFIF.getRecordReader(split.get(0), conf, reporter);
			
			rightFIF = (FileInputFormat) ReflectionUtils.newInstance(Class.forName(conf.get(RIGHT_INPUT_FORMAT)), conf);
			
			rightRR = rightFIF.getRecordReader(rightIS, conf, reporter);
			
			lkey = (K1) this.leftRR.createKey();
			lvalue = (V1) this.leftRR.createKey();
			
			rkey = (K2) this.rightRR.createKey();
			rvalue = (V2) this.rightRR.createValue();
		}
		
		@Override
		public boolean next(Text key, Text value) throws IOException {
			do{
			   if(getToNextLeft){
				   if(!leftRR.next(lkey, lvalue)){
					   alldone = true;
					   break;
				   }else{
					   key.set(lvalue.toString());
					   getToNextLeft = alldone =false;
					   this.rightRR = this.rightFIF.getRecordReader(rightIS, this.rightConf, rightReporter);
				   }
				   
				   if(rightRR.next(rkey, rvalue)){
					   value.set(value.toString());
				   }else{
					   getToNextLeft = true;
				   }
			   }
			}while(getToNextLeft);
			
			return !alldone;
		}

		@Override
		public Text createKey() {
			return null;
		}

		@Override
		public Text createValue() {
			return null;
		}

		@Override
		public long getPos() throws IOException {
			return 0;
		}

		@Override
		public void close() throws IOException {
			
		}

		@Override
		public float getProgress() throws IOException {
			return 0;
		}
		
	}
	
}
