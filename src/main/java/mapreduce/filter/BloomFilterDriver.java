package mapreduce.filter;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;

import mapreduce.util.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
/**
 *  预先使用词集产生一个布隆过滤器
 *
 */
public class BloomFilterDriver {
	
	private static int getOptimalK(int numMembers, int vectorSize) {
		return 0;
	}

	private static int getOptimalBloomFilterSize(int numMembers,
			float falsePosRate) {
		return 0;
	}
	
	
	public static void main(String[] args) throws IOException {
		Path inputFile = new Path(args[0]); //gzip输入文件或者gzip目录
		int numMembers = Integer.valueOf(args[1]);  //文件中元素数目
		float falsePosRate = Float.parseFloat(args[2]); //一个容忍错判率
		Path bFile = new Path(args[3]);  //输出文件名
		
		//最优化向量大小
		int vectorSize = getOptimalBloomFilterSize(numMembers,falsePosRate);
		//最优化散列函数数目
		int nbHash = getOptimalK(numMembers, vectorSize);
		
		BloomFilter filter = new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);
		
		String line = null;
		int numElements = 0;
		FileSystem fs = FileSystem.get(new Configuration());
		
		for(FileStatus status : fs.listStatus(inputFile)){
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new GZIPInputStream(fs.open(status.getPath()))));
			
			if((line =rdr.readLine())!= null){
				filter.add(new Key(line.getBytes()));
				++numElements;
			}
			rdr.close();
		}
		
		FSDataOutputStream strm = fs.create(bFile);
		filter.write(strm);
		strm.flush();
		strm.close();
		
		System.exit(0);
		
	}

	static class BloomFilterMapper extends Mapper<Object, Text, Text, NullWritable>{
		
		private BloomFilter filter = new BloomFilter();
		
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)throws IOException, InterruptedException {
			URI[] files = DistributedCache.getCacheFiles(context.getConfiguration());
			
			DataInputStream strm = new DataInputStream(new FileInputStream(files[0].getPath()));
			filter.readFields(strm);
			strm.close();
		
		}
		@Override
		protected void map(Object key, Text value,Context context)throws IOException, InterruptedException {
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			String comment = parsed.get("Text");
			
			StringTokenizer tokenizer = new StringTokenizer(comment);
			while(tokenizer.hasMoreElements()){
				String word = tokenizer.nextToken();
				if(filter.membershipTest(new Key(word.getBytes()))){
					context.write(value, NullWritable.get());
					break;
				}
			}
		}
	}
}
