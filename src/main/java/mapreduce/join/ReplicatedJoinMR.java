package mapreduce.join;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import mapreduce.util.MRDPUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

/**
 * 复制连接
 * 
 * 问题 : 给定一个小的用户集和大的评论集，通过用户数据丰富评论内容。 （小用户和评论内容)
 */
public class ReplicatedJoinMR {

	static class ReplicatedJoinMapper extends Mapper<Object, Text, Text, Text>{
		
		private HashMap<String,String> userIdToInfo = new HashMap<String,String>();
		
		private static final Text EMPTY_TEXT = new Text("");
		private String joinType = null;
		private Text outvalue = new Text();
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path p:files){
				BufferedReader rdr = new BufferedReader(
						new InputStreamReader(new GZIPInputStream(new FileInputStream(new File(p.toString())))));
				String line = null;
				while((line = rdr.readLine()) != null){
					Map<String,String> parsed = MRDPUtils.transformXmlToMap(line);
					String userId = parsed.get("Id");
					userIdToInfo.put(userId, line);
				}
			}
			joinType = context.getConfiguration().get("join.type");
		}
		
		@Override
		protected void map(Object key, Text value,Context context)
				throws IOException, InterruptedException {
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			
			String userId = parsed.get("UserId");
			String userInformation = userIdToInfo.get(userId);
			/**
			 * 输出评论内容, 用户详细信息
			 */
			if(userInformation != null){
				outvalue.set(userInformation);
				context.write(value, outvalue);
			}else if(joinType.equalsIgnoreCase("leftouter")){
				context.write(value, EMPTY_TEXT);
			}
			
		}
	}
	
	
}
