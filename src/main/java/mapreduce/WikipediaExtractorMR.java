package mapreduce;

import java.io.IOException;
import java.util.Map;

import mapreduce.util.MRDPUtils;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



/**
 * 倒排索引,所有包含维基百科URL的帖子的ID
 * combiner做预连接工作
 * @author xiu
 *
 */
public class WikipediaExtractorMR {
		
	static class WikipediaExtractor extends Mapper<Object, Text, Text, Text>{
		
		private Text link = new Text();
		private Text outKey = new Text();
		@Override
		protected void map(Object key, Text value,Context context)
				throws IOException, InterruptedException {
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			
			String text = parsed.get("Body");
			String posttype = parsed.get("PostTypeId");
			String rowid = parsed.get("Id");
			
			if(text == null || (posttype != null && posttype.equals("1"))){
				return;
			}
			
			text = StringEscapeUtils.unescapeHtml(text.toLowerCase());
			
			link.set(getWikiPidURL(text));
			outKey.set(rowid);
			context.write(link, outKey);
			
		}
		private String getWikiPidURL(String text) {
			return null;
		}
	}
	
	static class Concatenator extends Reducer<Text,Text,Text,Text>{
		
		private Text result = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			boolean first = true;
			for(Text id: values){
				if(first){
					first = false;
				}else{
					sb.append(" ");
				}
				sb.append(id.toString());
			}
			result.set(sb.toString());
			context.write(key, result);
		}
	}
	
}
