package mapreduce.util;

import java.util.HashMap;
import java.util.Map;

public class MRDPUtils {
	
	

	public static final String REDIS_INSTANCES = null;

	public static Map<String,String> transformXmlToMap(String xml){
		Map<String,String> map = new HashMap<String,String>();
		
		String[] tokens = xml.trim().substring(5, xml.trim().length()-3).split("\"");
		
		for(int i = 0; i < tokens.length ; i+=2){
			String key = tokens[i].trim();
			String value = tokens[i+1];
			
			map.put(key.substring(0, key.length()-1), value);
		}
		return map;
 	}
	
	public static void main(String[] args) {
		String xml = "<row Id=\"8189677\" PostId=\"6881722\" Text=\"Have you looked at Hadoop?\" CreationDate=\"2011-07-30T07:29:33.343\" UserId=\"831878\"/>";
		Map<String,String> map = transformXmlToMap(xml);
		System.out.println(map); 
		
		
		String[] result = "this is a test".split("\\s");
		for (int x=0; x<result.length; x++)
		System.out.println(result[x]);
	} 
}
