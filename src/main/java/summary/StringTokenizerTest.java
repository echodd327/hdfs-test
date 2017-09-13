package summary;

import java.util.StringTokenizer;

public class StringTokenizerTest {

	public static void main(String[] args) {
		String str = "1981	31	32	32	32	33	34	35	36	36	34	34	34	34";
		StringTokenizer stz = new StringTokenizer(str, "\t");
		String year =stz.nextToken();
		
		String lastTokens = null;
		while(stz.hasMoreTokens()){
			lastTokens = stz.nextToken();
		}
		System.out.println(year+","+lastTokens);
	}

}
