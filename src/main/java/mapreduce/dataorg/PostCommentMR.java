package mapreduce.dataorg;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import mapreduce.util.MRDPUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * 
 * 分层模式
 */
public class PostCommentMR {
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("postcommentHierarchy");
		job.setJarByClass(PostCommentMR.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PostMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CommentMapper.class);
		job.setReducerClass(PostCommentHierarchyReducer.class);
		
		job.setReducerClass(PostCommentHierarchyReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		boolean isSuccess = job.waitForCompletion(true);
		if(isSuccess){
			System.out.println("任务处理成功");
		}else{
			System.exit(0);
		}
		
	}
	static class PostMapper extends Mapper<Object, Text, Text, Text>{
		private Text outkey = new Text();
		private Text outvalue = new Text();
		
		@Override
		protected void map(Object key, Text value,Context context)
				throws IOException, InterruptedException {
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			
			outkey.set(parsed.get("Id"));
			outvalue.set("P"+value.toString());
			context.write(outkey, outvalue);
		}
	}
	
	
	static class CommentMapper extends Mapper<Object, Text, Text, Text>{
		private Text outkey = new Text();
		private Text outvalue = new Text();
		
		@Override
		protected void map(Object key, Text value,Context context)
				throws IOException, InterruptedException {
			Map<String,String> parsed = MRDPUtils.transformXmlToMap(value.toString());	
			outkey.set(parsed.get("PostId"));
			outvalue.set("C"+value.toString());
			context.write(outkey, outvalue);
		}
	}
	/**
	 * postcomment分层
	 */
	static class PostCommentHierarchyReducer extends Reducer<Text,Text, Text, NullWritable>{
		
		private ArrayList<String> comments = new ArrayList<String>();
		private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		private String post = null;
	
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException {
			post = null;
			comments.clear();
			
			for(Text t: values){
				if(t.charAt(0) == 'P'){
					post = t.toString().substring(0, t.toString().length()).trim();
				}else{
					comments.add(t.toString().substring(0, t.toString().length()).trim());
				}
			}
			if(post != null){
				String postWithCommentChildren;
				try {
					postWithCommentChildren = nestElement(post,comments);
					context.write(new Text(postWithCommentChildren), NullWritable.get());
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}
		}

		private String nestElement(String post, ArrayList<String> comments) throws Exception {
			DocumentBuilder bldr = dbf.newDocumentBuilder();
			Document doc = bldr.newDocument();
			
			Element postEle = this.getXmlElementFromString(post);
			Element toAddPostEl = doc.createElement("post");
			this.copyAttributesToElement(postEle.getAttributes(), toAddPostEl);
			
			for(String comment: comments){
				Element commentEle = this.getXmlElementFromString(comment);
				Element toAddCommentEl = doc.createElement("commnets");
				
				this.copyAttributesToElement(commentEle.getAttributes(),toAddCommentEl);
				
				toAddPostEl.appendChild(toAddCommentEl);
			}
			doc.appendChild(toAddPostEl);
			
			return transfromDocumentToString(doc);
		}
		
		private Element getXmlElementFromString(String xml) throws ParserConfigurationException, SAXException, IOException{
			DocumentBuilder bldr = dbf.newDocumentBuilder();
			return bldr.parse(new InputSource(new StringReader(xml))).getDocumentElement();
		}
		
		private void copyAttributesToElement(NamedNodeMap attributes, Element element){
			for(int i = 0; i < attributes.getLength(); i++){
				Attr toCopy = (Attr)attributes.item(i);
				element.setAttribute(toCopy.getName(), toCopy.getValue());
			}
		}
		
		private String transfromDocumentToString(Document doc) throws TransformerException{
			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer transformer = tf.newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			StringWriter writer = new StringWriter();
			transformer.transform(new DOMSource(doc), new StreamResult(writer));
			
			return writer.getBuffer().toString().replaceAll("\n|\r", "");
		}
	}
	
}
