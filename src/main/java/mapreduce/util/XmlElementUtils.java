package mapreduce.util;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class XmlElementUtils {
	
	private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	
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
	
	public static void main(String[] args) throws Exception {
		String post = "<row Id=\"6939296\" PostTypeId=\"2\" ParentId=\"6939137\" CreationDate=\"2011-08-04T09:50:25.043\" Score=\"4\" ViewCount=\"\" Body=\"&lt;p&gt;You should have imported Poll with &lt;code&gt; from polls.models import poll&lt;/code&gt;&lt;/p&gt;&#xA;\" OwnerUserId=\"634150\" LastActivityDate=\"2011-08-04T09:50:25.043\" CommentCount=\"1\"/>";
		ArrayList<String> comments = new ArrayList<String>();
		String comment = "<row Id=\"8189677\" PostId=\"6939296\" Text=\"Have you looked at Hadoop?\" CreationDate=\"2011-07-30T07:29:33.343\" UserId=\"831878\"/>";
		comments.add(comment);
		XmlElementUtils utils = new XmlElementUtils();
		String str = utils.nestElement(post, comments);
		System.out.println(str);
	}
}
