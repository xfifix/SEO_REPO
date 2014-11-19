package org.apache.nutch.parser.my_plugin;

import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.w3c.dom.DocumentFragment;

/**
 * The parse portion of the Tag Extractor module. Parses out blog tags 
 * from the body of the document and sets it into the ParseResult object.
 */
public class MyExtractorParseFilter implements HtmlParseFilter {

	public static final String TAG_KEY = "labels";

	public static final String ZTD_KEY = "ztd";

	public static final String H1_KEY = "h1";

	public static final String FOOTER_KEY = "footer";
	
	public static final String DESCR_KEY = "details";
	
	public static final String VENDOR_KEY = "vendor";
	
	public static final String NB_ATTRIBUTE_KEY = "nb_attributes";
	
	public static final String ATTRIBUTE_KEY = "attributes";
	

	private static final Logger LOG = 
			Logger.getLogger(MyExtractorParseFilter.class);

	private static final Pattern tagPattern = 
			Pattern.compile(">(\\w+)<");

	private Configuration conf;

	/**
	 * We use regular expressions to parse out the Labels section from
	 * the section snippet shown below:
	 * <pre>
	 * Labels:
	 * <a href='http://sujitpal.blogspot.com/search/label/ror' rel='tag'>ror</a>,
	 * ...
	 * </span>
	 * </pre>
	 * Accumulate the tag values into a List, then stuff the list into the
	 * parseResult with a well-known key (exposed as a public static variable
	 * here, so the indexing filter can pick it up from here).
	 */
	public ParseResult filter(Content content, ParseResult parseResult,
			HTMLMetaTags metaTags, DocumentFragment doc) {
		LOG.info("houhouhou yes I have been there MyExtractorParseFilter");	  
		// LOG.info("Parsing content: " + content.getContent());
		//    BufferedReader reader = new BufferedReader(
		//      new InputStreamReader(new ByteArrayInputStream(
		//      content.getContent())));
		//  String html = "bonjourmonsieur";
		String h1text ="";
		String footertext ="";
		String ztdtext ="";
		String descriptiontext ="";	
		String vendortext ="";	
		String attributetext ="";
		int nb_arguments=0;
		try{
			String string_to_parse = new String(content.getContent());
			//LOG.info("Jsoup content: " + string_to_parse);
			Document soup_doc = Jsoup.parse(string_to_parse);
			// finding H1 tags
			Elements h1elem = soup_doc.select("h1");
			if (!(h1elem == null)){
				h1text=h1elem.text();
			} 
			// finding the footer element
			Elements footerelem = soup_doc.select("div.ftMention");
			if (!(footerelem == null)){
				footertext=footerelem.text();
			} 
			// finding the ztd
			Elements ztdelem = soup_doc.select("p.scZtdTxt");
			if (!(ztdelem == null)){
				ztdtext=ztdelem.text();
			} 
			Elements ztdunfoldelem = soup_doc.select("p.scZtdH");
			if (!(ztdunfoldelem == null)){
				ztdtext=ztdtext+ztdunfoldelem.text();
			} 
			// finding the description
			Elements descrelem = soup_doc.select("p.fpMb");
			if (!(descrelem == null)){
				descriptiontext=descrelem.text();
			}	
			// finding the vendor
			Elements vendorelem = soup_doc.select(".logoCDS");
			if (!(vendorelem == null)){
				vendortext=vendorelem.text();
			}	
			
			// finding the vendor
			Elements attributeselem = soup_doc.select(".fpDescTb tbody");
			if (!(attributeselem == null)){
				attributetext=attributeselem.text();
				nb_arguments=attributeselem.size();
			}
			
			
			LOG.info("houhouhou footer"+footertext);
			LOG.info("houhouhou h1"+h1text);
			LOG.info("houhouhou ztd"+ztdtext);
			LOG.info("houhouhou description"+descriptiontext);
			LOG.info("houhouhou vendor"+vendortext);
		} catch (Exception e){
			e.printStackTrace();
		}
		Parse parse = parseResult.get(content.getUrl());
		Metadata metadata = parse.getData().getParseMeta();
		metadata.add(H1_KEY,h1text);
		metadata.add(FOOTER_KEY,footertext);
		metadata.add(ZTD_KEY,ztdtext);
		metadata.add(DESCR_KEY,descriptiontext);
		metadata.add(VENDOR_KEY,vendortext);
		metadata.add(ATTRIBUTE_KEY,attributetext);
		metadata.add(NB_ATTRIBUTE_KEY,Integer.toString(nb_arguments));
	
		// Jsoup.parseBodyFragment();
		//
		//    try {
		//        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		//        DocumentBuilder builder;
		//		builder = factory.newDocumentBuilder();
		//		Document my_dom = builder.parse(new ByteArrayInputStream(
		//			      content.getContent()));
		//	} catch (ParserConfigurationException | SAXException | IOException e1) {
		//		// TODO Auto-generated catch block
		//		e1.printStackTrace();
		//	}

		//
		//
		//		    String line;
		//		    boolean inTagSection = false;
		//		    List<String> tags = new ArrayList<String>();
		//		    try {
		//		      while ((line = reader.readLine()) != null) {
		//		        if (line == null) {
		//		          continue;
		//		        }
		//		        ///LOG.info("Parsing content: " + line);
		//		        if (line.contains("Labels:")) {
		//		          inTagSection = true;
		//		          continue;
		//		        }
		//		        if (inTagSection && line.contains("</span>")) {
		//		          inTagSection = false;
		//		          break;
		//		        }
		//		        if (inTagSection) {
		//		          Matcher m = tagPattern.matcher(line);
		//		          if (m.find()) {
		//		            LOG.debug("Adding tag=" + m.group(1));
		//		            tags.add(m.group(1));
		//		          }
		//		        }
		//		      }
		//		      reader.close();
		//		    } catch (IOException e) {
		//		      LOG.warn("IOException encountered parsing file:", e);
		//		    }
		//		    Parse parse = parseResult.get(content.getUrl());
		//		    Metadata metadata = parse.getData().getParseMeta();
		//		    for (String tag : tags) {
		//		      metadata.add(TAG_KEY, tag);
		//		    }
		return parseResult;
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}
}