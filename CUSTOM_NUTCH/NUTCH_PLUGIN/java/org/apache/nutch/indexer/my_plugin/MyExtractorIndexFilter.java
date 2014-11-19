package org.apache.nutch.indexer.my_plugin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parser.my_plugin.MyExtractorParseFilter;


/**
 * The indexing portion of the TagExtractor module. Retrieves the
 * tag information stuffed into the ParseResult object by the parse
 * portion of this module.
 */
//import org.apache.nutch.indexer.lucene.LuceneWriter;
//import org.apache.nutch.indexer.lucene.LuceneWriter.INDEX;
//import org.apache.nutch.indexer.lucene.LuceneWriter.STORE;
public class MyExtractorIndexFilter implements IndexingFilter {

	private static final Logger LOGGER = 
			Logger.getLogger(MyExtractorIndexFilter.class);

	private Configuration conf;
	//  
	//  public void addIndexBackendOptions(Configuration conf) {
	//    LuceneWriter.addFieldOptions(
	//      MyExtractorParseFilter.TAG_KEY, STORE.YES, INDEX.UNTOKENIZED, conf);
	//  }

	public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
			CrawlDatum datum, Inlinks inlinks) throws IndexingException {
		LOGGER.info("houhouhou yes I have been there MyExtractorIndexFilter");
		String[] h1text = 
				parse.getData().getParseMeta().getValues(
						MyExtractorParseFilter.H1_KEY);
//		StringBuilder builder = new StringBuilder();
//		for (String s : h1text){
//			builder.append(s);
//		}
		LOGGER.debug("houhouhouh Adding tag: [" + h1text + "] for URL: " + url.toString());
		doc.add(MyExtractorParseFilter.H1_KEY, h1text);

		String[] footertext = 
				parse.getData().getParseMeta().getValues(
						MyExtractorParseFilter.FOOTER_KEY);
//		builder = new StringBuilder();
//		for (String s : footertext){
//			builder.append(s);
//		}
		LOGGER.debug("houhouhou Adding tag: [" + footertext + "] for URL: " + url.toString());
		doc.add(MyExtractorParseFilter.FOOTER_KEY, footertext);	    

		String[] ztdtext = 
				parse.getData().getParseMeta().getValues(
						MyExtractorParseFilter.ZTD_KEY);
//		builder = new StringBuilder();
//		for (String s : ztdtext){
//			builder.append(s);
//		}
		LOGGER.debug("houhouhou Adding tag: [" + ztdtext + "] for URL: " + url.toString());
		doc.add(MyExtractorParseFilter.ZTD_KEY, ztdtext);	  

		String[] descrtext = 
				parse.getData().getParseMeta().getValues(
						MyExtractorParseFilter.DESCR_KEY);
//		builder = new StringBuilder();
//		for (String s : descrtext){
//			builder.append(s);
//		}
		LOGGER.debug("houhouhou Adding tag: [" + descrtext + "] for URL: " + url.toString());
		doc.add(MyExtractorParseFilter.DESCR_KEY, descrtext);	 

		String[] vendortext = 
				parse.getData().getParseMeta().getValues(
						MyExtractorParseFilter.VENDOR_KEY);
//		builder = new StringBuilder();
//		for (String s : vendortext){
//			builder.append(s);
//		}
		LOGGER.debug("houhouhou Adding tag: [" + vendortext + "] for URL: " + url.toString());
		doc.add(MyExtractorParseFilter.VENDOR_KEY, vendortext);	

		String[] attributestext = 
				parse.getData().getParseMeta().getValues(
						MyExtractorParseFilter.ATTRIBUTE_KEY);
//		builder = new StringBuilder();
//		for (String s : vendortext){
//			builder.append(s);
//		}
		LOGGER.debug("houhouhou Adding tag: [" + attributestext + "] for URL: " + url.toString());
		doc.add(MyExtractorParseFilter.ATTRIBUTE_KEY, attributestext);	

		String[] nbattributestext = 
				parse.getData().getParseMeta().getValues(
						MyExtractorParseFilter.NB_ATTRIBUTE_KEY);
//		builder = new StringBuilder();
//		for (String s : vendortext){
//			builder.append(s);
//		}
		LOGGER.debug("houhouhou Adding tag: [" + nbattributestext + "] for URL: " + url.toString());
		doc.add(MyExtractorParseFilter.NB_ATTRIBUTE_KEY, nbattributestext);	

		//    if (tags == null || tags.length == 0) {
		//      return doc;
		//    }

		// add to the nutch document, the properties of the field are set in
		// the addIndexBackendOptions method.
		//    for (String tag : tags) {
		//      LOGGER.debug("Adding tag: [" + tag + "] for URL: " + url.toString());
		//      doc.add(MyExtractorParseFilter.TAG_KEY, tag);
		//    }
		return doc;
	}

	public Configuration getConf() {
		return this.conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}
}