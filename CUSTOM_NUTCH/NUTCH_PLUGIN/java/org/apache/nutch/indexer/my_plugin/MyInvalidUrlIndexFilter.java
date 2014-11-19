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

/**
 * This indexing filter removes "invalid" urls that have been crawled
 * (out of necessity, since they lead to valid pages), but need to be
 * removed from the index. The invalid urls contain the string 
 * "archive" (for archive pages which contain full text and links to
 * individual blog pages), "label" (tag based search result page with
 * full text of blogs labelled with the tag, and links to the individual
 * blog pages), and "feeds" (for RSS/Atom feeds, which we don't care
 * about, since they are duplicates of our blog pages). We also don't
 * care about the urls that are not suffixed with a .html extension.
 * @author Sujit Pal
 * @version $Revision$
 */
public class MyInvalidUrlIndexFilter implements IndexingFilter {

  private static final Logger LOGGER = 
    Logger.getLogger(MyInvalidUrlIndexFilter.class);
  
  private Configuration conf;
  
  public void addIndexBackendOptions(Configuration conf) {
    // NOOP
    return;
  }

  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {
//    if (url == null) {
//      return null;
//    }
//    LOGGER.info("houhouhou yes I have been there InvalidUrlIndexFilter");
//    if (url.find("archive") > -1 ||
//        url.find("label") > -1 ||
//        url.find("feeds") > -1) {
//      // filter out if url contains "archive", "label" or "feeds"
//      LOGGER.debug("Skipping URL: " + new String(url.getBytes()));
//      return null;
//    }
//    if (url.find(".html") == -1) {
//      // filter out if url does not have a .html extension
//      LOGGER.debug("Skipping URL: " + new String(url.getBytes()));
//      return null;
//    }
    // otherwise, return the document
    return doc;
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}