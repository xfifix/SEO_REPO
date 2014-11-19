package org.apache.nutch.indexer.my_plugin;
 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
 
public class AddField implements IndexingFilter {
 
    private static final Log LOG = LogFactory.getLog(AddField.class);
    private Configuration conf;
 
    //implements the filter-method which gives you access to important Objects like NutchDocument
    public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
            CrawlDatum datum, Inlinks inlinks) {
        String content = parse.getText();
        LOG.info("houhouhou yes I have been there AddField");
        
        LOG.info("Document: " + doc.toString());
        LOG.info("Parse: " + parse.toString());
        LOG.info("ParseData: " + parse.getData());
        LOG.info("CrawlDatum: " + parse.getData());
    
        //adds the new field to the document
        doc.add("pageLength", content.length());
        return doc;
    }
 
    //Boilerplate
    public Configuration getConf() {
        return conf;
    }
 
    //Boilerplate
    public void setConf(Configuration conf) {
        this.conf = conf;
    }
}
