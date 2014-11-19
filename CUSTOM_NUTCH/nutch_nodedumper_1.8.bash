
#!/bin/bash
export JAVA_HOME='/usr/lib/jvm/java-7-oracle/jre/';
/home/sduprey/My_Programs/apache-nutch-1.8/runtime/local/bin/nutch org.apache.nutch.scoring.webgraph.NodeDumper -scores -topn 50000 -webgraphdb /home/sduprey/My_Programs/apache-nutch-1.8/nutch_results/crawl/webgraphdb -output /home/sduprey/My_Programs/apache-nutch-1.8/nutch_results/crawl/webgraphdb/dump/scores
