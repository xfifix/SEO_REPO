#!/bin/bash
export JAVA_HOME='/usr/lib/jvm/java-7-oracle/jre/';
/home/sduprey/My_Programs/apache-nutch-1.8/runtime/local/bin/nutch org.apache.nutch.scoring.webgraph.WebGraph -segmentDir /home/sduprey/My_Programs/apache-nutch-1.8/nutch_results/crawl/segments -webgraphdb /home/sduprey/My_Programs/apache-nutch-1.8/nutch_results/crawl/webgraphdb
