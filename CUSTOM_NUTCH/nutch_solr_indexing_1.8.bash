#!/bin/bash
export JAVA_HOME='/usr/lib/jvm/java-7-oracle/jre/';
/home/sduprey/My_Programs/apache-nutch-1.8/runtime/local/bin/nutch solrindex http://localhost:8983/solr /home/sduprey/My_Programs/apache-nutch-1.8/nutch_results/crawl/crawldb -linkdb /home/sduprey/My_Programs/apache-nutch-1.8/nutch_results/crawl/linkdb /home/sduprey/My_Programs/apache-nutch-1.8/nutch_results/crawl/segments/*
