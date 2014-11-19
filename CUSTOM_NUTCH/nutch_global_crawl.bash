#!/bin/bash
export JAVA_HOME='/usr/lib/jvm/java-7-oracle/jre/';
/data/apache-nutch-2.0/runtime/local/bin/nutch inject /data/apache-nutch-2.0/nutch_results/urls/;
/data/apache-nutch-2.0/runtime/local/bin/nutch generate; 
/data/apache-nutch-2.0/runtime/local/bin/nutch fetch -all; 
/data/apache-nutch-2.0/runtime/local/bin/nutch parse -all; 
/data/apache-nutch-2.0/runtime/local/bin/nutch updatedb;
/data/apache-nutch-2.0/runtime/local/bin/nutch solrindex http://localhost:8983/solr/new_core;