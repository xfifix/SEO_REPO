#!/bin/bash
export JAVA_HOME='/usr/lib/jvm/java-7-oracle/jre/';
# beware for this bash and this bash only : relative path
/home/sduprey/My_Programs/apache-nutch-1.8/runtime/local/bin/crawl urls/seed.txt crawl http://localhost:8983/solr/ 2
