#Depth by depth reading of URLs and creation of nodes and relationships each time
#Filling up NODES & EDGES
PostGresLinksByLevelDaemon


#Reading everything and updating by batch
#Filling up NODES & EDGES
FastBatchPostGresLinksDaemon

#Computing the page rank on the whole NODES, EDGES database and updating CRAWL_RESULTS database
ComputePageRank