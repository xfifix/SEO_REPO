package crawl4j.daemon.links;

import java.net.URI;

public class URLNode {
	String node_url;
	URI neo4jURI;
	
	public String getNode_url() {
		return node_url;
	}
	public void setNode_url(String node_url) {
		this.node_url = node_url;
	}
	public URI getNeo4jURI() {
		return neo4jURI;
	}
	public void setNeo4jURI(URI neo4jURI) {
		this.neo4jURI = neo4jURI;
	}
}
