package crawl4j.corpus;

public class CORPUSinfo {
	private String url="";
	private String documents_list="";
	private int documents_size=0;
	
	public String getUrl() {
		return url;
	}
	public String getDocuments_list() {
		return documents_list;
	}
	public void setDocuments_list(String documents_list) {
		this.documents_list = documents_list;
	}
	public int getDocuments_size() {
		return documents_size;
	}
	public void setDocuments_size(int documents_size) {
		this.documents_size = documents_size;
	}
	public void setUrl(String url) {
		this.url = url;
	}
}		
