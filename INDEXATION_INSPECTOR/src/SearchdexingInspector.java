import java.io.IOException;
import java.util.Random;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class SearchdexingInspector {

	private static String request_string ="https://www.google.fr/search?q=";
    // this URL works but gives ajax to play before you parse the DOM
	//	private static String request_string ="https://www.google.fr/#safe=off&q=";
	
	public static String[] to_follow =	{
		"site:www.cdiscount.com/sa-10/",
		"site:www.cdiscount.com/le-sport/r-",
		"site:www.cdiscount.com/telephonie/r-",
		"site:www.cdiscount.com/photo-numerique/r-",
		"site:www.cdiscount.com/high-tech/r-",
		"site:www.cdiscount.com/chaussures/r-",
		"site:www.cdiscount.com/bagages/r-",
		"site:www.cdiscount.com/bijouterie/r-",
		"site:www.cdiscount.com/vin-champagne/r-",
		"site:www.cdiscount.com/au-quotidien/r-",
		"site:www.cdiscount.com/maison/r-",
		"site:www.cdiscount.com/electromenager/r-",
		"site:www.cdiscount.com/juniors/r-",
		"site:www.cdiscount.com/informatique/r-",
		"site:www.cdiscount.com/dvd/r-",
		"site:www.cdiscount.com/auto/r-",
	    "site:www.cdiscount.com/livres-bd/r-"};

	public static void main(String[] args){
		System.setProperty("http.agent", "");
		for (String todo : to_follow){
			gettingIndexationCounter(todo);
		}
	}
	
	public static IndexationInfo gettingIndexationCounter(String request){
		IndexationInfo info= new IndexationInfo();
		info.setRequest(request);
		// we here fetch up to five paginations
		org.jsoup.nodes.Document doc;
		try{
			request=request.replace("/", "%2F");
			String my_url = request_string+request;
			doc =  Jsoup.connect(my_url)
					.userAgent("Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)")
					.ignoreHttpErrors(true)
					.timeout(0)
					.get();
			
			Elements stats = doc.select("#resultStats");
			for (Element stat : stats) {
				String text=stat.text().replace("&nbsp;"," ");
				text=text.replace("&eacute;","é");
				System.out.println("Request : "+info.getRequest());
				System.out.println("Results : "+text);
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		} 
		return info;
	}

	public static int randInt(int min, int max) {
		// NOTE: Usually this should be a field rather than a method
		// variable so that it is not re-seeded every call.
		Random rand = new Random();
		// nextInt is normally exclusive of the top value,
		// so add 1 to make it inclusive
		int randomNum = rand.nextInt((max - min) + 1) + min;
		return randomNum;
	}

	private static class IndexationInfo {
		String request="";
		int count=0;
		public String getRequest() {
			return request;
		}
		public void setRequest(String request) {
			this.request = request;
		}
		public int getCount() {
			return count;
		}
		public void setCount(int count) {
			this.count = count;
		}
	}
}