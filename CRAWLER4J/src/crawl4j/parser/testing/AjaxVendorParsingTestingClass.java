package crawl4j.parser.testing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class AjaxVendorParsingTestingClass {
	public static void main(String[] args){

		//String my_url_to_fetch = "http://www.cdiscount.com/electromenager/tous-nos-accessoires/joint-hublot-d-30-30-cm/f-11029-ind3662734065501.html#mpos=2|mp";
		//document.write("<p class='fpSellBy'>Vendu par et expédié par <a href='#seller'>SEM Boutique</a></p>");		
		String my_url_to_fetch = "http://www.cdiscount.com/le-sport/vetements-de-sport/kappa-survetement-armor-homme/f-121020526-3025ej0005.html#mpos=1|cd";
		//document.write("<p class='fpSellBy'>Vendu et expédié par <span class='logoCDS'>Cdiscount</span></p>");

		try{
			// we here don't even use JQuery or XPath
			URL page = new URL(my_url_to_fetch);
			HttpURLConnection conn = (HttpURLConnection) page.openConnection();
			conn.connect();
			InputStreamReader in = new InputStreamReader((InputStream) conn.getContent());
			BufferedReader buff = new BufferedReader(in);
			String line;
			StringBuilder contentbuilder = new StringBuilder();
			do {
				line = buff.readLine();
				contentbuilder.append(line);
			} while (line != null);

			int cdiscount_index = contentbuilder.toString().indexOf("<p class='fpSellBy'>Vendu et expédié par <span class='logoCDS'>");

			if (cdiscount_index >0){
				System.out.println("Cdiscount");
			}else{
				System.out.println("Market Place");
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		} 
	}
}