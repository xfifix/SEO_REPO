package com.omniture;

import java.util.concurrent.TimeUnit;

import com.crawljax.browser.EmbeddedBrowser.BrowserType;
import com.crawljax.core.CrawlerContext;
import com.crawljax.core.CrawljaxRunner;
import com.crawljax.core.configuration.BrowserConfiguration;
import com.crawljax.core.configuration.CrawljaxConfiguration;
import com.crawljax.core.configuration.CrawljaxConfiguration.CrawljaxConfigurationBuilder;
import com.crawljax.core.plugin.OnNewStatePlugin;
import com.crawljax.core.state.StateVertex;

public class AddBasketScenario {

	private static int waiting_times = 10;
	private static String[] scenario3 = {
		"scAdd", 
		"eVar45=22494", 
		"eVar8=Product:Maitre:Maitre", 
		"products=\";GTP5210ZWAXEF"
	};

	// URL 1 : fiche produit Samsung Galaxy Tab 3 10.1 16Go
	// Il faut qu’apparaissent dans le code html de la page retour ajout les variables suivantes avec les valeurs suivantes :
	//	scAdd
	//	eVar45=22494
	//	eVar8=Product:Maitre:Maitre
	//	products=";GTP5210ZWAXEF

	public static void main(String[] args){
		String my_departing_url = "http://www.cdiscount.com/informatique/tablettes-tactiles-ebooks/samsung-galaxy-tab-3-10-1-16go/f-10798010207-gtp5210zwaxef.html";
		CrawljaxConfigurationBuilder builder = CrawljaxConfiguration.builderFor(my_departing_url);
		// we just add to the basket at the current product page
		builder.crawlRules().click("input").withAttribute("type", "submit").withAttribute("id", "fpAddBsk");
		builder.setBrowserConfig(new BrowserConfiguration(BrowserType.PHANTOMJS, 1));
		// we give ourselves time (fetching additional resellers might be long)
		builder.crawlRules().waitAfterReloadUrl(waiting_times, TimeUnit.SECONDS);
		//builder.crawlRules().waitAfterEvent(WAIT_TIME_AFTER_EVENT, TimeUnit.MILLISECONDS);
		// adding our in-house parser plugin which will be used to fetch our resellers data
		builder.addPlugin(new OnNewStatePlugin() {
			@Override
			public void onNewState(CrawlerContext context, StateVertex newState) {
				// This will print the DOM when a new state is detected. You should see it in your
				// console.
				String content_to_parse = context.getBrowser().getStrippedDom();
				System.out.println("Checking basket scenario : " + checkBasketScenario(content_to_parse));
				
//				org.jsoup.nodes.Document doc = Jsoup.parse(content_to_parse);
//				Elements resellers = doc.select(".slrName");
//				Elements prices = doc.select("p.price");
//
//
//				// converting the elements to String[]
//				String[] reseller_names = new String[resellers.size()];
//				int index=0;
//				for (Element reseller : resellers) {
//					reseller_names[index]=reseller.text();
//					index++;
//				}
//				Double[] price_values = new Double[prices.size()];
//				index=0;
//				for (Element price : prices) {
//					String matching_price = price.text();
//					matching_price=matching_price.replace("\u20ac",".");
//					try {
//						price_values[index]=Double.valueOf(matching_price);
//					} catch (NumberFormatException e){
//						System.out.println("Trouble converting : "+matching_price);
//						price_values[index]=new Double(0);
//					}
//					index++;
//				}
				// filling now the prices map with the found values
			}
			@Override
			public String toString() {
				return "Add Basket Plugin";
			}
		});

		CrawljaxRunner crawljax = new CrawljaxRunner(builder.build());
		crawljax.call();
		// concatenate the crawl info from each crawled page



	}


	public static boolean checkBasketScenario(String content){
		// scView, prop1, prop2, prop3, prop7, prop14, prop16, prop17, prop18, prop62, prop63 must be present
		// they must take the following values
		//prop1="Cdiscount"
		//prop2="Orderprocess"
		//prop3="Panier"
		//prop7="Panier"
		//prop14=(typeof $.cookies === "object" && typeof $.cookies.get === "function") ? $.cookies.get("SiteVersionCookie").substring($.cookies.get("SiteVersionCookie").length - 100) : "";    s.eVar8="";    s.eVar11="";    s.eVar30=(typeof $.cookies === "object" && typeof $.cookies.get === "function") ? $.cookies.get("ClientId").substring($.cookies.get("ClientId").length - 100).split("&")[0] : ""
		boolean isScenarioOK = true;
		for (String localScenario : scenario3){
			if (!content.contains(localScenario)){
				isScenarioOK = false;
				System.out.println("Content missing for scenario 3 : "+localScenario);
			}
		}
		return isScenarioOK;
	}
}
