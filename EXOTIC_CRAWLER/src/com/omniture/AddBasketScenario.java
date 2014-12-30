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
	private static final long WAIT_TIME_AFTER_EVENT = 200;
	private static final long WAIT_TIME_AFTER_RELOAD = 20;
	private static String[] basket_scenario = {
		"scAdd", 
		"eVar45=22494", 
		"eVar8=Product:Maitre:Maitre", 
		"products=\";GTP5210ZWAXEF"
	};
	private static boolean[] basket_scenario_results = new boolean[basket_scenario.length];
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
		builder.crawlRules().insertRandomDataInInputForms(false);
		builder.crawlRules().followExternalLinks(false);
		builder.setMaximumStates(2);
		builder.setMaximumDepth(1);

		builder.crawlRules().click("input").withAttribute("type", "submit").withAttribute("id", "fpAddBsk");
		builder.crawlRules().clickOnce(true);
		builder.setBrowserConfig(new BrowserConfiguration(BrowserType.PHANTOMJS, 1));
		//builder.setBrowserConfig(new BrowserConfiguration(BrowserType.FIREFOX, 1));
		
		// we give ourselves time (fetching additional resellers might be long)
		// Set timeouts
		builder.crawlRules().waitAfterReloadUrl(WAIT_TIME_AFTER_RELOAD, TimeUnit.MILLISECONDS);
		builder.crawlRules().waitAfterEvent(WAIT_TIME_AFTER_EVENT, TimeUnit.MILLISECONDS);

		//builder.crawlRules().waitAfterEvent(WAIT_TIME_AFTER_EVENT, TimeUnit.MILLISECONDS);
		// adding our in-house parser plugin which will be used to fetch our resellers data

		builder.addPlugin(new OnNewStatePlugin() {
			@Override
			public void onNewState(CrawlerContext context, StateVertex newState) {
				// This will print the DOM when a new state is detected. You should see it in your
				// console.
				String content_to_parse = context.getBrowser().getUnStrippedDom();
				System.out.println("Checking basket scenario : " + checkBasketScenario(content_to_parse));
			}
			@Override
			public String toString() {
				return "Add Basket Plugin";
			}
		});
		CrawljaxRunner crawljax = new CrawljaxRunner(builder.build());
		crawljax.call();
		// concatenate the crawl info from each crawled page	
		System.out.println("Reporting scenario results : ");
		int counter = 0;
		for (String content_to_find : basket_scenario){
			if (basket_scenario_results[counter]){
				System.out.println("Content found for basket scenario : "+content_to_find);
			} else {
				System.out.println("Content missing for basket scenario : "+content_to_find);
			}
		}
	}

	public static boolean checkBasketScenario(String content){
		boolean isScenarioOK = true;
		int counter = 0;
		for (String localScenario : basket_scenario){
			if (!content.contains(localScenario)){
				isScenarioOK = false;
				System.out.println("Content missing for basket scenario : "+localScenario);
			}  else {
				System.out.println("Content found for basket scenario : "+localScenario);
			}
			basket_scenario_results[counter] = isScenarioOK;
			counter ++;
		}
		return isScenarioOK;
	}
}