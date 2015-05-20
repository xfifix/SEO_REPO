package com.lp.facettes.filtering;

import java.io.File;
import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriverService;
import org.openqa.selenium.remote.DesiredCapabilities;

import com.facettes.data.AdvancedFacettesInfo;
import com.facettes.data.URLFacettesData;
import com.facettes.utility.FacettesUtility;

public class FacettesFiltering {
	
	private static String path = "/home/sduprey/My_Programs/phantomjs-1.9.8-linux-x86_64/bin/phantomjs";

	public static void main(String[] args){
		URLFacettesData to_fetch = new URLFacettesData();
		to_fetch.setUrl("http://www.cdiscount.com/maison/linge-maison/linge-de-decoration/plaids-et-couvre-lits/l-117620403.html");
		// Create a new instance of the Firefox driver
		// Notice that the remainder of the code relies on the interface, 
		// not the implementation.
//		File phantomjs = new File(System.getProperty("java.io.tmpdir")+File.separator+"phantomjs-1.9.7");
		File phantomjs = new File(path);



		DesiredCapabilities dcaps = new DesiredCapabilities();
		dcaps.setCapability("takesScreenshot", true);


		dcaps.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, phantomjs.getAbsolutePath());
		PhantomJSDriver driver = new PhantomJSDriver(dcaps);
		//WebDriver driver = new PhantomJSDriver();
		//	WebDriver driver = new ChromeDriver();

		// And now use this to visit Google
		driver.get(to_fetch.getUrl());
		// Alternatively the same thing can be done like this
		// driver.navigate().to("http://www.google.com");


		// Radio Button: Check Monday using XPATH locator.

		//WebElement menuArrow = driver.findElement(By.cssSelector("div.mvFacets.jsFCategory"));
		WebElement menuArrow = driver.findElement(By.xpath("//*[@id='mvFilter']/form//div[text()='Vendeur']"));
		menuArrow.click();

		WebElement checkBoxFacetMarketPlaceFiltering = driver.findElement(By.xpath("//input[@value='f/368/c le marche']"));
		checkBoxFacetMarketPlaceFiltering.click();

		String htmlPageSourceCode = driver.getPageSource();
		//Close the browser
		driver.quit();
		// Check the title of the page
		System.out.println("Page source code : " + htmlPageSourceCode);
		List<AdvancedFacettesInfo> marketPlaceFacettes = FacettesUtility.extract_facettes_from_source_code(htmlPageSourceCode, to_fetch);
		System.out.println("Market place facettes number : "+marketPlaceFacettes.size());				

		
		String url = to_fetch.getUrl();
		System.out.println("Fetching URL with standard Apache library without ajax playing : "+url);
		List<AdvancedFacettesInfo> standardFacettes = FacettesUtility.extract_facettes_infos(to_fetch);
		System.out.println("Standard facettes number : "+standardFacettes.size());		
		
		 List<AdvancedFacettesInfo> merged_facettes = FacettesUtility.merge_facettes(marketPlaceFacettes,standardFacettes);

		System.out.println("Merged facettes size : "+merged_facettes.size());
	}
}
