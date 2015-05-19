package com.lp.facettes.filtering;

import java.io.File;

import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriverService;
import org.openqa.selenium.remote.DesiredCapabilities;

public class FacettesFiltering {
	
	private static String path = "/home/sduprey/My_Programs/phantomjs-1.9.8-linux-x86_64/bin/phantomjs";

	public static void main(String[] args){

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
		driver.get("http://www.cdiscount.com/maison/linge-maison/linge-de-decoration/plaids-et-couvre-lits/l-117620403.html");
		// Alternatively the same thing can be done like this
		// driver.navigate().to("http://www.google.com");


		// Radio Button: Check Monday using XPATH locator.

		//WebElement menuArrow = driver.findElement(By.cssSelector("div.mvFacets.jsFCategory"));
		WebElement menuArrow = driver.findElement(By.xpath("//*[@id='mvFilter']/form//div[text()='Vendeur']"));
		menuArrow.click();

		WebElement checkBoxFacetMarketPlaceFiltering = driver.findElement(By.xpath("//input[@value='f/368/c le marche']"));
		checkBoxFacetMarketPlaceFiltering.click();

		String htmlPageSourceCode = driver.getPageSource();

		// Check the title of the page
		System.out.println("Page source code : " + htmlPageSourceCode);

		//Close the browser
		driver.quit();


	}
}
