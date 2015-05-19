package com.lp.facettes.filtering;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

public class FacettesFiltering {
	public static void main(String[] args){

        // Create a new instance of the Firefox driver
        // Notice that the remainder of the code relies on the interface, 
        // not the implementation.
        WebDriver driver = new FirefoxDriver();
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
