package com.compare.facette.advanced.batch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriverService;
import org.openqa.selenium.remote.DesiredCapabilities;

import com.compare.facette.batch.URLListFacettesThreadPool;
import com.facettes.data.AdvancedFacettesInfo;
import com.facettes.data.URLFacettesData;


public class BatchFacetteSeleniumURLListBatch {

	private static String drop_facettes_list_results_table = "DROP TABLE IF EXISTS REFERENTIAL_FACETTES_LIST_RESULTS";
	private static String create_facettes_list_results_table = "CREATE TABLE IF NOT EXISTS REFERENTIAL_FACETTES_LIST_RESULTS (URL TEXT, MAGASIN VARCHAR(250), LEVEL_TWO VARCHAR(300), LEVEL_THREE VARCHAR(400), FACETTE_NAME VARCHAR(400), FACETTE_VALUE VARCHAR(250), FACETTE_COUNT INT, PRODUCTLIST_COUNT INT, IS_FACETTE_OPENED BOOLEAN, IS_VALUE_OPENED BOOLEAN) TABLESPACE mydbspace";

	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/referential_facettes.properties";
	private static String select_statement = "SELECT ID, URL FROM REFERENTIAL_FACETTES_LIST WHERE TO_FETCH = TRUE";

	private static Pattern bracketPattern = Pattern.compile("\\(.*?\\)");

	private static String path = "/home/sduprey/My_Programs/phantomjs-1.9.8-linux-x86_64/bin/phantomjs";
	
	public static void main(String[] args) {
		List<URLFacettesData> tofetch_list = new ArrayList<URLFacettesData>();

		String my_user_agent= "CdiscountBot-crawler";
		if (args.length>=1){
			my_user_agent= args[0];
		} else {
			System.out.println("You didn't specify any user agent, we'll use : "+my_user_agent);
		}

		System.out.println("User agent selected : "+my_user_agent);

		//	Getting the database property
		Properties props = new Properties();
		FileInputStream in = null;      
		try {
			in = new FileInputStream(database_con_path);
			props.load(in);
		} catch (IOException ex) {
			System.out.println("Trouble fetching database configuration");
			ex.printStackTrace();

		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				System.out.println("Trouble fetching database configuration");
				ex.printStackTrace();
			}
		}
		//the following properties have been identified
		String url = props.getProperty("db.url");
		String user = props.getProperty("db.user");
		String passwd = props.getProperty("db.passwd");

		// The database connection
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;

		try {  
			con = DriverManager.getConnection(url, user, passwd);
			// cleaning up the database results
			PreparedStatement drop_table_st = con.prepareStatement(drop_facettes_list_results_table);
			drop_table_st.executeUpdate();
			drop_table_st.close();
			System.out.println("Dropping the old facettes results table");

			PreparedStatement create_table_st = con.prepareStatement(create_facettes_list_results_table);
			create_table_st.executeUpdate();
			create_table_st.close();
			System.out.println("Creating the new RESULTS table");

			// getting the number of URLs to fetch
			pst = con.prepareStatement(select_statement);
			rs = pst.executeQuery();
			while (rs.next()) {
				URLFacettesData url_data = new URLFacettesData();
				url_data.setId(rs.getInt(1));
				url_data.setUrl(rs.getString(2));

				tofetch_list.add(url_data);
			}
			int size=tofetch_list.size();

			System.out.println("We have : " +size + " URL status to fetch according to the database \n");

			fetch_and_update_list_urls(tofetch_list);
			tofetch_list.clear();
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(URLListFacettesThreadPool.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (pst != null) {
					pst.close();
				}
				if (con != null) {
					con.close();
				}

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(URLListFacettesThreadPool.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}
		System.out.println("Finished all threads");
	}

	private static void fetch_and_update_list_urls(List<URLFacettesData> tofetch_list){
		File phantomjs = new File(path);

		DesiredCapabilities dcaps = new DesiredCapabilities();

		dcaps.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, phantomjs.getAbsolutePath());
		PhantomJSDriver driver = new PhantomJSDriver(dcaps);
		
		for (URLFacettesData to_fetch: tofetch_list){
			try {
				String url = to_fetch.getUrl();
                System.out.println("Fetching URL with Selenium API : "+url);
				// And now use this to visit Google
				driver.get(url);
				// Alternatively the same thing can be done like this
				// driver.navigate().to("http://www.google.com");
				WebElement menuArrow = driver.findElement(By.xpath("//*[@id='mvFilter']/form//div[text()='Vendeur']"));
				menuArrow.click();	        
				WebElement checkBoxFacetMarketPlaceFiltering = driver.findElement(By.xpath("//input[@value='f/368/c le marche']"));
				checkBoxFacetMarketPlaceFiltering.click();

				String htmlPageSourceCode = driver.getPageSource();
				 List<AdvancedFacettesInfo> extracted_facettes = extract_facettes(htmlPageSourceCode, to_fetch);
				 System.out.println("Market place facettes number : "+extracted_facettes.size());
			} catch (Exception e){
				e.printStackTrace();
			}
		}
		//Close the browser
		driver.quit();
	}

	private static List<AdvancedFacettesInfo> extract_facettes(String sourceCodePage,URLFacettesData to_fetch){
		List<AdvancedFacettesInfo> my_fetched_infos = new ArrayList<AdvancedFacettesInfo>();

		org.jsoup.nodes.Document doc =  Jsoup.parse(sourceCodePage);
		AdvancedFacettesInfo my_info = new AdvancedFacettesInfo();
		my_info.setId(to_fetch.getId());
		my_info.setUrl(to_fetch.getUrl());

		Elements counter_elements = doc.select(".lpStTit strong");		
		String product_size_text = counter_elements.text();
		my_info.setProducts_size(product_size_text);
		boolean isFacetteOpened = false;

		Elements facette_elements = doc.select("div.mvFacets.jsFCategory.mvFOpen");			
		for (Element facette : facette_elements ){
			//System.out.println(e.toString());
			Elements facette_name = facette.select("div.mvFTitle.noSel");
			my_info.setFacetteName(facette_name.text());
			Elements facette_values = facette.select("a");
			for (Element facette_value : facette_values){		
				//System.out.println(facette_value);
				// old way
				String categorie_value = facette_value.text();
				if ("".equals(categorie_value)){
					categorie_value = facette_value.attr("title");
				}
				Matcher matchPattern = bracketPattern.matcher(categorie_value);
				String categorieCount ="";
				while (matchPattern.find()) {		
					categorieCount=matchPattern.group();
				}
				categorie_value=categorie_value.replace(categorieCount,"");
				categorieCount=categorieCount.replace("(", "");
				categorieCount=categorieCount.replace(")", "");	
				//System.out.println(categorie_value);
				try{
					my_info.setFacetteCount(Integer.valueOf(categorieCount));
					//System.out.println(Integer.valueOf(categorieCount));	
				} catch (NumberFormatException e){
					System.out.println("Trouble while formatting a facette");
					my_info.setFacetteCount(0);
				}
				my_info.setFacetteValue(categorie_value);
				my_info.setIs_opened(isFacetteOpened);
				my_fetched_infos.add(my_info);
				my_info = new AdvancedFacettesInfo();
				my_info.setId(to_fetch.getId());
				my_info.setUrl(to_fetch.getUrl());
				my_info.setProducts_size(product_size_text);
				my_info.setFacetteName(facette_name.text());
			}		
		}
		return my_fetched_infos;
	}
}
