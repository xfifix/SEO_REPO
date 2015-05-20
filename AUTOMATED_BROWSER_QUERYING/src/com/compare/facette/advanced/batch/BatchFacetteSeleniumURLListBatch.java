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

import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriverService;
import org.openqa.selenium.remote.DesiredCapabilities;

import com.compare.facette.batch.URLListFacettesThreadPool;
import com.facettes.data.AdvancedFacettesInfo;
import com.facettes.data.URLFacettesData;
import com.facettes.utility.FacettesUtility;


public class BatchFacetteSeleniumURLListBatch {

	private static String drop_facettes_list_results_table = "DROP TABLE IF EXISTS REFERENTIAL_FACETTES_LIST_RESULTS";
	private static String create_facettes_list_results_table = "CREATE TABLE IF NOT EXISTS REFERENTIAL_FACETTES_LIST_RESULTS (URL TEXT, MAGASIN VARCHAR(250), LEVEL_TWO VARCHAR(300), LEVEL_THREE VARCHAR(400), FACETTE_NAME VARCHAR(400), FACETTE_VALUE VARCHAR(250), FACETTE_COUNT INT, PRODUCTLIST_COUNT INT, IS_FACETTE_OPENED BOOLEAN, IS_VALUE_OPENED BOOLEAN) TABLESPACE mydbspace";

	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/referential_facettes.properties";
	private static String select_statement = "SELECT ID, URL FROM REFERENTIAL_FACETTES_LIST WHERE TO_FETCH = TRUE";

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
				// to debug we use a known url
				String url = to_fetch.getUrl();
				System.out.println("Fetching URL with standard Apache library without ajax playing : "+url);
				List<AdvancedFacettesInfo> standardFacettes = FacettesUtility.extract_facettes_infos(to_fetch);
				System.out.println("Standard facettes number : "+standardFacettes.size());				
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
				List<AdvancedFacettesInfo> marketPlaceFacettes = FacettesUtility.extract_facettes_from_source_code(htmlPageSourceCode, to_fetch);
				System.out.println("Market place facettes number : "+marketPlaceFacettes.size());
			} catch (Exception e){
				e.printStackTrace();
			}
		}
		//Close the browser
		driver.quit();
	}




}
