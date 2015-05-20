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

import org.jsoup.Jsoup;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriverService;
import org.openqa.selenium.remote.DesiredCapabilities;

import com.facettes.data.AdvancedFacettesInfo;
import com.facettes.data.URLFacettesData;
import com.facettes.utility.FacettesUtility;

public class BatchFacetteSeleniumURLListBatch {

	private static String drop_facettes_list_results_table = "DROP TABLE IF EXISTS REFERENTIAL_FACETTES_LIST_RESULTS";
	private static String create_facettes_list_results_table = "CREATE TABLE IF NOT EXISTS REFERENTIAL_FACETTES_LIST_RESULTS (URL TEXT,MAGASIN VARCHAR(250),LEVEL_TWO VARCHAR(300),LEVEL_THREE VARCHAR(400),FACETTE_NAME VARCHAR(400),FACETTE_VALUE VARCHAR(250), FACETTE_COUNT INT,MARKETPLACE_FACETTE_COUNT INT,MARKETPLACE_QUOTE_PART NUMERIC,PRODUCTLIST_COUNT INT,IS_FACETTE_OPENED BOOLEAN, OPENED_URL TEXT) TABLESPACE mydbspace";

	private static String insertStatement ="INSERT INTO REFERENTIAL_FACETTES_LIST_RESULTS (URL, MAGASIN, LEVEL_TWO, LEVEL_THREE, FACETTE_NAME, FACETTE_VALUE, FACETTE_COUNT, MARKETPLACE_FACETTE_COUNT, MARKETPLACE_QUOTE_PART, PRODUCTLIST_COUNT, IS_FACETTE_OPENED, OPENED_URL) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	private static String updateStatement = "UPDATE REFERENTIAL_FACETTES_LIST SET TO_FETCH=false where ID=?";


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

			fetch_and_update_list_urls(con, tofetch_list);
			tofetch_list.clear();
		} catch (SQLException ex) {
			ex.printStackTrace();
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
				ex.printStackTrace();
			}
		}
		System.out.println("Finished all threads");
	}

	private static void fetch_and_update_list_urls(Connection con, List<URLFacettesData> tofetch_list){
		File phantomjs = new File(path);

		DesiredCapabilities dcaps = new DesiredCapabilities();

		dcaps.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, phantomjs.getAbsolutePath());
		PhantomJSDriver driver = new PhantomJSDriver(dcaps);

		for (URLFacettesData to_fetch : tofetch_list){
			String htmlPageSourceCode = "";
			// to debug we use a known url
			String url = to_fetch.getUrl();
			try {
				System.out.println("Fetching URL with standard Apache library without ajax playing : "+url);
				String my_user_agent= "CdiscountBot-crawler";
				org.jsoup.nodes.Document doc = null;
				try {
					doc =  Jsoup.connect(url)
							.userAgent(my_user_agent)
							.ignoreHttpErrors(true)
							.timeout(0)
							.get();
				}catch (IOException e){
					e.printStackTrace();
				}
				List<AdvancedFacettesInfo> standardFacettes = new ArrayList<AdvancedFacettesInfo>();
				if (doc != null){
					standardFacettes = FacettesUtility.extract_facettes_infos(doc, to_fetch);
				}
				System.out.println("Standard facettes number : "+standardFacettes.size());				
				System.out.println("Fetching URL with Selenium API : "+url);
				try{
					driver.get(url);
					// Alternatively the same thing can be done like this
					// driver.navigate().to("http://www.google.com");
					WebElement menuArrow = driver.findElement(By.xpath("//*[@id='mvFilter']/form//div[text()='Vendeur']"));	     
					boolean isFacetVisible=false;
					try{
						menuArrow.findElement(By.xpath("//input[@style='display: none;']"));
					} catch (org.openqa.selenium.NoSuchElementException e){
						isFacetVisible=true;
					}
					if (!isFacetVisible){
						menuArrow.click();
					}
					WebElement checkBoxFacetMarketPlaceFiltering = null;
					try{
						checkBoxFacetMarketPlaceFiltering = driver.findElement(By.xpath("//input[@value='f/368/c le marche']"));
						checkBoxFacetMarketPlaceFiltering.click();
					} catch (org.openqa.selenium.ElementNotVisibleException e){
						System.out.println("The facet was already displayed, we undisplay it");
						try{
							menuArrow.click();
							checkBoxFacetMarketPlaceFiltering.click();
						} catch (Exception e2){
							System.out.println("Trouble checking the market place facet");
						}
					}
					htmlPageSourceCode = driver.getPageSource();
				} catch (Exception e){
					System.out.println("Trouble getting facette C le marché for URL : "+url);
					e.printStackTrace();
				}
				List<AdvancedFacettesInfo> marketPlaceFacettes = FacettesUtility.extract_facettes_from_source_code(htmlPageSourceCode, to_fetch);
				System.out.println("Market place facettes number : "+marketPlaceFacettes.size());

				// merging the facettes to get the market place quote part
				List<AdvancedFacettesInfo> merged_facettes = FacettesUtility.merge_facettes(marketPlaceFacettes,standardFacettes);

				System.out.println("Merged facettes size : "+merged_facettes.size());

				updateDatabaseFacettes(con, to_fetch,merged_facettes);

			} catch (Exception e){
				e.printStackTrace();
			}
		}
		//Close the browser
		driver.quit();
	}

	private static void updateDatabaseFacettes(Connection con, URLFacettesData to_fetch,List<AdvancedFacettesInfo> merged_facettes ){
		insertResults(con,merged_facettes);
		updateURL(con,to_fetch);
	}

	private static void updateURL(Connection con, URLFacettesData to_fetch){
		try {
			PreparedStatement st = con.prepareStatement(updateStatement);
			st.setInt(1, to_fetch.getId());
			st.executeUpdate();		
			st.close();
		} catch (SQLException e){
			e.printStackTrace();
			System.out.println("Trouble inserting batch ");
		}
	}

	private static void insertResultsStepByStep(Connection con, List<AdvancedFacettesInfo> infos){
		System.out.println("Adding to batch : " + infos.size() + "ULRs into database");
		try {
			PreparedStatement st = con.prepareStatement(insertStatement);
			for (AdvancedFacettesInfo info_to_update : infos){
				String url_to_update = info_to_update.getUrl();		
				String[] levels = FacettesUtility.getFirstLevels(url_to_update);			
				String facette_name = info_to_update.getFacetteName();
				String facette_value = info_to_update.getFacetteValue();
				int facette_count = info_to_update.getFacetteCount();
				int marketplace_facette_count =info_to_update.getMarketPlaceFacetteCount();
				int products_size = info_to_update.getProducts_size();
				boolean isopened = info_to_update.isIs_opened();
				String lfURL = info_to_update.getOpenedURL();
				double market_place_quote_part = info_to_update.getMarket_place_quote_part();	
				//URL
				st.setString(1, url_to_update);
				//MAGASIN
				st.setString(2, levels[0]);
				//LEVEL_TWO
				st.setString(3, levels[1]);
				//LEVEL_THREE
				st.setString(4, levels[2]);
				//FACETTE_NAME
				st.setString(5, facette_name);
				//FACETTE_VALUE
				st.setString(6, facette_value);
				//FACETTE_COUNT
				st.setInt(7, facette_count);
				//MARKETPLACE_FACETTE_COUNT
				st.setInt(8, marketplace_facette_count);
				//MARKETPLACE_QUOTE_PART
				st.setDouble(9, market_place_quote_part);
				//PRODUCTLIST_COUNT
				st.setInt(10, products_size);
				//IS_FACETTE_OPENED
				st.setBoolean(11, isopened);
				//IS_VALUE_OPENED
				st.setString(12, lfURL);
				st.executeUpdate();		
			}      
			st.close();
			System.out.println("Having inserted : " + infos.size() + "ULRs into database");
		} catch (SQLException e){
			e.printStackTrace();
			System.out.println("Trouble inserting batch ");
		}
	}

	private static void insertResults(Connection con, List<AdvancedFacettesInfo> infos){
		System.out.println("Adding to batch : " + infos.size() + "ULRs into database");
		try {
			//Statement st = con.createStatement();
			con.setAutoCommit(false); 
			PreparedStatement st = con.prepareStatement(insertStatement);
			for (AdvancedFacettesInfo info_to_update : infos){
				String url_to_update = info_to_update.getUrl();		
				String[] levels = FacettesUtility.getFirstLevels(url_to_update);			
				String facette_name = info_to_update.getFacetteName();
				String facette_value = info_to_update.getFacetteValue();
				int facette_count = info_to_update.getFacetteCount();
				int marketplace_facette_count =info_to_update.getMarketPlaceFacetteCount();
				int products_size = info_to_update.getProducts_size();
				boolean isopened = info_to_update.isIs_opened();
				String lfURL = info_to_update.getOpenedURL();
				double market_place_quote_part = info_to_update.getMarket_place_quote_part();	
				//URL
				st.setString(1, url_to_update);
				//MAGASIN
				st.setString(2, levels[0]);
				//LEVEL_TWO
				st.setString(3, levels[1]);
				//LEVEL_THREE
				st.setString(4, levels[2]);
				//FACETTE_NAME
				st.setString(5, facette_name);
				//FACETTE_VALUE
				st.setString(6, facette_value);
				//FACETTE_COUNT
				st.setInt(7, facette_count);
				//MARKETPLACE_FACETTE_COUNT
				st.setInt(8, marketplace_facette_count);
				//MARKETPLACE_QUOTE_PART
				st.setDouble(9, market_place_quote_part);
				//PRODUCTLIST_COUNT
				st.setInt(10, products_size);
				//IS_FACETTE_OPENED
				st.setBoolean(11, isopened);
				//IS_VALUE_OPENED
				st.setString(12, lfURL);
				st.addBatch();		
			}      
			//int counts[] = st.executeBatch();
			System.out.println("Beginning to insert : " + infos.size() + "ULRs into database");
			st.executeBatch();
			con.commit();
			st.close();
			System.out.println("Having inserted : " + infos.size() + "ULRs into database");
		} catch (SQLException e){
			e.printStackTrace();
			System.out.println("Trouble inserting batch ");
		}
	}


}
