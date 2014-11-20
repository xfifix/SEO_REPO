package com.marketplace;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.crawljax.browser.EmbeddedBrowser.BrowserType;
import com.crawljax.core.CrawlerContext;
import com.crawljax.core.CrawljaxRunner;
import com.crawljax.core.configuration.BrowserConfiguration;
import com.crawljax.core.configuration.CrawljaxConfiguration;
import com.crawljax.core.configuration.CrawljaxConfiguration.CrawljaxConfigurationBuilder;
import com.crawljax.core.plugin.OnNewStatePlugin;
import com.crawljax.core.state.StateVertex;

public class MarketPlaceCrawler {
	private static String cdiscount_url = "http://www.cdiscount.com/mp-9000-";
	private static Set<String> skuid = new HashSet<String>(); 
	private static int max_number_tries = 10;

	private static Map<String, Double> mapped_prices = new HashMap<String, Double>();
	private static Map<String,List<ResellerInfo>> resellers = new HashMap<String,List<ResellerInfo>>();
	private static List<OutRow> output_results = new ArrayList<OutRow>();
	private static List<CrawlInformation> ajax_results = new ArrayList<CrawlInformation>();

	public static void main(String[] args){	
		System.setProperty("http.agent", "");
		String phantomJSPath = "/home/sduprey/My_Programs/phantomjs-1.9.8-linux-x86_64/bin/phantomjs";
		System.setProperty("phantomjs.binary.path",phantomJSPath);
		System.out.println("We have set the path to PhantomJS executable here : "+phantomJSPath);
		if (args.length != 2){
			System.out.println("You must specify the input xls file and then the output csv file\n");
			System.out.println("Example : java MarketPlaceCrawler.java /home/sduprey/My_Data/FULFILLMENT/sku_list.xlsx /home/sduprey/My_Data/FULFILLMENT/sku_list_results.csv");
		}
		//String file_to_inject_path = "D:\\My_Data\\My_Market_Place_Data\\sku_list.xlsx";
		String file_to_inject_path ="/home/sduprey/My_Data/My_Market_Place_Data/sku_list.xlsx";
		//String file_to_inject_path =args[0];

		//String file_to_output = "D:\\My_Data\\My_Market_Place_Data\\sku_list_results.csv";
		String file_to_output ="/home/sduprey/My_Data/My_Market_Place_Data/sku_list_results.csv";
		//String file_to_output = args[1];

		populatingMapFromFile(file_to_inject_path);
		crawlingForPrice();
		writingResultFile(file_to_output);
	}

	public static void crawlingForPrice(){
		// we here fetch vendor and prices from Cdiscount.com
		for (String my_skuid :skuid){		
			List<OutRow> my_rows = get_sku_front_price(my_skuid);
			output_results.addAll(my_rows);
		}
	}

	public static List<OutRow> get_sku_front_price(String my_skuid){
		CrawlInformation crawlInfo = standard_crawling_without_pagination(my_skuid);
		PresenceInformation infos = populatePriceMap(my_skuid,crawlInfo);
		if (infos.getMissing_information()){
			// we missed some prices for the current sku : most likey due to pagination issues
			System.out.println("We missed some prices for the current sku : most likey due to pagination issues\n");
			System.out.println("We'll try a violenter crawler \n");
			int nb_try = 1;
			int nb_seconds=1;
			// we try ten times, and each time we wait a little more
			while(infos.getMissing_information() &&  nb_try <= max_number_tries){
				System.out.println(nb_try +" trying times with loading waiting time at"+nb_seconds);
				infos=ajax_crawler_for_pagination(my_skuid,nb_seconds);
				nb_try++;
				nb_seconds++;
			}
		}
		// to do : merge all rows
		return infos.getFound_Rows();
	}

	public static void writingResultFile(String output_file_path){
		System.out.println("Writing output results to file : "+output_file_path);
		BufferedWriter writer = null;
		// we open the file
		try {
			writer=  new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_file_path), "UTF-8"));
			writer.write("PRODUCTID;SHOPNAME;REMAINING;STATUS;GIVEN_PRICE;FOUND_PRICE\n");
			// we open the database
			for (OutRow row:output_results){
				writer.write(row.getProductid()+";"+row.getShopname()+";"+row.getGiven_remaining_quantity()+";"+row.getStatus()+";"+row.getGiven_price()+";"+row.getFound_price()+"\n");
			}
		} catch (IOException e) {
			if(writer != null){
				try {
					writer.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			e.printStackTrace();
		}finally{
			if(writer != null){
				try {
					writer.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
	}

	public static CrawlInformation standard_crawling_without_pagination(String my_skuid){
		String my_url = cdiscount_url+my_skuid+".html";
		// fetching data using jQuery
		org.jsoup.nodes.Document doc = null;
		try {
			doc =  Jsoup.connect(my_url)
					//.userAgent("Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)")
					.userAgent("CdiscountBot-crawler")
					//.referrer("accounterlive.com")
					.ignoreHttpErrors(true)
					.timeout(0)
					.get();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Elements resellers  = doc.select(".slrName");
		Elements prices = doc.select("p.price");

		// converting the elements to String[]
		String[] reseller_names = new String[resellers.size()];
		int index=0;
		for (Element reseller : resellers) {
			reseller_names[index]=reseller.text();
			index++;
		}
		Double[] price_values = new Double[prices.size()];
		index=0;
		for (Element price : prices) {
			String matching_price = price.text();
			matching_price=matching_price.replace("\u20ac",".");
			try {
				price_values[index]=Double.valueOf(matching_price);
			} catch (NumberFormatException e){
				System.out.println("Trouble converting : "+matching_price);
				price_values[index]=new Double(0);
			}
			index++;
		}
		// filling now the prices map with the found values

		CrawlInformation crawl_info = new CrawlInformation();
		crawl_info.setResellers_names(reseller_names);
		crawl_info.setResellers_prices(price_values);
		return crawl_info;
	}

	public static CrawlInformation ajax_pagination_fetch_sku(String skuid){
		CrawlInformation crawlInfo = new CrawlInformation();
		return crawlInfo;
	}

	public static PresenceInformation ajax_crawler_for_pagination(String skuid, int waiting_times){		
		String my_url =cdiscount_url+skuid+".html";
		CrawljaxConfigurationBuilder builder = CrawljaxConfiguration.builderFor(my_url);
		// we just follow the pagination
		builder.crawlRules().click("a").withAttribute("class","mpNext");
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

				org.jsoup.nodes.Document doc = Jsoup.parse(content_to_parse);
				Elements resellers = doc.select(".slrName");
				Elements prices = doc.select("p.price");


				// converting the elements to String[]
				String[] reseller_names = new String[resellers.size()];
				int index=0;
				for (Element reseller : resellers) {
					reseller_names[index]=reseller.text();
					index++;
				}
				Double[] price_values = new Double[prices.size()];
				index=0;
				for (Element price : prices) {
					String matching_price = price.text();
					matching_price=matching_price.replace("\u20ac",".");
					try {
						price_values[index]=Double.valueOf(matching_price);
					} catch (NumberFormatException e){
						System.out.println("Trouble converting : "+matching_price);
						price_values[index]=new Double(0);
					}
					index++;
				}
				// filling now the prices map with the found values

				CrawlInformation crawl_info = new CrawlInformation();
				crawl_info.setResellers_names(reseller_names);
				crawl_info.setResellers_prices(price_values);
				ajax_results.add(crawl_info);
			}
			@Override
			public String toString() {
				return "Market Place Plugin";
			}
		});

		CrawljaxRunner crawljax = new CrawljaxRunner(builder.build());
		crawljax.call();
		// concatenate the crawl info from each crawled page

		System.out.println("We found "+ajax_results.size() + " ajax pages");
		PresenceInformation infos = populatePriceMapFromAjax(skuid);
		return infos;
	}

	public static PresenceInformation populatePriceMapFromAjax(String skuid){
		PresenceInformation my_infos = new PresenceInformation();
		List<OutRow> found_rows = new ArrayList<OutRow>();
		List<OutRow> not_found_rows = new ArrayList<OutRow>();
		List<ResellerInfo> given_shopname_list = resellers.get(skuid);
		boolean any_missed = false;
		for (ResellerInfo my_info : given_shopname_list){	
			String given_shopname=my_info.getShopname();
			Double given_remaining_quantity=my_info.getRemainingQuantity();
			Double given_price=my_info.getIntegrationPrice();			
			OutRow row = new OutRow();
			row.setShopname(given_shopname);
			row.setGiven_remaining_quantity(given_remaining_quantity);
			row.setGiven_price(given_price);
			row.setProductid(skuid);
			// we go from a pessimistic point of view
			Double found_price = new Double(0);
			boolean found = false;
			for (CrawlInformation loc_info : ajax_results){
				String[] my_resellers=loc_info.getResellers_names();
				Double[] my_prices=loc_info.getResellers_prices();
				// we loop over each shopname to find the price
				// we here find the price & fill the matching status
				if (my_resellers.length != my_prices.length){
					System.out.println("Cdiscount has an offer on this product\n");
					if (my_resellers.length != (my_prices.length-1)){
						System.out.println("Warning there might be a mistake, check for yourself\n");		
					} else {
						System.out.println("The first price is the Cdiscount offer\n");
						// checking if we find the reseller
						for (int i=0;i<my_resellers.length;i++){
							if (given_shopname.equals(my_resellers[i])){
								found_price=my_prices[i+1];
								found=true;
								System.out.println("Price found for "+given_shopname+" : "+found_price);
								row.setFound_price(found_price);
								row.setStatus("OK");
							}
						}
					}
				} else {
					System.out.println("Cdiscount has no offer on this product\n");
					// checking if we find the reseller
					for (int i=0;i<my_resellers.length;i++){
						if (given_shopname.equals(my_resellers[i])){
							found_price=my_prices[i];
							found=true;
							row.setFound_price(found_price);
							row.setStatus("OK");
							System.out.println("Price found for "+given_shopname+" : "+found_price);
						}
					}
				}
			}
			if (!found){
				System.out.println("We didn't find any price for this reseller : "+given_shopname+" and for this sku : "+skuid);
				any_missed=true;
				not_found_rows.add(row);
			}
			else{
				found_rows.add(row);
				// we populate the map at the appropriate key/values
				mapped_prices.put(skuid+given_shopname, found_price);
			}
		}
		my_infos.setFound_Rows(found_rows);
		my_infos.setNot_found_Rows(not_found_rows);
		my_infos.setMissing_information(any_missed);
		return my_infos;
	}

	public static PresenceInformation populatePriceMap(String skuid, CrawlInformation crawlInfo ){
		PresenceInformation my_infos = new PresenceInformation();
		String[] my_resellers=crawlInfo.getResellers_names();
		Double[] my_prices=crawlInfo.getResellers_prices();
		List<OutRow> found_rows = new ArrayList<OutRow>();
		List<OutRow> not_found_rows = new ArrayList<OutRow>();
		List<ResellerInfo> given_shopname_list = resellers.get(skuid);
		boolean any_missed = false;
		// we loop over each shopname to find the price
		for (ResellerInfo my_info : given_shopname_list){	
			String given_shopname=my_info.getShopname();
			Double given_remaining_quantity=my_info.getRemainingQuantity();
			Double given_price=my_info.getIntegrationPrice();			
			OutRow row = new OutRow();
			row.setShopname(given_shopname);
			row.setGiven_remaining_quantity(given_remaining_quantity);
			row.setGiven_price(given_price);
			row.setProductid(skuid);
			// we go from a pessimistic point of view
			Double found_price = new Double(0);
			boolean found = false;
			// we here find the price & fill the matching status
			if (my_resellers.length != my_prices.length){
				System.out.println("Cdiscount has an offer on this product\n");
				if (my_resellers.length != (my_prices.length-1)){
					System.out.println("Warning there might be a mistake, check for yourself\n");		
				} else {
					System.out.println("The first price is the Cdiscount offer\n");
					// checking if we find the reseller
					for (int i=0;i<my_resellers.length;i++){
						if (given_shopname.equals(my_resellers[i])){
							found_price=my_prices[i+1];
							found=true;
							System.out.println("Price found for "+given_shopname+" : "+found_price);
							row.setFound_price(found_price);
							row.setStatus("OK");
						}
					}
				}
			} else {
				System.out.println("Cdiscount has no offer on this product\n");
				// checking if we find the reseller
				for (int i=0;i<my_resellers.length;i++){
					if (given_shopname.equals(my_resellers[i])){
						found_price=my_prices[i];
						found=true;
						row.setFound_price(found_price);
						row.setStatus("OK");
						System.out.println("Price found for "+given_shopname+" : "+found_price);
					}
				}
			}
			if (!found){
				System.out.println("We didn't find any price for this reseller : "+given_shopname+" and for this sku : "+skuid);
				any_missed=true;
				not_found_rows.add(row);
			}
			else{
				found_rows.add(row);
				// we populate the map at the appropriate key/values
				mapped_prices.put(skuid+given_shopname, found_price);
			}
		}
		my_infos.setFound_Rows(found_rows);
		my_infos.setNot_found_Rows(not_found_rows);
		my_infos.setMissing_information(any_missed);
		return my_infos;
	}

	public static void populatingMapFromFile(String inputFilePath){
		FileInputStream file = null; 
		try{
			file = new FileInputStream(new File(inputFilePath));
			// Create Workbook instance holding reference to .xlsx file
			// this format is explicitly for XLSX
			// XSSFWorkbook workbook = new XSSFWorkbook(file);
			// this format is explicitly for XLS
			org.apache.poi.ss.usermodel.Workbook workbook = WorkbookFactory.create(file);
			//Get first/desired sheet from the workbook
			// this format is explicitly for XLSX
			// XSSFSheet sheet = workbook.getSheetAt(0);
			// Extracting each row
			// this format is explicitly for XLS where we reed just the first sheet
			org.apache.poi.ss.usermodel.Sheet sheet = workbook.getSheetAt(0);
			System.out.println("Dealing with sheet" + sheet.getSheetName());
			//Iterate through each rows one by one
			Iterator<Row> rowIterator = sheet.iterator();
			int row_counter=0;
			while (rowIterator.hasNext()) 
			{
				row_counter++;
				Row row = rowIterator.next();
				//For each row, iterate through all the columns
				Iterator<Cell> cellIterator = row.cellIterator();
				if( row_counter == 1){
					// we here just want to extract the date
					while (cellIterator.hasNext()) 
					{
						Cell cell = cellIterator.next();
						switch (cell.getCellType()) 
						{
						case Cell.CELL_TYPE_NUMERIC:
							System.out.print(cell.getNumericCellValue() + "\t");
							break;
						case Cell.CELL_TYPE_STRING:
							System.out.print(cell.getStringCellValue() + "\t");
							break;
						}
					}
				}else {
					// we deal with each row
					// each must have exactly four columns
					if(cellIterator.hasNext()){
						Cell cell = cellIterator.next();
						String given_shopname = cell.getStringCellValue();	
						System.out.println(given_shopname);
						cell = cellIterator.next();
						String productid = cell.getStringCellValue();
						System.out.println(productid);
						cell = cellIterator.next();
						double integrationPrice = cell.getNumericCellValue();
						System.out.println(integrationPrice);
						cell = cellIterator.next();
						double remainingQuantity = cell.getNumericCellValue();
						System.out.println(remainingQuantity);
						// populating the map
						skuid.add(productid);
						// feeding the resellers map
						ResellerInfo reseller_info = new ResellerInfo();
						List<ResellerInfo> shopList = resellers.get(productid);
						if (shopList == null){
							shopList=new ArrayList<ResellerInfo>();
							resellers.put(productid,shopList);
						} 
						reseller_info.setIntegrationPrice(integrationPrice);
						reseller_info.setShopname(given_shopname);
						reseller_info.setRemainingQuantity(remainingQuantity);
						shopList.add(reseller_info);
					}
				}
			}
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}finally{
			try {
				file.close();
			} catch (Exception e){
				e.printStackTrace();
			}
		}
	}

	private static class PresenceInformation{
		Boolean missing_information = false;
		List<OutRow> found_Rows;
		public Boolean getMissing_information() {
			return missing_information;
		}
		public void setMissing_information(Boolean missing_information) {
			this.missing_information = missing_information;
		}
		public List<OutRow> getFound_Rows() {
			return found_Rows;
		}
		public void setFound_Rows(List<OutRow> found_Rows) {
			this.found_Rows = found_Rows;
		}
		public List<OutRow> getNot_found_Rows() {
			return not_found_Rows;
		}
		public void setNot_found_Rows(List<OutRow> not_found_Rows) {
			this.not_found_Rows = not_found_Rows;
		}
		List<OutRow> not_found_Rows;


	}

	private static class CrawlInformation{
		String[] resellers_names;
		Double[] resellers_prices;
		public String[] getResellers_names() {
			return resellers_names;
		}
		public void setResellers_names(String[] resellers_names) {
			this.resellers_names = resellers_names;
		}
		public Double[] getResellers_prices() {
			return resellers_prices;
		}
		public void setResellers_prices(Double[] resellers_prices) {
			this.resellers_prices = resellers_prices;
		}
	}

	private static class ResellerInfo{
		String shopname;
		Double remainingQuantity;
		Double integrationPrice;
		public String getShopname() {
			return shopname;
		}
		public void setShopname(String shopname) {
			this.shopname = shopname;
		}
		public Double getRemainingQuantity() {
			return remainingQuantity;
		}
		public void setRemainingQuantity(Double remainingQuantity) {
			this.remainingQuantity = remainingQuantity;
		}
		public Double getIntegrationPrice() {
			return integrationPrice;
		}
		public void setIntegrationPrice(Double integrationPrice) {
			this.integrationPrice = integrationPrice;
		}
	}

	private static class OutRow{
		String productid;
		String shopname;
		Double given_remaining_quantity;
		String status = "KO";
		Double given_price;
		Double found_price;
		public String getProductid() {
			return productid;
		}
		public void setProductid(String productid) {
			this.productid = productid;
		}
		public Double getGiven_remaining_quantity() {
			return given_remaining_quantity;
		}
		public void setGiven_remaining_quantity(Double given_remaining_quantity) {
			this.given_remaining_quantity = given_remaining_quantity;
		}
		public String getStatus() {
			return status;
		}
		public void setStatus(String status) {
			this.status = status;
		}
		public Double getGiven_price() {
			return given_price;
		}
		public void setGiven_price(Double given_price) {
			this.given_price = given_price;
		}
		public Double getFound_price() {
			return found_price;
		}
		public void setFound_price(Double found_price) {
			this.found_price = found_price;
		}
		public String getShopname() {
			return shopname;
		}
		public void setShopname(String shopname) {
			this.shopname = shopname;
		}
	}
} 
