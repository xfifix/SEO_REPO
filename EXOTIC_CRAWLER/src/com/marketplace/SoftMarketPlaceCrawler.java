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

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class SoftMarketPlaceCrawler {
	private static String cdiscount_url = "http://www.cdiscount.com/mp-9000-";
	private static Set<String> skuid = new HashSet<String>(); 
	private static Map<String,List<String>> shopname = new HashMap<String,List<String>>();
	private static Map<String,List<Double>> remaining_quantity = new HashMap<String,List<Double>>();
	private static Map<String,List<Double>> integration_price = new HashMap<String,List<Double>>();
	private static List<OutRow> output_results = new ArrayList<OutRow>();

	public static void main(String[] args){	
		if (args.length != 2){
			System.out.println("You must specify the input xls file and then the output csv file\n");
			System.out.println("Example : java MarketPlaceCrawler.java /home/sduprey/My_Data/FULFILLMENT/sku_list.xlsx /home/sduprey/My_Data/FULFILLMENT/sku_list_results.csv");
		}
		//String file_to_inject_path = "D:\\My_Data\\My_Market_Place_Data\\sku_list.xlsx";
		String file_to_inject_path =args[0];

		//String file_to_output = "D:\\My_Data\\My_Market_Place_Data\\sku_list_results.csv";
		String file_to_output = args[1];

		populatingMapFromFile(file_to_inject_path);
		crawlingForPrice();
		writingResultFile(file_to_output);
	}

	public static void writingResultFile(String output_file_path){
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

	public static void crawlingForPrice(){
		// we here fetch vendor and prices from Cdiscount.com
		for (String my_skuid :skuid){
			// fetching data using jQuery
			org.jsoup.nodes.Document doc;
			try{
				// we wait between 30 and 70 seconds
				doc =  Jsoup.connect(cdiscount_url+my_skuid+".html")
						.userAgent("Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)")
						.referrer("accounterlive.com")
						.ignoreHttpErrors(true)
						.timeout(0)
						.get();
				Elements resellers = doc.select(".slrName");
				Elements prices = doc.select("p.price");
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
				List<OutRow> my_rows = parseOutRow(my_skuid,reseller_names, price_values);
				output_results.addAll(my_rows);
			}
			catch (IOException e) {
				e.printStackTrace();
			} 
		}
	}

	public static List<OutRow> parseOutRow(String skuid, String[] my_resellers, Double[] my_prices){
		List<OutRow> my_rows = new ArrayList<OutRow>();
		
		List<String> given_shopname_list = shopname.get(skuid);
		List<Double> given_remaining_quantity_list = remaining_quantity.get(skuid);
		List<Double> given_price_list = integration_price.get(skuid);

		if (given_shopname_list.size() != given_price_list.size()){
			System.out.println("The xls file seems to cause some trouble");
			System.exit(0);
		}
		if (given_shopname_list.size() != given_remaining_quantity_list.size()){
			System.out.println("The xls file seems to cause some trouble");
			System.exit(0);	
		}

		// we loop over each shopname to find the price
		for (int counter=0;counter<given_shopname_list.size();counter++){
			String given_shopname=given_shopname_list.get(counter);
			Double given_remaining_quantity=given_remaining_quantity_list.get(counter);
			Double given_price=given_price_list.get(counter);			
			OutRow row = new OutRow();
			row.setShopname(given_shopname);
			row.setGiven_remaining_quantity(given_remaining_quantity);
			row.setGiven_price(given_price);
			row.setProductid(skuid);
			// we go from a pessimistic point of view
			row.setStatus("KO");
			Double found_price = new Double(0);
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
						row.setFound_price(found_price);
						row.setStatus("OK");
						System.out.println("Price found for "+given_shopname+" : "+found_price);
					}
				}
			}
			my_rows.add(row);
		}
		return my_rows;
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
						// feeding the map
						List<String> shopList = shopname.get(productid);
						if (shopList == null){
							shopList=new ArrayList<String>();
							shopname.put(productid,shopList);
						} 
						shopList.add(given_shopname);

						List<Double> priceList = integration_price.get(productid);
						if (priceList == null){
							priceList=new ArrayList<Double>();
							integration_price.put(productid,priceList);
						} 
						priceList.add(integrationPrice);

						List<Double> quantityList = remaining_quantity.get(productid);
						if (quantityList == null){
							quantityList=new ArrayList<Double>();
							remaining_quantity.put(productid,quantityList);
						} 
						quantityList.add(remainingQuantity);
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

	private static class OutRow{
		String productid;
		String shopname;
		Double given_remaining_quantity;
		String status;
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
