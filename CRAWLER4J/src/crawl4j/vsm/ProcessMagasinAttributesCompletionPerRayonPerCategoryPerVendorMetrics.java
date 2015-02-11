package crawl4j.vsm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;


public class ProcessMagasinAttributesCompletionPerRayonPerCategoryPerVendorMetrics {

	private static String config_path = "/home/sduprey/My_Code/My_Java_Workspace/SIMILARITY_METRICS/config/";
	public static Properties properties;
	public  static File stop_words;
	public  static String properties_file_path;

	// threshold above which we deem the content too much similar !!
	//	private static double threshold = 0;
	private static Connection con = null;
	private static List<URLContentInfo> magasins_datas = new ArrayList<URLContentInfo>();
	private static Map<String, String> properties_map = new HashMap<String, String>();

	private static String categoryString = "Catégorie";
	private static String unknownCategory = "UNKNOWN";

	public static void main(String[] args) {
		//String magasin_to_analyse ="informatique";
		String magasin_to_analyse ="musique-instruments";
		//String magasin_to_analyse ="dvd";
		String output_directory="/home/sduprey/My_Data/My_Outgoing_Data/My_Attributes_Filling";
		if (args.length >= 1){
			magasin_to_analyse = args[0];
		} 

		if (args.length == 0) {
			System.out.println("No magasin specified by parameters : choosing "+magasin_to_analyse);
		}

		if (args.length == 2){
			output_directory = args[1];
		}

		if (args.length == 1){
			System.out.println("No output directory specified : choosing "+output_directory);
		}

		loadProperties();
		// getting the french stop words 
		stop_words = new File(properties.getProperty("config.stop_words_path"));
		properties_file_path = properties.getProperty("config.properties_path");
		//magasin_to_filter=properties.getProperty("data.magasintofilter");
		String url = properties.getProperty("db.url");
		String user = properties.getProperty("db.user");
		String mdp = properties.getProperty("db.passwd");    
		// database variables

		System.out.println("Parsing the ID Model/Property referential file");
		parse_model_properties(properties_file_path);
		try {
			con = DriverManager.getConnection(url, user, mdp);
			fetch_magasin_info(magasin_to_analyse);
			//fetch_rayon();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble with the database");
			System.exit(0);
		}
		System.out.println("Launching the attributes completion metrics computation per category and per vendor for magasin : "+magasin_to_analyse);
		System.out.println("The result will be saved in the following directory : "+output_directory);				
		System.out.println("We dissociate Cdiscount and Market place");
		analyse_magasin_per_rayon_per_category_per_vendor(magasin_to_analyse,output_directory);
	}

	private static void parse_model_properties(String property_path_file){
		FileInputStream in = null;     
		BufferedReader br = null;
		String line = "";
		String header = null;
		String[] column_names = null;
		String cvsSplitBy = ";";
		try {
			in = new FileInputStream(property_path_file);
			br = new BufferedReader(new InputStreamReader(in, "UTF-8"));	
			// we skip the first line : the headers
			header = br.readLine();
			column_names = header.split(cvsSplitBy);
			while ((line = br.readLine()) != null) {
				String[] fields = line.split(cvsSplitBy);
				//System.out.println(fields[0]+fields[1]+fields[2]+fields[3]);
				String alreadythere = properties_map.get(fields[3]);
				String value_to_put = column_names[0]+" : "+fields[0]+"/"+column_names[1]+" : "+fields[1]+"/"+column_names[2]+" : "+fields[2];
				if (alreadythere != null){
					value_to_put=value_to_put+alreadythere;
				}
				properties_map.put(fields[3],value_to_put);
			} 
		}catch (IOException ex) {
			ex.printStackTrace();
			System.out.println("Trouble reading the property file : "+property_path_file);
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
				System.out.println("Trouble reading the property file : "+property_path_file);		
			}
		}
	}
	
	private static void analyse_magasin_per_rayon_per_category_per_vendor(String magasin_to_analyse, String output_directory){
		// Map to store the counter of product occurence for a certain category	and for a certain type of vendor	
		Map<String, Integer> category_vendor_counter = new HashMap<String, Integer>();
		// Map to store for each attribut of a category its own counter
		Map<String, Map<String, Integer>> attributs_count_inside_category_vendor_map = new HashMap<String, Map<String, Integer>>();

		System.out.println("Assessing " +magasins_datas.size()  + " URLs" );
		// Looping over the collected datas for the magasin
		for (URLContentInfo fiche_info : magasins_datas){
			String attributes_listing = fiche_info.getAttributes();
			String checkType = fiche_info.getPageType();
			String vendor = fiche_info.isCdiscountVendor() ? "Cdiscount" : "Market place";
			String rayon = fiche_info.getRayon();
			// we handle only the <Fiche product>
			if ("FicheProduit".equals(checkType)){
				// we make sure that attributes were found for our <Fiche product>
				if (attributes_listing.contains("|||")){
					// we parse the found attribute
					Map<String,String> arguments_map = parse_arguments(attributes_listing);
					// we locate the <Category> value in the list of our arguments 
					String category = arguments_map.get(categoryString);
					if (category == null){
						category=unknownCategory;
					}
					String keyValue = rayon + ";" + category + ";" + vendor;
					System.out.println("Adding a product to the rayon & category & vendor : "+keyValue);
					Integer counter = category_vendor_counter.get(keyValue);
					if (counter == null){
						counter = new Integer(1);
						category_vendor_counter.put(keyValue,counter);
					} else {
						counter=counter+1;
						category_vendor_counter.put(keyValue,counter);
					}
					System.out.println("Incrementing counter for each attribute inside the following category & vendor : "+keyValue);

					Map<String, Integer> attributs_count_inside_category = attributs_count_inside_category_vendor_map.get(keyValue);
					if (attributs_count_inside_category == null){
						attributs_count_inside_category = new HashMap<String, Integer>();
						attributs_count_inside_category_vendor_map.put(keyValue,attributs_count_inside_category);
					} 
					// iterating over every attribut found
					Iterator<Map.Entry<String,String>> arg_it = arguments_map.entrySet().iterator();
					while (arg_it.hasNext()) {
						Map.Entry<String,String> pairs = (Map.Entry<String,String>)arg_it.next();
						// we are here just interested by our argument naming
						String argument_name=pairs.getKey();
						Integer arg_counter = attributs_count_inside_category.get(argument_name);
						if (arg_counter == null){
							arg_counter = new Integer(1);
							attributs_count_inside_category.put(argument_name,arg_counter);
						} else {
							arg_counter=arg_counter+1;
							attributs_count_inside_category.put(argument_name,arg_counter);
						}
					}
				}
			}
		}
		// to do : save the results for the rayon
		System.out.println("Saving the results for magasin : "+magasin_to_analyse + " as a csv file in : " + output_directory);
		savingSingleFileDataArguments(category_vendor_counter,attributs_count_inside_category_vendor_map,magasin_to_analyse,output_directory);
	}

	private static Map<String,String> parse_arguments(String arguments_listing){
		Map<String,String> output = new  HashMap<String,String>();
		StringTokenizer arguments_tokenizer = new StringTokenizer(arguments_listing,"@@");
		while(arguments_tokenizer.hasMoreTokens()){
			String argument_pair = arguments_tokenizer.nextToken();
			StringTokenizer pair_tokenizer = new StringTokenizer(argument_pair,"|||");
			String value = "";
			if(pair_tokenizer.hasMoreTokens()){
				value = pair_tokenizer.nextToken();
			}
			if(pair_tokenizer.hasMoreTokens()){
				String description = pair_tokenizer.nextToken();
				output.put(value,description);
				//we here know that the description is present, we just don't add it
				System.out.println("Adding here the value  : "+value+" and description : "+description);
			}
		}
		return output;
	}

	private static void savingSingleFileDataArguments(Map<String, Integer> global_counter, Map<String, Map<String, Integer>> arguments_counter, String magasin_to_analyse, String output_directory){
		System.out.println("Displaying attributs counting results for magasin : "+magasin_to_analyse+ "\n");	

		// we loop over each category and create the matching result file
		Iterator<Map.Entry<String,Integer>> cat_counter_it = global_counter.entrySet().iterator();
		while (cat_counter_it.hasNext()) {
			Map.Entry<String,Integer> pairs = (Map.Entry<String,Integer>)cat_counter_it.next();
			// we are here just interested by our argument naming
			String category_name =pairs.getKey();
			Integer global_count =pairs.getValue();
			Map<String, Integer> rayon_argument_counting = arguments_counter.get(category_name);
			BufferedWriter writer;
			try {
				String category_name_for_file = category_name.replace(" ", "_");
				String output_file = output_directory+"/"+magasin_to_analyse+"_"+category_name_for_file+".csv";
				System.out.println("Writing the file : "+output_file);
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_file), "UTF-8"));
				// we write the header
				writer.write("ATTRIBUTE_NAME;FILLED_PERCENTAGE\n");
				Iterator<Map.Entry<String,Integer>> it = rayon_argument_counting.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry<String,Integer> local_pairs = (Map.Entry<String,Integer>)it.next();
					String argument_name=local_pairs.getKey();
					Integer count=local_pairs.getValue();
					System.out.println("Attribut name : " +argument_name);
					double filled_percent =((double)count)/((double)global_count)*100;
					System.out.println("Filled percentage : " +filled_percent +"%");
					if (filled_percent>= 100){
						System.out.println("Trouble greater than 100%");
						filled_percent=100;
					}
					String associated_properties = properties_map.get(argument_name);
					writer.write(argument_name+";"+Double.toString(filled_percent)+";"+associated_properties+"\n");
				}	
				writer.close();	
			} catch (UnsupportedEncodingException | FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
	}

	private static void fetch_magasin_info(String magasin_to_analyse){
		// getting the URLs infos for each rayon
		PreparedStatement field_pst;
		try {
			field_pst  = con.prepareStatement("SELECT NB_ATTRIBUTES,ATTRIBUTES,URL,MAGASIN,RAYON,PRODUIT,PAGE_TYPE,CDISCOUNT_VENDOR FROM CRAWL_RESULTS WHERE MAGASIN='" +magasin_to_analyse+ "'");
			System.out.println("I am requesting the database, please wait a few seconds");
			ResultSet field_rs = field_pst.executeQuery();
			while (field_rs.next()) {
				URLContentInfo url_info = new URLContentInfo();
				int nb_attributes = field_rs.getInt(1);
				String attributes = field_rs.getString(2);
				System.out.println("Adding attributes "+attributes);
				String my_url = field_rs.getString(3);
				String my_magasin = field_rs.getString(4);
				String my_rayon = field_rs.getString(5);
				String my_produit = field_rs.getString(6);
				String my_page_type = field_rs.getString(7);
				boolean isCdiscountVendor = field_rs.getBoolean(8);
				url_info.setNb_attributes(nb_attributes);
				url_info.setAttributes(attributes);
				url_info.setUrl(my_url);
				url_info.setMagasin(my_magasin);
				url_info.setRayon(my_rayon);
				url_info.setProduit(my_produit);
				url_info.setPageType(my_page_type);
				url_info.setCdiscountVendor(isCdiscountVendor);
				magasins_datas.add(url_info);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("Database trouble with the magasin :"+magasin_to_analyse);
			e.printStackTrace();
		}
	}

	private static void loadProperties(){
		properties = new Properties();
		try {
			properties.load(new FileReader(new File(config_path+"properties")));
		} catch (Exception e) {
			System.out.println("Failed to load properties file!!");
			e.printStackTrace();
		}
	}

	public String getProperty(String key){
		return properties.getProperty(key);
	}
	public Properties getProperties(){
		return properties;
	}
}