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

import crawl4j.urlutilities.URL_Utilities;


public class ProcessOneMagasinQualityScore {

	private static String config_path = "/home/sduprey/My_Code/My_Java_Workspace/SIMILARITY_METRICS/config/";
	public static Properties properties;
	public  static File stop_words;
	public  static String properties_file_path;

	// threshold above which we deem the content too much similar !!
	//	private static double threshold = 0;
	private static Connection con = null;
	private static List<URLContentInfo> magasins_datas = new ArrayList<URLContentInfo>();
	private static int global_number_products_with_arguments = 0;
	private static Map<String, String> properties_map = new HashMap<String, String>();

	public static void main(String[] args) {
		String magasin_to_analyse ="musique-instruments";
		//String magasin_to_analyse ="dvd";
		String output_directory="/home/sduprey/My_Data/My_Outgoing_Data/My_Attributes_Filling";
		if (args.length >= 1){
			magasin_to_analyse = args[0];
		} 

		if (args.length == 0) {
			System.out.println("No magasin specified : choosing "+magasin_to_analyse);
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
		analyse_magasin(magasin_to_analyse,output_directory);
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

	private static void analyse_magasin(String magasin_to_analyse, String output_directory){
		Map<String, Integer> rayon_argument_counting = new HashMap<String, Integer>();
		System.out.println("Assessing " +magasins_datas.size()  + " URLs" );
		for (URLContentInfo rayon_info : magasins_datas){
			String attributes_listing = rayon_info.getAttributes();
			String url = rayon_info.getUrl();
			String checkType = URL_Utilities.checkType(url);
			if ("FicheProduit".equals(checkType)){
				if (attributes_listing.contains("|||")){
					global_number_products_with_arguments++;
					System.out.println(attributes_listing);
					List<String> arguments_list = parse_arguments(attributes_listing);
					for (String argument_string : arguments_list){

						Integer counter = rayon_argument_counting.get(argument_string);
						if (counter == null){
							counter = new Integer(1);
							rayon_argument_counting.put(argument_string,counter);
						} else {
							counter=counter+1;
							rayon_argument_counting.put(argument_string,counter);
						}
						//						if (counter >global_number_products_with_arguments){
						//							System.out.println("Trouble greater than 100%");
						//						}
					}
				}
			}
		}
		// to do : save the results for the rayon
		System.out.println("Saving the results as a csv file in : ");
		savingDataArguments(rayon_argument_counting,magasin_to_analyse,output_directory);
	}

	private static List<String> parse_arguments(String arguments_listing){
		List<String> output = new ArrayList<String>();
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
				output.add(value);
				//we here know that the description is present, we just don't add it
				System.out.println("Adding here the value  : "+value+" and description : "+description);
			}
		}
		return output;
	}

	private static void savingDataArguments(Map<String, Integer> rayon_argument_counting,String magasin_to_analyse, String output_directory){
		System.out.println("Displaying attributs counting results for magasin : "+magasin_to_analyse+ "\n");	
		BufferedWriter writer;
		try {
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_directory+"/"+magasin_to_analyse+".csv"), "UTF-8"));
			// we write the header
			writer.write("ATTRIBUTE_NAME;FILLED_PERCENTAGE\n");
			Iterator it = rayon_argument_counting.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pairs = (Map.Entry)it.next();
				String argument_name=(String)pairs.getKey();
				Integer count = (Integer)pairs.getValue();
				System.out.println("Attribut name : " +argument_name);
				double filled_percent =((double)count)/((double)global_number_products_with_arguments)*100;
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

	private static void fetch_magasin_info(String magasin_to_analyse){
		// getting the URLs infos for each rayon
		PreparedStatement field_pst;
		try {
			field_pst  = con.prepareStatement("SELECT NB_ATTRIBUTES,ATTRIBUTES,URL,VENDOR,MAGASIN,RAYON,PRODUIT FROM CRAWL_RESULTS WHERE MAGASIN='" +magasin_to_analyse+ "'");
			System.out.println("I am requesting the database, please wait a few seconds");
			ResultSet field_rs = field_pst.executeQuery();
			while (field_rs.next()) {
				URLContentInfo url_info = new URLContentInfo();
				int nb_attributes = field_rs.getInt(1);
				String attributes = field_rs.getString(2);
				System.out.println("Adding attributes "+attributes);
				String my_url = field_rs.getString(3);
				String my_vendor = field_rs.getString(4);
				String my_magasin = field_rs.getString(5);
				String my_rayon = field_rs.getString(6);
				String my_produit = field_rs.getString(7);
				url_info.setNb_attributes(nb_attributes);
				url_info.setAttributes(attributes);
				url_info.setUrl(my_url);
				url_info.setMagasin(my_magasin);
				url_info.setRayon(my_rayon);
				url_info.setProduit(my_produit);
				url_info.setVendor(my_vendor);
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