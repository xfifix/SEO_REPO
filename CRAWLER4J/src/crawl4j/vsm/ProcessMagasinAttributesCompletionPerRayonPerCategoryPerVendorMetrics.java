package crawl4j.vsm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import crawl4j.attributesutility.AttributesUtility;


public class ProcessMagasinAttributesCompletionPerRayonPerCategoryPerVendorMetrics {

	private static String similarity_conf_path = "/home/sduprey/My_Data/My_Similarity_Conf/similarity_properties";
	public static Properties properties;
	public static File stop_words;
	public static String properties_file_path;
	public static String category_mapping_file_path;

	// threshold above which we deem the content too much similar !!
	//	private static double threshold = 0;
	private static Connection con = null;
	private static List<URLContentInfo> magasins_datas = new ArrayList<URLContentInfo>();
	private static Map<String, String> properties_map = new HashMap<String, String>();

	// category mapping cache
	private static List<CategoryInfo> category_datas = new ArrayList<CategoryInfo>();

	private static String categoryString = "Catégorie";
	private static String unknownCategory = "UNKNOWN";

	public static void main(String[] args) {
		loadProperties();
		//String magasin_to_analyse ="informatique";
		//String magasin_to_analyse ="musique-instruments";
		//String magasin_to_analyse ="dvd";
		//String magasin_to_analyse =properties.getProperty("config.default_magasin");
		String output_directory=properties.getProperty("config.output_directory");
		String magasin_to_analyse ="musique-instruments";
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
		// getting the french stop words 
		stop_words = new File(properties.getProperty("config.stop_words_path"));
		properties_file_path = properties.getProperty("config.properties_path");
		category_mapping_file_path = properties.getProperty("config.category_model_mappping");
		//magasin_to_filter=properties.getProperty("data.magasintofilter");

		System.out.println("Saving output file to directory : "+output_directory);
		String url = properties.getProperty("db.url");
		String user = properties.getProperty("db.user");
		String mdp = properties.getProperty("db.passwd");    
		// database variables
		System.out.println("Parsing the ID Model/Property referential file");
		parse_model_properties(properties_file_path);
		build_category_mapping(category_mapping_file_path);

		try {
			con = DriverManager.getConnection(url, user, mdp);
			fetch_magasin_info(magasin_to_analyse);
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

	private static void build_category_mapping(String category_mapping_file_path){
		//reading the file
		FileInputStream in = null;     
		BufferedReader br = null;
		String line = "";
		String header = null;
		String[] column_names = null;
		System.out.println("Column headers : "+column_names);
		String cvsSplitBy = ";";
		try {
			in = new FileInputStream(category_mapping_file_path);
			br = new BufferedReader(new InputStreamReader(in, "UTF-8"));	
			// we skip the first line : the headers
			header = br.readLine();
			column_names = header.split(cvsSplitBy);
			while ((line = br.readLine()) != null) {
				String[] fields = line.split(cvsSplitBy);		
				CategoryInfo row_info = new CategoryInfo();
				row_info.setCategoryId(fields[0]);
				row_info.setCode(fields[1]);
				row_info.setNiv1(fields[2]);
				row_info.setNiv2(fields[3]);
				row_info.setNiv3(fields[4]);
				row_info.setNiv4(fields[5]);
				row_info.setModelid(fields[6]);
				row_info.setName(fields[7]);
				category_datas.add(row_info);
			} 
		}catch (IOException ex) {
			ex.printStackTrace();
			System.out.println("Trouble reading the property file : "+category_mapping_file_path);
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
				System.out.println("Trouble reading the property file : "+category_mapping_file_path);		
			}
		}
	}

	private static String find_relevant_category_model(String front_category_text){
		// we search the level 4
		Set<String> modelIds = new HashSet<String>();
		Set<String> categoryIds = new HashSet<String>();		
		boolean found = false;
		for (CategoryInfo category_infos : category_datas){
			if (category_infos.getNiv4().contains(front_category_text)){
				categoryIds.add(category_infos.getCategoryId());
				modelIds.add(category_infos.getModelid()+":"+category_infos.getName());
				found=true;
			}
		}

		if (!found){
			for (CategoryInfo category_infos : category_datas){
				if (category_infos.getNiv3().contains(front_category_text)){
					categoryIds.add(category_infos.getCategoryId());
					modelIds.add(category_infos.getModelid()+":"+category_infos.getName());
					found=true;
				}
			}
		}

		if (!found){
			for (CategoryInfo category_infos : category_datas){
				if (category_infos.getNiv2().contains(front_category_text)){
					categoryIds.add(category_infos.getCategoryId());
					modelIds.add(category_infos.getModelid()+":"+category_infos.getName());
					found=true;
				}
			}
		}

		if (!found){
			for (CategoryInfo category_infos : category_datas){
				if (category_infos.getNiv1().contains(front_category_text)){
					categoryIds.add(category_infos.getCategoryId());
					modelIds.add(category_infos.getModelid()+":"+category_infos.getName());
					found=true;
				}
			}
		}

		StringBuilder response = new StringBuilder();
		for (String catIds : categoryIds){
			response.append(catIds);
			response.append(",");
		}
		response.append(";");
		for (String modIds : modelIds){
			response.append(modIds);
			response.append(",");
		}
		response.append(";");
		return response.toString();
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
		Map<String, Integer> rayon_counter = new HashMap<String, Integer>();
		// Map to store the counter of product occurence for a certain category	and for a certain type of vendor	
		Map<String, Integer> rayon_category_vendor_key_counter = new HashMap<String, Integer>();
		// Map to store for each attribut of a category its own counter
		Map<String, Map<String, Integer>> attributs_count_inside_rayon_category_vendor_key_map = new HashMap<String, Map<String, Integer>>();

		System.out.println("Assessing " +magasins_datas.size()  + " URLs" );
		// Looping over the collected datas for the magasin
		for (URLContentInfo fiche_info : magasins_datas){
			String attributes_listing = fiche_info.getAttributes();
			String checkType = fiche_info.getPageType();
			String vendor = fiche_info.isCdiscountVendor() ? "Cdiscount" : "Market place";
			String rayon = fiche_info.getRayon();
			// we handle only the <Fiche product>
			if ("FicheProduit".equals(checkType)){
				// we here count the number of "Fiche Produit" per rayon
				Integer rayon_count = rayon_counter.get(rayon);
				if (rayon_count == null){
					rayon_count = new Integer(1);
					rayon_counter.put(rayon,rayon_count);
				} else {
					rayon_count=rayon_count+1;
					rayon_counter.put(rayon,rayon_count);
				}
				// we make sure that attributes were found for our <Fiche product>
				// we parse the found attribute
				Map<String,String> arguments_map = AttributesUtility.unserializeJSONStringtoAttributesMap(attributes_listing);
				// we locate the <Category> value in the list of our arguments 
				String category = arguments_map.get(categoryString);
				if (category == null){
					category=unknownCategory;
				}
				String keyValue = rayon + ";" + vendor + ";" + category;
				System.out.println("Adding a product to the rayon & category & vendor : "+keyValue);
				Integer counter = rayon_category_vendor_key_counter.get(keyValue);
				if (counter == null){
					counter = new Integer(1);
					rayon_category_vendor_key_counter.put(keyValue,counter);
				} else {
					counter=counter+1;
					rayon_category_vendor_key_counter.put(keyValue,counter);
				}
				System.out.println("Incrementing counter for each attribute inside the following category & vendor : "+keyValue);

				Map<String, Integer> attributs_count_inside_category = attributs_count_inside_rayon_category_vendor_key_map.get(keyValue);
				if (attributs_count_inside_category == null){
					attributs_count_inside_category = new HashMap<String, Integer>();
					attributs_count_inside_rayon_category_vendor_key_map.put(keyValue,attributs_count_inside_category);
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
		// to do : save the results for the rayon
		System.out.println("Saving the results for magasin : "+magasin_to_analyse + " as a csv file in : " + output_directory);
		try {
			savingSingleFileDataArguments(rayon_counter,rayon_category_vendor_key_counter,attributs_count_inside_rayon_category_vendor_key_map,magasin_to_analyse,output_directory);
		} catch (IOException e) {
			String output_file = output_directory+"/"+magasin_to_analyse+".csv";
			System.out.println("Trouble writing the output path : "+output_file);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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

	private static void savingSingleFileDataArguments(Map<String, Integer> rayon_counter, Map<String, Integer> global_counter, Map<String, Map<String, Integer>> arguments_counter, String magasin_to_analyse, String output_directory) throws IOException{
		System.out.println("Displaying attributs counting results for magasin : "+magasin_to_analyse+ "\n");	
		System.out.println("Writing a single file from magasin  : "+magasin_to_analyse+ "\n");	
		BufferedWriter writer;
		String output_file = output_directory+"/"+magasin_to_analyse+".csv";
		System.out.println("Writing the file : "+output_file);
		writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output_file), "UTF-8"));
		// we write the header
		writer.write("RAYON;VENDOR;CATEGORY;CATEGORYID*;MODELID*;CATEGORY_PER_VENDOR_PER_PERCENTAGE_FILLING;ATTRIBUTE_NAME*;FILLED_PERCENTAGE*\n");
		// we loop over each category and create the matching result file
		Iterator<Map.Entry<String,Integer>> cat_counter_it = global_counter.entrySet().iterator();
		while (cat_counter_it.hasNext()) {
			Map.Entry<String,Integer> pairs = (Map.Entry<String,Integer>)cat_counter_it.next();
			// we are here just interested by our argument naming
			String category_name =pairs.getKey();
			Integer global_count =pairs.getValue();

			Map<String, Integer> rayon_argument_counting = arguments_counter.get(category_name);
			String category_text = extract_category_from_name(category_name);
			String rayon_text = extract_rayon_from_name(category_name);	
			Integer rayon_global_count =rayon_counter.get(rayon_text);
			String category_model_id =find_relevant_category_model(category_text);
			String category_name_to_write = category_name.replace(" ","_");
			// we write the rayon;category;vendor
			writer.write(category_name_to_write+";"+category_model_id);
			// computing and writing the category per vendor per rayon filling percentage
			double cat_filled_percent =((double)global_count)/((double)rayon_global_count)*100;
			writer.write(Double.toString(cat_filled_percent)+";");
			// we then write the attributes : beware the rows won't have the same number of parameters
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
				// we here don't write the properties of the attribute
				//String associated_properties = properties_map.get(argument_name);
				//writer.write(argument_name+";"+Double.toString(filled_percent)+";"+associated_properties+"\n");
				// we just write the attribute
				String argument_name_to_write = argument_name.replace("\n", " ");
				writer.write(argument_name_to_write+";"+Double.toString(filled_percent)+";");
			}	
			writer.write("\n");
		} 
		writer.close();	
	}

	private static String extract_category_from_name(String category_key){
		String[] listing = category_key.split(";");
		return listing[2];
	}

	private static String extract_rayon_from_name(String category_key){
		String[] listing = category_key.split(";");
		return listing[0];
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
			properties.load(new FileReader(new File(similarity_conf_path)));
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


	static class CategoryInfo{
		private String categoryId;
		private String code;
		private String niv1;
		private String niv2;
		private String niv3;
		private String niv4;
		private String modelid;
		private String name;
		public String getCategoryId() {
			return categoryId;
		}
		public void setCategoryId(String categoryId) {
			this.categoryId = categoryId;
		}
		public String getCode() {
			return code;
		}
		public void setCode(String code) {
			this.code = code;
		}
		public String getNiv1() {
			return niv1;
		}
		public void setNiv1(String niv1) {
			this.niv1 = niv1;
		}
		public String getNiv2() {
			return niv2;
		}
		public void setNiv2(String niv2) {
			this.niv2 = niv2;
		}
		public String getNiv3() {
			return niv3;
		}
		public void setNiv3(String niv3) {
			this.niv3 = niv3;
		}
		public String getNiv4() {
			return niv4;
		}
		public void setNiv4(String niv4) {
			this.niv4 = niv4;
		}
		public String getModelid() {
			return modelid;
		}
		public void setModelid(String modelid) {
			this.modelid = modelid;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
	}
}