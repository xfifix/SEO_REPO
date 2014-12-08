package crawl4j.vsm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import crawl4j.urlutilities.URL_Utilities;

public class InsideMagasinQualityWorkerThread implements Runnable {
	private static String insert_statement="INSERT INTO RAYON_SIMILARITIES (MAGASIN, RAYON1, RAYON2, PERCENTAGE_EXACT, AVG_SIMILARITY, SIMILARITIES, LAST_UPDATE) VALUES (?,?,?,?,?,?,?)";
	private String magasin;
	private Map<String,List<URLContentInfo>> rayons_datas = new HashMap<String,List<URLContentInfo>>();
	private List<String> rayons;
	private Connection con;
	private double threshold = 0.8;

	public InsideMagasinQualityWorkerThread(Connection con, String magasin,List<String> rayons) throws SQLException{
		this.magasin=magasin;
		this.rayons=rayons;
		System.out.println("Computing metrics for field : "+" and for "+magasin +" :"+ this.magasin);
		this.con = con;
		for (String rayon : rayons){
			System.out.println("Getting content from rayon :"+rayon);
			List<URLContentInfo> info_list = rayons_datas.get(rayon);
			if (info_list==null){
				info_list=new ArrayList<URLContentInfo>();
				rayons_datas.put(rayon,info_list);
			}	
			// getting the URLs infos for each rayon
			PreparedStatement field_pst;
			try {
				field_pst  = this.con.prepareStatement("SELECT NB_ATTRIBUTES,ATTRIBUTES,URL,VENDOR,MAGASIN,RAYON,PRODUIT FROM CRAWL_RESULTS WHERE RAYON='" +rayon+ "'");
				ResultSet field_rs = field_pst.executeQuery();
				while (field_rs.next()) {
					URLContentInfo url_info = new URLContentInfo();
					int nb_attributes = field_rs.getInt(1);
					String attributes = field_rs.getString(2);
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
					info_list.add(url_info);
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				System.out.println("Database trouble with the rayon :"+rayon);
				e.printStackTrace();
			}
		}
	}

	public void run() {
		// we here compute the filling percentage for each rayon
		for (String rayon : rayons){
			List<URLContentInfo> rayon_infos = rayons_datas.get(rayon);
			Map<String, Integer> rayon_argument_counting = new HashMap<String, Integer>();
			System.out.println("Assessing " +rayon_infos.size()  + " URLs" );
			for (URLContentInfo rayon_info : rayon_infos){
				String attributes_listing = rayon_info.getAttributes();
				String url = rayon_info.getUrl();
				String checkType = URL_Utilities.checkType(url);
				if ("FicheProduit".equals(checkType)){
					if (attributes_listing.contains("|||")){
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

						}
					}
				}
			}
			// to do : save the results for the rayon
			savingDataArguments(rayon_argument_counting);
		}
		System.out.println(Thread.currentThread().getName()+" End");
	}

	private void savingDataArguments(Map<String, Integer> rayon_argument_counting ){
		System.out.println("Displaying arguments counting results \n");
		Iterator it = rayon_argument_counting.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry)it.next();
			String argument_name=(String)pairs.getKey();
			Integer count = (Integer)pairs.getValue();
			System.out.println(argument_name+" : "+count);
		}	
	}

	public static List<String> parse_arguments(String arguments_listing){
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
				output.add(description);
			}
		}
		return output;
	}

	private void save_database(RayonComparison comp){
		String urls_to_insert = "";
		try {
			PreparedStatement ps = con.prepareStatement(insert_statement);
			ps.setString(1, this.magasin);
			ps.setString(2, comp.getRayoni());
			ps.setString(3, comp.getRayonj());
			ps.setDouble(4, comp.getPercent_exactly_matching());
			ps.setDouble(5, comp.getAverage_similarity());
			urls_to_insert = getFileDoublons(comp);
			ps.setString(6,urls_to_insert);
			//urls.toString().getBytes(StandardCharsets.UTF_8);
			//ps.setBinaryStream(4,new ByteArrayInputStream(urls_to_print), urls_to_print.length);
			java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
			ps.setDate(7,sqlDate);
			ps.executeUpdate();
		} catch (SQLException e) {
			System.out.println("Trouble inserting "+comp.getRayoni()+" "+comp.getRayonj());
			System.out.println(urls_to_insert);
			e.printStackTrace();
		}
	}

	private String getFileDoublons(RayonComparison comp){
		List<RayonLevelDoublon>  doublon_list = comp.getDoublons();
		StringBuilder urls = new StringBuilder();
		for (RayonLevelDoublon doublon : doublon_list){
			urls.append(doublon.getURL1()+";"+doublon.getURL2()+"\n");
		}
		return  urls.toString();
	}
}
