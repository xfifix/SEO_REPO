package com.similarity.computing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import com.similarity.parameter.KriterParameter;
import com.statistics.processing.CatalogEntry;
import com.statistics.processing.StatisticsUtility;

public class SimilarityComputingNoFetchWorkerThread implements Runnable {
	private Connection con;
	private Random my_rand = new Random();
	private Map<String, List<CatalogEntry>> my_categories_to_compute  = new HashMap<String, List<CatalogEntry>>();
	// beware static shared global cache for unfetched skus
	private Map<CatalogEntry, Set<String>> unfetched_skus_local_cache = new HashMap<CatalogEntry, Set<String>>();

	private static String select_entry_from_rayon = " select SKU, CATEGORIE_NIVEAU_1, CATEGORIE_NIVEAU_2, CATEGORIE_NIVEAU_3, CATEGORIE_NIVEAU_4,  LIBELLE_PRODUIT, MARQUE, DESCRIPTION_LONGUEUR80, VENDEUR, ETAT, RAYON FROM CATALOG where RAYON=?";
	private static String select_entry_from_category1 = " select SKU, CATEGORIE_NIVEAU_1, CATEGORIE_NIVEAU_2, CATEGORIE_NIVEAU_3, CATEGORIE_NIVEAU_4,  LIBELLE_PRODUIT, MARQUE, DESCRIPTION_LONGUEUR80, VENDEUR, ETAT, RAYON FROM CATALOG where CATEGORIE_NIVEAU_1=?";
	private static String select_entry_from_category3 = " select SKU, CATEGORIE_NIVEAU_1, CATEGORIE_NIVEAU_2, CATEGORIE_NIVEAU_3, CATEGORIE_NIVEAU_4,  LIBELLE_PRODUIT, MARQUE, DESCRIPTION_LONGUEUR80, VENDEUR, ETAT, RAYON FROM CATALOG where CATEGORIE_NIVEAU_3=?";
	private static String select_entry_from_category2 = " select SKU, CATEGORIE_NIVEAU_1, CATEGORIE_NIVEAU_2, CATEGORIE_NIVEAU_3, CATEGORIE_NIVEAU_4,  LIBELLE_PRODUIT, MARQUE, DESCRIPTION_LONGUEUR80, VENDEUR, ETAT, RAYON FROM CATALOG where CATEGORIE_NIVEAU_2=?";

	
	//private static String insert_cds_statement = "INSERT INTO CDS_SIMILAR_PRODUCTS(SKU,SKU1,SKU2,SKU3,SKU4,SKU5,SKU6) VALUES(?,?,?,?,?,?,?)";
	private static String update_catalog_statement = "UPDATE CATALOG SET SKU1=?,SKU2=?,SKU3=?,SKU4=?,SKU5=?,SKU6=?,TO_FETCH=false where SKU=?";

	private Map<String,List<String>> matching_skus = new HashMap<String,List<String>>();


	public SimilarityComputingNoFetchWorkerThread(Connection con, Map<String, List<CatalogEntry>>  to_fetch) throws SQLException{
		this.con = con;
		this.my_categories_to_compute = to_fetch;
	}

	public void run() {
		String category_to_debug="";
		try {  
			Iterator<Entry<String, List<CatalogEntry>>> it = my_categories_to_compute.entrySet().iterator();
			// dispatching to threads
			while (it.hasNext()){	
				Map.Entry<String, List<CatalogEntry>> pairs = (Map.Entry<String, List<CatalogEntry>>)it.next();
				String category=pairs.getKey();
				List<CatalogEntry> my_data = pairs.getValue();
				category_to_debug=category;
				System.out.println(Thread.currentThread()+" Dealing with category : "+category);
				System.out.println(Thread.currentThread()+" Category skus all fetched for data : "+category);
				List<CatalogEntry> my_tofetch_data = filterToFetchData(my_data);
				computeResumableDataList(my_tofetch_data,my_data);
				//saving_similar_step_by_step();
				saving_similar();
				System.gc();
			}		
			// dealing with unfetched skus
			// we loop over each sku and get back to fomer category level to find matching offer
			System.out.println("Getting back to category level 3 to get products with missing similar products (<6) : number = "+unfetched_skus_local_cache.size());
			if (unfetched_skus_local_cache.size()>0){
				backup_category3();
			}
			System.out.println("Getting back to category level 2 to get products with missing similar products (<6) : number = "+unfetched_skus_local_cache.size());
			if (unfetched_skus_local_cache.size()>0){
				backup_category2();
			}
			System.out.println("Getting back to category level 1 to get products with missing similar products (<6) : number = "+unfetched_skus_local_cache.size());
			if (unfetched_skus_local_cache.size()>0){
				backup_category1();
			}			;
			// saving the current batch
			System.out.println("Saving the last batch from category level 3 2 1");
			saving_similar();
			matching_skus.clear();
			if (unfetched_skus_local_cache.size()>0){
				System.out.println("We have still products with less than 6 similar products : "+unfetched_skus_local_cache.size());
				System.out.println("We'll complete the similar products with rayon level similar_products ");
				backup_rayon();
			}
			System.out.println("Saving the last batch from rayon");
			//saving_similar_step_by_step();
			saving_similar();
			matching_skus.clear();

			close_connection();
		} catch (Exception ex) {
			System.out.println("Trouble with category : "+category_to_debug);
			ex.printStackTrace();
		} finally {
			try {
				if (con != null) {
					con.close();
				}
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
	}



	public boolean updateDataList(CatalogEntry current_entry, List<CatalogEntry> my_data){
		boolean done = false;
		if (my_data.size() > KriterParameter.kriter_threshold){
			// we do it the standard way
			done = find_my_extra_similar_products(current_entry,my_data);
		} else if (my_data.size() <= KriterParameter.kriter_threshold) {
			Set<String> current_similars = unfetched_skus_local_cache.get(current_entry);
			for (CatalogEntry to_add : my_data){
				if (!current_entry.getSKU().equals(to_add.getSKU())){
					current_similars.add(to_add.getSKU());
				}
			}
			if (current_similars.size()>= KriterParameter.kriter_threshold){
				matching_skus.put(current_entry.getSKU(),new ArrayList<String>(current_similars));
				if ((matching_skus.size() != 0) && matching_skus.size() % KriterParameter.batch_size == 0 ){
					saving_similar();
					matching_skus.clear();
				}
				done = true;
			} else {
				unfetched_skus_local_cache.put(current_entry,current_similars);
			}
		}
		return done;
	}

	public void computeResumableDataList(List<CatalogEntry> my_tofetch_data, List<CatalogEntry> my_data){
		if (my_data.size() > KriterParameter.kriter_threshold && my_data.size()<= KriterParameter.computing_max_list_size){
			// we do it the standard way
			find_my_similar_products(my_tofetch_data,my_data);
		} else if (my_data.size() <= KriterParameter.kriter_threshold) {
			smallCategoryCase(my_tofetch_data,my_data);
		}else if (my_data.size() > KriterParameter.computing_max_list_size) {
			// we here have to restrain ourselves
			// we do it randomly
			// but we should get a more proper criteria (business value, clicking trend)
			find_similar_with_restriction(my_tofetch_data,my_data);	
		}
		System.gc();
	}

	public void smallCategoryCase(List<CatalogEntry> my_tofetch_data, List<CatalogEntry> my_data){
		for (CatalogEntry to_process : my_tofetch_data){
			Set<String> similars = new HashSet<String>();
			for (CatalogEntry to_add : my_data){
				if (!to_process.getSKU().equals(to_add.getSKU())){
					similars.add(to_add.getSKU());
				}
			}
			// the SKU is not fully computed here as the size of my data is inferior to the threshold minus one
			unfetched_skus_local_cache.put(to_process,similars);
		}
		
	}

	public void find_restricted_similar(List<CatalogEntry> entries){
		int size_list = entries.size();
		System.out.println(Thread.currentThread() +" Beginning to compute distance matrix from "+size_list);
		for (int i=0;i<size_list;i++){
			CatalogEntry current_entry = entries.get(i);
			if (i!=0 && i%KriterParameter.displaying_threshold == 0){
				System.out.println(Thread.currentThread() +" Having computed distance matrix "+i+" from "+size_list);
			}
			String current_entry_brand = current_entry.getMARQUE();
			List<CatalogEntry> my_shrunk_entries = shrink_with_brand_and_max_size(current_entry_brand,entries);
			int restricted_size_list = my_shrunk_entries.size();
			Double[] vector_list = new Double[restricted_size_list]; 
			// computing the vector distance
			for (int j= 0;j<restricted_size_list;j++){
				CatalogEntry entryj = my_shrunk_entries.get(j);
				Double innerDistance = computeWeightedDistance(current_entry,entryj);
				vector_list[j] = innerDistance;
			}
			// sorting the array and keeping the indexes
			DescendingArrayIndexComparator comparator = new DescendingArrayIndexComparator(vector_list);
			Integer[] indexes = comparator.createIndexArray();
			Arrays.sort(indexes, comparator);

			List<String> similars = new ArrayList<String>();
			// adding the 6 first closest skus
			similars.add(my_shrunk_entries.get(indexes[0]).getSKU());
			similars.add(my_shrunk_entries.get(indexes[1]).getSKU());
			similars.add(my_shrunk_entries.get(indexes[2]).getSKU());
			similars.add(my_shrunk_entries.get(indexes[3]).getSKU());
			similars.add(my_shrunk_entries.get(indexes[4]).getSKU());
			similars.add(my_shrunk_entries.get(indexes[5]).getSKU());
			matching_skus.put(current_entry.getSKU(),similars);
			if ((matching_skus.size() != 0) && matching_skus.size() % KriterParameter.batch_size == 0 ){
				saving_similar();
				matching_skus.clear();
			}
		}
	}

	public void find_similar(List<CatalogEntry> my_entries){
		int size_list = my_entries.size();

		System.out.println(Thread.currentThread() +" Beginning to compute distance matrix from "+size_list);

		for (int i=0;i<size_list;i++){
			if (i!=0 && i%KriterParameter.displaying_threshold == 0){
				System.out.println(Thread.currentThread() +" Having computed distance matrix "+i+" from "+size_list);
			}
			CatalogEntry current_entry = my_entries.get(i);
			Double[] vector_list = new Double[size_list]; 
			for (int j= 0;j<size_list;j++){
				CatalogEntry entryj = my_entries.get(j);
				Double innerDistance = computeWeightedDistance(current_entry,entryj);
				vector_list[j] = innerDistance;
			}
			// sorting the array and keeping the indexes
			DescendingArrayIndexComparator comparator = new DescendingArrayIndexComparator(vector_list);
			Integer[] indexes = comparator.createIndexArray();
			Arrays.sort(indexes, comparator);
			List<String> similars = new ArrayList<String>();
			// adding the 6 first closest skus
			similars.add(my_entries.get(indexes[0]).getSKU());
			similars.add(my_entries.get(indexes[1]).getSKU());
			similars.add(my_entries.get(indexes[2]).getSKU());
			similars.add(my_entries.get(indexes[3]).getSKU());
			similars.add(my_entries.get(indexes[4]).getSKU());
			similars.add(my_entries.get(indexes[5]).getSKU());
			matching_skus.put(current_entry.getSKU(),similars);
			if ((matching_skus.size() != 0) && matching_skus.size() % KriterParameter.batch_size == 0 ){
				saving_similar();
				matching_skus.clear();
			}
		}
	}

	public List<CatalogEntry> filterCDSAvailableEntries(List<CatalogEntry> my_data){
		List<CatalogEntry> filtered_List = new ArrayList<CatalogEntry>();
		for (CatalogEntry entry : my_data){
			if (("CDS".equals(entry.getVENDEUR()))&&("non épuisé".equals(entry.getETAT()))) { 
				filtered_List.add(entry);
			}
		}
		return filtered_List; 
	}


	public List<CatalogEntry> filterBrandAvailableEntries(String brandToFilter, List<CatalogEntry> my_data){
		List<CatalogEntry> filtered_List = new ArrayList<CatalogEntry>();
		for (CatalogEntry entry : my_data){
			if ((brandToFilter.equals(entry.getMARQUE()))) { 
				filtered_List.add(entry);
			}
		}
		return filtered_List; 
	}

	public List<CatalogEntry> filterToFetchData(List<CatalogEntry> my_data){
		List<CatalogEntry> filtered_List = new ArrayList<CatalogEntry>();
		for (CatalogEntry entry : my_data){
			if (entry.getTO_FETCH()) { 
				filtered_List.add(entry);
			}
		}
		return filtered_List; 
	}

	public void backup_category3() throws SQLException{
		Iterator<Entry<CatalogEntry, Set<String>>> it = unfetched_skus_local_cache.entrySet().iterator();	
		List<CatalogEntry> to_remove = new ArrayList<CatalogEntry>();
		int global_size_to_process = unfetched_skus_local_cache.size();
		int iteration =1;
		while (it.hasNext()){
			if (iteration%KriterParameter.displaying_threshold == 0){
				System.out.println(Thread.currentThread() +" Having computed remaining SKUs "+iteration+" from "+global_size_to_process);
			}
			Map.Entry<CatalogEntry, Set<String>> pairs = (Map.Entry<CatalogEntry, Set<String>>)it.next();
			CatalogEntry current_entry=pairs.getKey();			
			List<CatalogEntry> newSet = fetch_category_data3(current_entry.getCATEGORIE_NIVEAU_3());
			if (updateDataList(current_entry,newSet)){
				to_remove.add(current_entry);	
			}
			iteration++;
		}

		for (CatalogEntry torem : to_remove){
			unfetched_skus_local_cache.remove(torem);
		}
	}

	public void backup_category2() throws SQLException{
		int global_size_to_process = unfetched_skus_local_cache.size();
		int iteration =1;
		Iterator<Entry<CatalogEntry, Set<String>>> it = unfetched_skus_local_cache.entrySet().iterator();	
		List<CatalogEntry> to_remove = new ArrayList<CatalogEntry>();
		while (it.hasNext()){
			if (iteration%KriterParameter.displaying_threshold == 0){
				System.out.println(Thread.currentThread() +" Having computed remaining SKUs "+iteration+" from "+global_size_to_process);
			}
			Map.Entry<CatalogEntry, Set<String>> pairs = (Map.Entry<CatalogEntry, Set<String>>)it.next();
			CatalogEntry current_entry=pairs.getKey();			
			List<CatalogEntry> newSet = fetch_category_data2(current_entry.getCATEGORIE_NIVEAU_2());
			if (updateDataList(current_entry,newSet)){
				to_remove.add(current_entry);	
			}
			iteration++;
		}

		for (CatalogEntry torem : to_remove){
			unfetched_skus_local_cache.remove(torem);
		}
	}

	public void backup_category1() throws SQLException{
		int global_size_to_process = unfetched_skus_local_cache.size();
		int iteration =1;
		Iterator<Entry<CatalogEntry, Set<String>>> it = unfetched_skus_local_cache.entrySet().iterator();	
		List<CatalogEntry> to_remove = new ArrayList<CatalogEntry>();
		while (it.hasNext()){
			if (iteration%KriterParameter.displaying_threshold == 0){
				System.out.println(Thread.currentThread() +" Having computed remaining SKUs "+iteration+" from "+global_size_to_process);
			}
			Map.Entry<CatalogEntry, Set<String>> pairs = (Map.Entry<CatalogEntry, Set<String>>)it.next();
			CatalogEntry current_entry=pairs.getKey();			
			List<CatalogEntry> newSet = fetch_category_data1(current_entry.getCATEGORIE_NIVEAU_1());
			if (updateDataList(current_entry,newSet)){
				to_remove.add(current_entry);	
			}
			iteration++;
		}

		for (CatalogEntry torem : to_remove){
			unfetched_skus_local_cache.remove(torem);
		}
	}

	public void backup_rayon() throws SQLException{
		int global_size_to_process = unfetched_skus_local_cache.size();
		int iteration =1;
		Iterator<Entry<CatalogEntry, Set<String>>> it = unfetched_skus_local_cache.entrySet().iterator();	
		List<CatalogEntry> to_remove = new ArrayList<CatalogEntry>();
		while (it.hasNext()){
			if (iteration%KriterParameter.displaying_threshold == 0){
				System.out.println(Thread.currentThread() +" Having computed remaining SKUs "+iteration+" from "+global_size_to_process);
			}
			Map.Entry<CatalogEntry, Set<String>> pairs = (Map.Entry<CatalogEntry, Set<String>>)it.next();
			CatalogEntry current_entry=pairs.getKey();			
			List<CatalogEntry> newSet = fetch_rayon_data(current_entry.getRAYON());
			if (updateDataList(current_entry,newSet)){
				to_remove.add(current_entry);	
			}
			iteration++;
		}

		for (CatalogEntry torem : to_remove){
			unfetched_skus_local_cache.remove(torem);
		}
	}

	public List<CatalogEntry> shrink_with_brand_and_max_size(String current_entry_brand, List<CatalogEntry> my_list){
		List<CatalogEntry> brandFilteredList = filterBrandAvailableEntries(current_entry_brand, my_list);
		if (brandFilteredList.size() >= KriterParameter.computing_max_list_size){
			Set<CatalogEntry> to_return = new HashSet<CatalogEntry>();
			// to_return is a set forbidding duplicated entries
			while (to_return.size() < KriterParameter.computing_max_list_size){
				CatalogEntry candidate = brandFilteredList.get(my_rand.nextInt(brandFilteredList.size()));
				to_return.add(candidate);
			}
			return new ArrayList<CatalogEntry>(to_return);
		} else {
			Set<CatalogEntry> to_return = new HashSet<CatalogEntry>();
			// to_return is a set forbidding duplicated entries
			to_return.addAll(brandFilteredList);
			while (to_return.size() < KriterParameter.computing_max_list_size){
				CatalogEntry candidate = my_list.get(my_rand.nextInt(my_list.size()));
				to_return.add(candidate);
			}
			return new ArrayList<CatalogEntry>(to_return);	
		}
	}

	public void saving_similar_step_by_step(){
		System.out.println("Inserting the batch "+matching_skus.size());
		Iterator<Entry<String, List<String>>> it = matching_skus.entrySet().iterator();
		int local_counter = 0;
		PreparedStatement st = null;
		String current_sku = "";
		while (it.hasNext()){
			try{
				st = con.prepareStatement(update_catalog_statement);
				local_counter++;
				Map.Entry<String, List<String>> pairs = (Map.Entry<String, List<String>>)it.next();
				current_sku=pairs.getKey();
				List<String> similars =pairs.getValue();
				//System.out.println("Current Sku :" + current_sku + similars);
				// preparing the statement
				st.setString(1,similars.get(0));
				st.setString(2,similars.get(1));
				st.setString(3,similars.get(2));
				st.setString(4,similars.get(3));
				st.setString(5,similars.get(4));				
				st.setString(6,similars.get(5));
				st.setString(7,current_sku);
				st.executeUpdate();
				st.close();
			} catch (SQLException e){
				if(e.getMessage().contains("cds_similar_products_sku_key")){
					System.out.println("Already inserted : "+current_sku);
				} else {
					e.printStackTrace();  
				}

				if (st != null){
					try {
						st.close();
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}	
		}
		System.out.println(Thread.currentThread()+"Committed " + local_counter + " updates");
	}

	public void saving_similar(){
		System.out.println(Thread.currentThread()+"Inserting the batch "+matching_skus.size());
		try{
			Iterator<Entry<String, List<String>>> it = matching_skus.entrySet().iterator();
			int local_counter = 0;
			con.setAutoCommit(false);
			PreparedStatement st = con.prepareStatement(update_catalog_statement);
			while (it.hasNext()){
				local_counter++;
				Map.Entry<String, List<String>> pairs = (Map.Entry<String, List<String>>)it.next();
				String current_sku=pairs.getKey();
				List<String> similars =pairs.getValue();
				//System.out.println("Current Sku :" + current_sku + similars);
				// preparing the statement
				st.setString(1,similars.get(0));
				st.setString(2,similars.get(1));
				st.setString(3,similars.get(2));
				st.setString(4,similars.get(3));
				st.setString(5,similars.get(4));				
				st.setString(6,similars.get(5));
				st.setString(7,current_sku);
				st.addBatch();
			}
			st.executeBatch();
			con.commit();
			st.close();
			System.out.println(Thread.currentThread()+"Committed " + local_counter + " updates");
		} catch (SQLException e){
			//System.out.println("Line already inserted : "+nb_lines);
			e.printStackTrace();  
			if (con != null) {
				try {
					con.rollback();
				} catch (SQLException ex1) {
					ex1.printStackTrace();
				}
			}
			e.printStackTrace();
		}	
	}

	public void find_similar_with_restriction(List<CatalogEntry> my_tofetch_entries, List<CatalogEntry> my_entries){
		int size_list = my_tofetch_entries.size();
		System.out.println(Thread.currentThread() +" Beginning to compute distance matrix from "+size_list);
		for (int i=0;i<size_list;i++){
			CatalogEntry current_entry = my_tofetch_entries.get(i);
			if (i%KriterParameter.displaying_threshold == 0){
				System.out.println(Thread.currentThread() +" Having computed distance matrix "+i+" from "+size_list);
			}
			String current_entry_brand = current_entry.getMARQUE();
			List<CatalogEntry> my_shrunk_entries = shrink_with_brand_and_max_size(current_entry_brand,my_entries);
			int restricted_size_list = my_shrunk_entries.size();

			Double[] vector_list = new Double[restricted_size_list]; 
			// computing the vector distance
			for (int j= 0;j<restricted_size_list;j++){
				CatalogEntry entryj = my_shrunk_entries.get(j);
				Double innerDistance = computeWeightedDistance(current_entry,entryj);
				vector_list[j] = innerDistance;
			}
			// sorting the array and keeping the indexes
			DescendingArrayIndexComparator comparator = new DescendingArrayIndexComparator(vector_list);
			Integer[] indexes = comparator.createIndexArray();
			Arrays.sort(indexes, comparator);

			List<String> similars = new ArrayList<String>();
			// adding the 6 first closest skus
			similars.add(my_shrunk_entries.get(indexes[0]).getSKU());
			similars.add(my_shrunk_entries.get(indexes[1]).getSKU());
			similars.add(my_shrunk_entries.get(indexes[2]).getSKU());
			similars.add(my_shrunk_entries.get(indexes[3]).getSKU());
			similars.add(my_shrunk_entries.get(indexes[4]).getSKU());
			similars.add(my_shrunk_entries.get(indexes[5]).getSKU());
			matching_skus.put(current_entry.getSKU(),similars);
			if ((matching_skus.size() != 0) && matching_skus.size() % KriterParameter.batch_size == 0 ){
				saving_similar();
				matching_skus.clear();
			}
		}
	}

	public boolean find_my_extra_similar_products(CatalogEntry current_entry, List<CatalogEntry> entries){

		boolean done = false;
		Set<String> current_similars = unfetched_skus_local_cache.get(current_entry);
		// sorting the array and keeping the indexes
		Double[] vector_list = computeDistanceVector(current_entry, entries);
		DescendingArrayIndexComparator comparator = new DescendingArrayIndexComparator(vector_list);
		Integer[] indexes = comparator.createIndexArray();
		Arrays.sort(indexes, comparator);
		int loc = 0;
		while (current_similars.size()<KriterParameter.kriter_threshold){
			current_similars.add(entries.get(indexes[loc]).getSKU());
			loc++;
		}
		if (current_similars.size()== KriterParameter.kriter_threshold){
			//that is theorically always true
			matching_skus.put(current_entry.getSKU(),new ArrayList<String>(current_similars));
			if ((matching_skus.size() != 0) && matching_skus.size() % KriterParameter.batch_size == 0 ){
				saving_similar();
				matching_skus.clear();
			}
			done = true;
		} else {
			// we theorically can not go there 
			unfetched_skus_local_cache.put(current_entry,current_similars);
		}
		return done;
	}

	public void find_my_similar_products(List<CatalogEntry> my_tofetch_entries,List<CatalogEntry> my_entries){
		int size_list = my_tofetch_entries.size();
		int filtered_size_list = my_entries.size();
		System.out.println(Thread.currentThread() +" Beginning to compute distance matrix from "+size_list);

		for (int i=0;i<size_list;i++){
			if (i!=0 && i%KriterParameter.displaying_threshold == 0){
				System.out.println(Thread.currentThread() +" Having computed distance matrix "+i+" from "+size_list);
			}
			CatalogEntry current_entry = my_tofetch_entries.get(i);
			Double[] vector_list = new Double[filtered_size_list]; 
			for (int j= 0;j<filtered_size_list;j++){
				CatalogEntry entryj = my_entries.get(j);
				Double innerDistance = computeWeightedDistance(current_entry,entryj);
				vector_list[j] = innerDistance;

			}
			// sorting the array and keeping the indexes
			DescendingArrayIndexComparator comparator = new DescendingArrayIndexComparator(vector_list);
			Integer[] indexes = comparator.createIndexArray();
			Arrays.sort(indexes, comparator);
			List<String> similars = new ArrayList<String>();
			// adding the 6 first closest skus
			similars.add(my_entries.get(indexes[0]).getSKU());
			similars.add(my_entries.get(indexes[1]).getSKU());
			similars.add(my_entries.get(indexes[2]).getSKU());
			similars.add(my_entries.get(indexes[3]).getSKU());
			similars.add(my_entries.get(indexes[4]).getSKU());
			similars.add(my_entries.get(indexes[5]).getSKU());
			matching_skus.put(current_entry.getSKU(),similars);
			if ((matching_skus.size() != 0) && matching_skus.size() % KriterParameter.batch_size == 0 ){
				saving_similar();
				matching_skus.clear();
			}
		}
	}

	public List<CatalogEntry> fetch_category_data3(String category) throws SQLException{
		List<CatalogEntry> my_entries = new ArrayList<CatalogEntry>();
		PreparedStatement select_statement = con.prepareStatement(select_entry_from_category3);
		select_statement.setString(1, category);
		ResultSet rs = select_statement.executeQuery();
		while (rs.next()) {
			CatalogEntry entry = new CatalogEntry();
			String sku = rs.getString(1);
			entry.setSKU(sku);
			// category fetching
			String CATEGORIE_NIVEAU_1 = rs.getString(2);
			entry.setCATEGORIE_NIVEAU_1(CATEGORIE_NIVEAU_1);
			String CATEGORIE_NIVEAU_2 = rs.getString(3);
			entry.setCATEGORIE_NIVEAU_2(CATEGORIE_NIVEAU_2);
			String CATEGORIE_NIVEAU_3 = rs.getString(4);
			entry.setCATEGORIE_NIVEAU_3(CATEGORIE_NIVEAU_3);
			String CATEGORIE_NIVEAU_4 = rs.getString(5);
			entry.setCATEGORIE_NIVEAU_4(CATEGORIE_NIVEAU_4);
			// product libelle
			String  LIBELLE_PRODUIT = rs.getString(6);
			entry.setLIBELLE_PRODUIT(LIBELLE_PRODUIT);
			String MARQUE = rs.getString(7);
			entry.setMARQUE(MARQUE);
			// brand description
			String  DESCRIPTION_LONGUEUR80 = rs.getString(8);
			entry.setDESCRIPTION_LONGUEUR80(DESCRIPTION_LONGUEUR80);
			// vendor and state (available or not)
			String VENDEUR = rs.getString(9);
			entry.setVENDEUR(VENDEUR);
			String ETAT = rs.getString(10);
			entry.setETAT(ETAT);
			String RAYON = rs.getString(11);
			entry.setRAYON(RAYON);
			my_entries.add(entry);
		}
		select_statement.close();
		return my_entries;
	}

	public List<CatalogEntry> fetch_category_data2(String category) throws SQLException{
		List<CatalogEntry> my_entries = new ArrayList<CatalogEntry>();
		PreparedStatement select_statement = con.prepareStatement(select_entry_from_category2);
		select_statement.setString(1, category);
		ResultSet rs = select_statement.executeQuery();
		while (rs.next()) {
			CatalogEntry entry = new CatalogEntry();
			String sku = rs.getString(1);
			entry.setSKU(sku);
			// category fetching
			String CATEGORIE_NIVEAU_1 = rs.getString(2);
			entry.setCATEGORIE_NIVEAU_1(CATEGORIE_NIVEAU_1);
			String CATEGORIE_NIVEAU_2 = rs.getString(3);
			entry.setCATEGORIE_NIVEAU_2(CATEGORIE_NIVEAU_2);
			String CATEGORIE_NIVEAU_3 = rs.getString(4);
			entry.setCATEGORIE_NIVEAU_3(CATEGORIE_NIVEAU_3);
			String CATEGORIE_NIVEAU_4 = rs.getString(5);
			entry.setCATEGORIE_NIVEAU_4(CATEGORIE_NIVEAU_4);
			// product libelle
			String  LIBELLE_PRODUIT = rs.getString(6);
			entry.setLIBELLE_PRODUIT(LIBELLE_PRODUIT);
			String MARQUE = rs.getString(7);
			entry.setMARQUE(MARQUE);
			// brand description
			String  DESCRIPTION_LONGUEUR80 = rs.getString(8);
			entry.setDESCRIPTION_LONGUEUR80(DESCRIPTION_LONGUEUR80);
			// vendor and state (available or not)
			String VENDEUR = rs.getString(9);
			entry.setVENDEUR(VENDEUR);
			String ETAT = rs.getString(10);
			entry.setETAT(ETAT);
			String RAYON = rs.getString(11);
			entry.setRAYON(RAYON);
			my_entries.add(entry);
		}
		select_statement.close();
		return my_entries;
	}

	public List<CatalogEntry> fetch_category_data1(String category) throws SQLException{
		List<CatalogEntry> my_entries = new ArrayList<CatalogEntry>();
		PreparedStatement select_statement = con.prepareStatement(select_entry_from_category1);
		select_statement.setString(1, category);
		ResultSet rs = select_statement.executeQuery();
		while (rs.next()) {
			CatalogEntry entry = new CatalogEntry();
			String sku = rs.getString(1);
			entry.setSKU(sku);
			// category fetching
			String CATEGORIE_NIVEAU_1 = rs.getString(2);
			entry.setCATEGORIE_NIVEAU_1(CATEGORIE_NIVEAU_1);
			String CATEGORIE_NIVEAU_2 = rs.getString(3);
			entry.setCATEGORIE_NIVEAU_2(CATEGORIE_NIVEAU_2);
			String CATEGORIE_NIVEAU_3 = rs.getString(4);
			entry.setCATEGORIE_NIVEAU_3(CATEGORIE_NIVEAU_3);
			String CATEGORIE_NIVEAU_4 = rs.getString(5);
			entry.setCATEGORIE_NIVEAU_4(CATEGORIE_NIVEAU_4);
			// product libelle
			String  LIBELLE_PRODUIT = rs.getString(6);
			entry.setLIBELLE_PRODUIT(LIBELLE_PRODUIT);
			String MARQUE = rs.getString(7);
			entry.setMARQUE(MARQUE);
			// brand description
			String  DESCRIPTION_LONGUEUR80 = rs.getString(8);
			entry.setDESCRIPTION_LONGUEUR80(DESCRIPTION_LONGUEUR80);
			// vendor and state (available or not)
			String VENDEUR = rs.getString(9);
			entry.setVENDEUR(VENDEUR);
			String ETAT = rs.getString(10);
			entry.setETAT(ETAT);
			String RAYON = rs.getString(11);
			entry.setRAYON(RAYON);
			my_entries.add(entry);
		}
		select_statement.close();
		return my_entries;
	}


	public List<CatalogEntry> fetch_rayon_data(String rayon) throws SQLException{
		List<CatalogEntry> my_entries = new ArrayList<CatalogEntry>();
		PreparedStatement select_statement = con.prepareStatement(select_entry_from_rayon);
		select_statement.setString(1, rayon);
		ResultSet rs = select_statement.executeQuery();
		while (rs.next()) {
			CatalogEntry entry = new CatalogEntry();
			String sku = rs.getString(1);
			entry.setSKU(sku);
			// category fetching
			String CATEGORIE_NIVEAU_1 = rs.getString(2);
			entry.setCATEGORIE_NIVEAU_1(CATEGORIE_NIVEAU_1);
			String CATEGORIE_NIVEAU_2 = rs.getString(3);
			entry.setCATEGORIE_NIVEAU_2(CATEGORIE_NIVEAU_2);
			String CATEGORIE_NIVEAU_3 = rs.getString(4);
			entry.setCATEGORIE_NIVEAU_3(CATEGORIE_NIVEAU_3);
			String CATEGORIE_NIVEAU_4 = rs.getString(5);
			entry.setCATEGORIE_NIVEAU_4(CATEGORIE_NIVEAU_4);
			// product libelle
			String  LIBELLE_PRODUIT = rs.getString(6);
			entry.setLIBELLE_PRODUIT(LIBELLE_PRODUIT);
			String MARQUE = rs.getString(7);
			entry.setMARQUE(MARQUE);
			// brand description
			String  DESCRIPTION_LONGUEUR80 = rs.getString(8);
			entry.setDESCRIPTION_LONGUEUR80(DESCRIPTION_LONGUEUR80);
			// vendor and state (available or not)
			String VENDEUR = rs.getString(9);
			entry.setVENDEUR(VENDEUR);
			String ETAT = rs.getString(10);
			entry.setETAT(ETAT);
			String RAYON = rs.getString(11);
			entry.setRAYON(RAYON);
			my_entries.add(entry);
		}
		select_statement.close();
		return my_entries;
	}

	public Double[] computeDistanceVector(CatalogEntry currentEntry, List<CatalogEntry> entries){
		int size_list = entries.size();
		Double[] to_return = new Double[size_list];
		for (int j=0;j<size_list;j++){
			CatalogEntry entryj = entries.get(j);
			to_return[j] = computeWeightedDistance(currentEntry,entryj);
		}
		return to_return;
	}

	public Double[] computeVectorizedDistanceMatrix(List<CatalogEntry> entries){
		int size_list = entries.size();
		int vector_size_list = size_list*(size_list+1)/2;
		Double[] to_return = new Double[vector_size_list];
		for (int i=0;i<size_list;i++){
			if (i%1000 == 0){
				System.out.println(Thread.currentThread() +" Having computed distance matrix"+i+" from "+size_list);
			}
			CatalogEntry entryi = entries.get(i);
			for (int j=i;j<size_list;j++){
				CatalogEntry entryj = entries.get(j);
				Double innerDistance = computeWeightedDistance(entryi,entryj);
				to_return[fromMatrixToVector(i,j,size_list)] = innerDistance;
			}
		}
		return to_return;
	}

	public Double computeWeightedDistance(CatalogEntry entryi, CatalogEntry entryj){
		Double distone;
		if (entryi.getSKU().equals(entryj.getSKU())){
			distone = Double.POSITIVE_INFINITY;
		} else {
			distone =StatisticsUtility.computeAlgoWeightedDistance(entryi.getLIBELLE_PRODUIT(), entryj.getLIBELLE_PRODUIT());
		}
		//	Double disttwo =StatisticsUtility.computeAlgoWeightedDistance(entryi.getDESCRIPTION_LONGUEUR80(), entryj.getDESCRIPTION_LONGUEUR80());
		//    return distone + disttwo;
		return distone;
	}

	public int fromMatrixToVector(int i, int j, int N)
	{
		int my_index;
		if (i <= j)
			my_index = i * N - (i - 1) * i / 2 + j - i;
		else
			my_index = j * N - (j - 1) * j / 2 + i - j;

		return my_index;
	}
	private void close_connection(){
		try {
			if (con != null) {
				con.close();
			}
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}
}
