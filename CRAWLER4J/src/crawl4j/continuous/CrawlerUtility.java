package crawl4j.continuous;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.json.simple.JSONArray;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.xml.sax.SAXException;

import crawl4j.attributesutility.AttributesInfo;
import crawl4j.attributesutility.AttributesUtility;
import crawl4j.facettesutility.FacettesInfo;
import crawl4j.facettesutility.FacettesUtility;
import crawl4j.urlutilities.URL_Utilities;
import crawl4j.urlutilities.URLinfo;
import crawl4j.xpathutility.XPathUtility;

public class CrawlerUtility {

	public static Pattern bracketPattern = Pattern.compile("\\(.*?\\)");
	public static String category_name = "Catégorie";
	public static String product_name = "Nom du produit";
	public static String brand_name = "Marque";

	public static URLinfo basicParsing(URLinfo info, String fullurl){
		String page_type = URL_Utilities.checkTypeFullUrl(fullurl);
		String magasin = URL_Utilities.checkMagasinFullUrl(fullurl);
		String rayon = URL_Utilities.checkRayonFullUrl(fullurl);
		// filling up url regexp attributes
		info.setPage_type(page_type);
		info.setMagasin(magasin);
		info.setRayon(rayon);
		return info;
	}

	public static URLinfo advancedTextParsing(URLinfo info, String html){
		boolean vendor = CrawlerUtility.is_cdiscount_best_vendor_from_page_source_code(html);
		info.setCdiscountBestBid(vendor);
		info.setVendor(vendor ? "Cdiscount" : "Market Place");
		boolean youtube = CrawlerUtility.is_youtube_referenced_from_page_source_code(html);
		info.setYoutubeVideoReferenced(youtube);

		// XPATH parsing
		if (ContinuousCrawlParameter.isXPATHparsed){
			try {
				String[] xpathOutput = XPathUtility.parse_page_code_source(html);
				info.setXPATH_results(xpathOutput);
				info.setZtd(xpathOutput[8]);
				info.setFooter(xpathOutput[9]);
			} catch (XPathExpressionException | ParserConfigurationException
					| SAXException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Trouble parsing XPATH expressions for URL : "+info.getUrl());
			}
		}

		// filling up entity to be cached with page source code
		if (ContinuousCrawlParameter.isBlobStored){
			byte[] compressedPageContent = CrawlerUtility.gzip_compress_byte_stream(html.getBytes());
			info.setPage_source_code(compressedPageContent);
		}

		Document doc = Jsoup.parse(html);
		info.setText(doc.text());
		Elements titleel = doc.select("title");
		info.setTitle(titleel.text());
		// fetching the H1 element
		Elements h1el = doc.select("h1");
		info.setH1(h1el.text());
		// finding the footer with jQuery
		//			Elements footerel = doc.select("div.ftMention");
		//			info.setFooter(footerel.text());
		// finding the ztd with jQuery
		//			Elements ztdunfolded = doc.select("p.scZtdTxt");
		//			Elements ztdfolded = doc.select("p.scZtdH");
		//			info.setZtd((ztdunfolded==null? "":ztdunfolded.text())+(ztdfolded==null? "":ztdfolded.text()));
		// finding the short description
		Elements short_desc_el = doc.select("p.fpMb");
		info.setShort_desc((short_desc_el==null? "":short_desc_el.text()));
		// finding the number of attributes
		if ("FicheProduit".equals(info.getPage_type())){		
			List<AttributesInfo> attributesList = new ArrayList<AttributesInfo>();
			Elements attributes = doc.select(".fpDescTb tr");
			int nb_arguments = 0 ;
			for (Element tr_element : attributes){
				Elements td_elements = tr_element.select("td");
				if (td_elements.size() == 2){
					nb_arguments++;
					AttributesInfo toAdd = new AttributesInfo();
					String category = td_elements.get(0).text();
					toAdd.setData_name(category);
					String description = td_elements.get(1).text();                                    
					toAdd.setData(description);
					attributesList.add(toAdd);
					if (CrawlerUtility.category_name.equals(category)){
						info.setCategory(description);
					}
					if (CrawlerUtility.brand_name.equals(category)){
						info.setBrand(description);
					}
					if (CrawlerUtility.product_name.equals(category)){
						info.setProduit(description);
					}
				}
			}
			info.setAtt_number(nb_arguments);
			String attribute_json=AttributesUtility.getAttributesJSONStringToStore(attributesList);
			info.setAtt_desc(attribute_json);
		}
		// parsing the facettes
		if (("ListeProduit".equals(info.getPage_type()))|| ("ListeProduitFiltre".equals(info.getPage_type()))){
			// finding the number of attributes
			List<FacettesInfo> list_facettes = new ArrayList<FacettesInfo>();
			FacettesInfo my_info = new FacettesInfo();
			Elements facette_elements = doc.select("div.mvFilter");			
			for (Element facette : facette_elements ){
				//System.out.println(e.toString());
				Elements facette_name = facette.select("div.mvFTit");
				my_info.setFacetteName(facette_name.text());
				Elements facette_values = facette.select("a");
				for (Element facette_value : facette_values){
					String categorie_value = facette_value.text();
					if ("".equals(categorie_value)){
						categorie_value = facette_value.attr("title");
					}
					Matcher matchPattern = CrawlerUtility.bracketPattern.matcher(categorie_value);
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
					list_facettes.add(my_info);
					my_info = new FacettesInfo();
					my_info.setFacetteName(facette_name.text());
				}		
			}
			String facette_json=FacettesUtility.getFacettesJSONStringToStore(list_facettes);
			info.setFacettes(facette_json);
		}
		return info;
	}


	public static boolean is_cdiscount_best_vendor_from_page_source_code(String str_source_code){
		int cdiscount_index = str_source_code.indexOf("<p class='fpSellBy'>Vendu et expédié par <span class='logoCDS'>");
		if (cdiscount_index >0){
			return true;
		}else{
			return false;
		}
	}

	public static boolean is_youtube_referenced_from_page_source_code(String str_source_code){
		int youtube_index = str_source_code.indexOf("http://www.youtube.com/");
		if (youtube_index >0){
			return true;
		}else{
			return false;
		}
	}

	// compressing the byte stream
	public static byte[] gzip_compress_byte_stream(byte[] dataToCompress ){
		ByteArrayOutputStream byteStream = null;
		try{
			byteStream =
					new ByteArrayOutputStream(dataToCompress.length);
			GZIPOutputStream zipStream = new GZIPOutputStream(byteStream);
			try
			{
				zipStream.write(dataToCompress);
			}
			finally
			{
				zipStream.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			if (byteStream != null){
				try {
					byteStream.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		byte[] compressedData = new byte[0];
		if (byteStream != null){
			compressedData = byteStream.toByteArray();
		}	
		return compressedData;
	}

	@SuppressWarnings("unchecked")
	public static String linksSettoJSON(Set<String> linksSet){
		JSONArray setsArray = new JSONArray();
		for (String link : linksSet){
			setsArray.add(link);
		}
		return setsArray.toJSONString();
	}
}
