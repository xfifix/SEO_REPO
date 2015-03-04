package crawl4j.continuous;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.json.simple.JSONArray;

public class CrawlerUtility {

	public static Pattern bracketPattern = Pattern.compile("\\(.*?\\)");

	public static String category_name = "Catégorie";
	public static String product_name = "Nom du produit";
	public static String brand_name = "Marque";

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
