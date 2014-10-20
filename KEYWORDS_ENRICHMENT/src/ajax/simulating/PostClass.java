package ajax.simulating;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;


public class PostClass {


	// We here need to handle the cookies
	
	//	    trying to request pixmania with ajax
	//	    POST http://www.pixmania.fr/autocomplete/ajax.html HTTP/1.1
	//		X-Requested-With: XMLHttpRequest
	//		Accept: text/html
	//		Content-Type: application/x-www-form-urlencoded
	//		Referer: http://www.pixmania.fr/index.html
	//		Accept-Language: fr-FR
	//		Accept-Encoding: gzip, deflate
	//		User-Agent: Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko
	//		Host: www.pixmania.fr
	//		Content-Length: 15
	//		DNT: 1
	//		Proxy-Connection: Keep-Alive
	//		Pragma: no-cache
	//		Cookie: PHPSESSID=138e4bcd67ad773e2bed36796da7d20d; m_ses=20140902144838; m_cnt=8; sCountryCode=fr; sLanguageCode=fr; cookie_allowed=allowed; cookieSource2=a%3A1%3A%7Bi%3A0%3Ba%3A9%3A%7Bs%3A2%3A%22DF%22%3Bi%3A1412254136%3Bs%3A2%3A%22LS%22%3Bd%3A2963252396%3Bs%3A7%3A%22MCTAGID%22%3Bi%3A-1%3Bs%3A5%3A%22SRCID%22%3Bi%3A302%3Bs%3A7%3A%22TAGCMDE%22%3Bi%3A-1%3Bs%3A5%3A%22IDTID%22%3Bi%3A-1%3Bs%3A2%3A%22DD%22%3Bi%3A1409662136%3Bs%3A3%3A%22PKL%22%3Bd%3A2963253454%3Bs%3A3%3A%22FKL%22%3Bd%3A2963252396%3B%7D%7D; RR_n=1; RR_mvtid=424-1409667297534-57-0; RR_psthc=b424.1705.1409662349378.pi; RR_uc=c65d575a-7847-497d-160d-46520d98bf12; RR_m=1; RR_s=b23494454.23494454; _wt.mode-313349=WT3hFjlTeHt-ps~; etuix=M42xgNP57jFhukAEKs9hKq47iKBtbj22AJz1sHIaOevRJq82RZiVuw--; _ga=GA1.2.830981553.1409662119; _wt.user-313349=WT3fK3kRTrnLwIQMburGN5_Z9-KLfK_PY4_Pc4MWQ1HbIZIcIorJ7tljf5A86WL35JgK60mFZY9A7r2Pk6aCFVNJvtubaVc1iGgI1Xd6KSkfjA~; __sonar=12803988684136436009; WT_FPC=id=2435b068f856c4300681409658519264:lv=1409663715042:ss=1409663697512; TW_VISITOR_ID=2c417fe1-ae9c-4133-8985-cf7c7e4d20ca
	//
	//		search_word=to*

	public static void main(String args[]) throws IOException{
		URL url = new URL("http://www.pixmania.fr/autocomplete/ajax.html");

		HttpURLConnection connection = (HttpURLConnection)url.openConnection();
		connection.setRequestMethod("GET");
		connection.setRequestProperty("User-Agent","CdiscountBot-crawler-httptest");
		connection.setInstanceFollowRedirects(false);

		connection.addRequestProperty("X-Requested-With","XMLHttpRequest");
		connection.addRequestProperty("Accept","text/html");
		connection.addRequestProperty("Content-Type","application/x-www-form-urlencoded");
		connection.addRequestProperty("Referer","http://www.pixmania.fr/index.html");
		connection.addRequestProperty("Accept-Language","fr-FR");
		connection.addRequestProperty("Accept-Encoding","gzip, deflate");
		connection.addRequestProperty("Proxy-Connection", "Keep-Alive");
		connection.addRequestProperty("Pragma", "no-cache");
		connection.addRequestProperty("search_word", "pisc*");
		connection.connect();
		//connection.
		System.out.println(connection.getResponseCode());
		//		BufferedInputStream my_stream = new BufferedInputStream(connection.getInputStream());
		//		String line ="";
		//		while ((line = my_stream.readLine()) != null) {
		//		}
		System.out.println(connection.getContentLength());
		//System.out.println(HttpURLConnection.HTTP_MOVED_TEMP);
		//get all headers

		InputStreamReader in = new InputStreamReader((InputStream) connection.getContent());
		BufferedReader buff = new BufferedReader(in);

		String line;
		do {
			line = buff.readLine();
			System.out.println(line);
		} while (line != null);

		Map<String, List<String>> map = connection.getHeaderFields();
		for (Map.Entry<String, List<String>> entry : map.entrySet()) {
			System.out.println("Key : " + entry.getKey() + 
					" ,Value : " + entry.getValue());
		}

		String redirect = connection.getHeaderField("Location");
		System.out.println("Redirect  " + redirect);
		//	HttpClient httpclient = new DefaultHttpClient();
		//	httpclient.setConnectionTimeout(10000);
		//	                            
		//	PostMethod httppost = new PostMethod(urlStr);
		//	httppost.setRequestHeader("X-Requested-With", "XMLHttpRequest");
		//	httppost.setRequestHeader("X-Prototype-Version", "1.4.0");
		//	httppost.setRequestHeader("Content-Type",
		//	"application/x-www-form-urlencoded");
		//	httppost.addParameter("destination", "topic://" + channelID);
		//	httppost.addParameter("message", URLEncoder.encode("<message type='chat'>Hallo</message>"));
		//	httppost.addParameter("type", "send");
		//	                            
		//	httpclient.startSession(urlStr, 80);
		//	result = httpclient.executeMethod(httppost);
		//	httppost.releaseConnection();
	}

	//    
	//    public static void main(String[] args)
	//    {
	//        HttpParameterPost();
	// 
	//    }
	// 
	//    private static void  HttpParameterPost() {
	// 
	//        HttpClient httpclient = new DefaultHttpClient();
	// 
	//        try {
	// 
	//            HttpPost httpPost = new HttpPost("http://localhost:8080/examples/servlets/servlet/RequestParamExample");
	// 
	//            List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>(2);
	//            nameValuePairs.add(new BasicNameValuePair("firstname","as400"));
	//            nameValuePairs.add(new BasicNameValuePair("lastname","samplecode"));
	//            httpPost.setEntity(new UrlEncodedFormEntity(nameValuePairs)); 
	// 
	//            System.out.println("executing request " + httpPost.getRequestLine());
	//            HttpResponse response = httpclient.execute(httpPost);
	//            HttpEntity resEntity = response.getEntity();
	// 
	//            System.out.println("----------------------------------------");
	//            System.out.println(response.getStatusLine());
	//            if (resEntity != null) {
	//                System.out.println("Response content length: " + resEntity.getContentLength());
	//                System.out.println("Chunked?: " + resEntity.isChunked());
	//                String responseBody = EntityUtils.toString(resEntity);
	//                System.out.println("Data: " + responseBody);
	//            }
	//            
	//            
	//            EntityUtils.consume(resEntity);
	//        } 
	//        catch (Exception e) {
	//            System.out.println(e);
	//        }
	//        finally {
	//            // When HttpClient instance is no longer needed,
	//            // shut down the connection manager to ensure
	//            // immediate deallocation of all system resources
	//            httpclient.getConnectionManager().shutdown();
	//        }
	// 
	// 
	// 
	//    }
	// 
}