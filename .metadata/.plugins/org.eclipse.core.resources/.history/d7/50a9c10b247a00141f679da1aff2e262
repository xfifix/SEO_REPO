package com.keywords.processing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

public class ExaleadKeywordsPostExample {


	public static void main(String[] args) throws ClientProtocolException, IOException{
		//		 rajouter &applicationId=FT-PERTINENCE pour le tracking interne
		//						Par ailleurs vous pouvez vous adresser au load balancer plutôt qu’à un serveur :
		//					http://exasearchv6.gslb.cdweb.biz:10010/search-api/search
		//					Vous aurez ainsi le failover et si les serveurs viennent à changer c’est transparent pour vous.
		//					soit ajouter l’option streaming=true, ce qui désactive le sort
		//					soit paginer en mettant nresults=100 et utilisation de start= pour les offset.

		String query = "raspberry pi";
		String number = "10";
		RequestResults result = new RequestResults();
		int volume=10;
		result.setRequest(query);
		result.setVolume(volume);
		CloseableHttpClient httpclient = HttpClients.createDefault();
		String url_string = "http://exasearchv6.gslb.cdweb.biz:10010/search-api/search";
		HttpPost httpPost = new HttpPost(url_string);		
		//HttpPost httpPost = new HttpPost("http://targethost/login");
		List <NameValuePair> nvps = new ArrayList <NameValuePair>();
		nvps.add(new BasicNameValuePair("lang", "fr"));
		nvps.add(new BasicNameValuePair("sl", "sl0"));
		nvps.add(new BasicNameValuePair("f.20.field", "facet_mut_technical"));
		nvps.add(new BasicNameValuePair("f.20.in_hits", "False"));
		nvps.add(new BasicNameValuePair("f.20.in_synthesis", "False"));
		nvps.add(new BasicNameValuePair("f.20.max_per_level", "10"));
		nvps.add(new BasicNameValuePair("f.20.root", "Top/ClassProperties/is_best_total_offer"));
		nvps.add(new BasicNameValuePair("f.20.sort", "num"));
		nvps.add(new BasicNameValuePair("f.20.type", "category"));
		nvps.add(new BasicNameValuePair("refine", "+f/20/1"));
		nvps.add(new BasicNameValuePair("use_logic_facets", "false"));
		nvps.add(new BasicNameValuePair("use_logic_hit_metas", "false"));
		nvps.add(new BasicNameValuePair("add_hit_meta", "offer_product_id"));
		nvps.add(new BasicNameValuePair("add_hit_meta", "offer_price"));
		nvps.add(new BasicNameValuePair("add_hit_meta", "offer_seller_id"));
		nvps.add(new BasicNameValuePair("add_hit_meta", "title"));
		nvps.add(new BasicNameValuePair("hit_meta.termscore.expr", "100000*@term.score"));
		nvps.add(new BasicNameValuePair("hit_meta.proximity.expr", "@proximity"));
		nvps.add(new BasicNameValuePair("hit_meta.categoryweight.expr", "100000*offer_category_weight"));
		nvps.add(new BasicNameValuePair("hit_meta.ca14.expr", "offer_stats_income14_global"));
		nvps.add(new BasicNameValuePair("hit_meta.ca7.expr", "offer_stats_income7_global"));
		nvps.add(new BasicNameValuePair("hit_meta.ca1.expr", "offer_stats_income1_global"));
		nvps.add(new BasicNameValuePair("add_hit_metas","offer_default_departmentpath"));
		nvps.add(new BasicNameValuePair("output_format", "csv"));
		nvps.add(new BasicNameValuePair("nresults",number));
		nvps.add(new BasicNameValuePair("q", query));
		nvps.add(new BasicNameValuePair("applicationId", "FT-PERTINENCE"));
		//http://ldc-exa6-search01.cdweb.biz:10010/search-api/search?lang=fr&sl=sl0&use_logic_facets=false&use_logic_hit_metas=false&add_hit_meta=offer_product_id&add_hit_meta=offer_price:&&hit_meta.proximity.expr=@proximity&hit_meta.categoryweight.expr=100000*offer_category_weight&hit_meta.ca14.expr=offer_stats_income14_global&hit_meta.ca7.expr=offer_stats_income7_global&hit_meta.ca1.expr=offer_stats_income1_global&output_format=csv&nresults=2500&q=canape+angle AND offer_is_best_offer=1
		UrlEncodedFormEntity url_encoding = new UrlEncodedFormEntity(nvps);
		httpPost.setEntity(url_encoding);
		CloseableHttpResponse response2 = httpclient.execute(httpPost);

		try {
			//			System.out.println(response2.getStatusLine());
			//			HttpEntity entity2 = response2.getEntity();
			//			// do something useful with the response body
			//			// and ensure it is fully consumed
			//			System.out.println(EntityUtils.toString(response2.getEntity()));
			//			EntityUtils.consume(entity2);

			//System.out.println(Thread.currentThread() +response2.getStatusLine());
			HttpEntity entity2=response2.getEntity();
			String to_parse = EntityUtils.toString(entity2);
			// do something useful with the response body
			// and ensure it is fully consumed
			List<ULRLineToInsert> toresult = parse_results(to_parse,result.getRequest());
			if (isOfferRelevant(toresult,result.getRequest())){
				System.out.println("Yes, keyword : "+result.getRequest()+" is present in the title of one of our products requested from Exalead");
			}
			EntityUtils.consume(entity2);

		} finally {
			response2.close();
		}
	}

	private static boolean isOfferRelevant(List<ULRLineToInsert> toresult, String request){
		boolean found_in_title = false;
		for (ULRLineToInsert lineToInsert : toresult){
			String tocheck = lineToInsert.getTitle().toLowerCase();
			if (tocheck.contains(request)){
				found_in_title = true;
			}
		}
		return found_in_title;
	}

	private static List<ULRLineToInsert> parse_results(String to_parse, String request){
		List<ULRLineToInsert> all_parsed_results = new ArrayList<ULRLineToInsert>();
		System.out.println(to_parse);
		String[] lines = to_parse.split("\n");
		for (int i=1;i<lines.length;i++){		
			//# did,url,buildGroup,source,slice,score,mask,sort,offer_product_id,offer_price,offer_seller_id,title,proximity,ca1,categoryweight,ca14,ca7,termscore
			//   1   2   3         4       5     6     7    8        9                10          11          12       13    14        15        16  17     18
			String[] fields = lines[i].split(",");
			if (fields.length != 18){
				System.out.println("Trouble with request : "+request);
			} else {
				ULRLineToInsert toinsert = new ULRLineToInsert();
				toinsert.setRequest(request);
				toinsert.setDid(Integer.valueOf(fields[0]));
				String url = fields[1];
				url=url.replace("\"","");
				toinsert.setUrl(url);
				toinsert.setScore(Integer.valueOf(fields[5]));
				String offer_product_id = fields[8];
				offer_product_id=offer_product_id.replace("\"","");
				toinsert.setOffer_product_id(offer_product_id);
				String offer_price = fields[9];
				offer_price=offer_price.replace("\"","");
				toinsert.setOffer_price(Double.valueOf(offer_price));
				String offer_seller_id = fields[10];
				offer_seller_id=offer_seller_id.replace("\"","");
				toinsert.setOffer_seller_id(Integer.valueOf(offer_seller_id));
				String title = fields[11];
				title=title.replace("\"","");
				toinsert.setTitle(title);
				String proximity = fields[12];
				proximity=proximity.replace("\"","");			
				toinsert.setProximity(Double.valueOf(proximity));
				String ca1 = fields[13];
				ca1=ca1.replace("\"","");
				toinsert.setCa1(Double.valueOf(ca1));
				String categoryWeight = fields[14];
				categoryWeight=categoryWeight.replace("\"","");
				toinsert.setCategoryweight(Double.valueOf(categoryWeight));
				String ca14 = fields[15];
				ca14=ca14.replace("\"","");
				toinsert.setCa14(Double.valueOf(ca14));
				String ca7 = fields[16];
				ca7=ca7.replace("\"","");
				toinsert.setCa7(Double.valueOf(ca7));
				String termscore = fields[17];
				termscore=termscore.replace("\"","");
				toinsert.setTermscore(Integer.valueOf(termscore));
				all_parsed_results.add(toinsert);
			}
		}
		return all_parsed_results;
	}

	static class ULRLineToInsert{
		public String getRequest() {
			return request;
		}
		public void setRequest(String request) {
			this.request = request;
		}
		public int getDid() {
			return did;
		}
		public void setDid(int did) {
			this.did = did;
		}
		public String getUrl() {
			return url;
		}
		public void setUrl(String url) {
			this.url = url;
		}
		public int getScore() {
			return score;
		}
		public void setScore(int score) {
			this.score = score;
		}
		public String getOffer_product_id() {
			return offer_product_id;
		}
		public void setOffer_product_id(String offer_product_id) {
			this.offer_product_id = offer_product_id;
		}
		public double getOffer_price() {
			return offer_price;
		}
		public void setOffer_price(double offer_price) {
			this.offer_price = offer_price;
		}
		public int getOffer_seller_id() {
			return offer_seller_id;
		}
		public void setOffer_seller_id(int offer_seller_id) {
			this.offer_seller_id = offer_seller_id;
		}
		public double getProximity() {
			return proximity;
		}
		public void setProximity(double proximity) {
			this.proximity = proximity;
		}
		public double getCa1() {
			return ca1;
		}
		public void setCa1(double ca1) {
			this.ca1 = ca1;
		}
		public double getCategoryweight() {
			return categoryweight;
		}
		public void setCategoryweight(double categoryweight) {
			this.categoryweight = categoryweight;
		}
		public double getCa14() {
			return ca14;
		}
		public void setCa14(double ca14) {
			this.ca14 = ca14;
		}
		public double getCa7() {
			return ca7;
		}
		public void setCa7(double ca7) {
			this.ca7 = ca7;
		}
		public int getTermscore() {
			return termscore;
		}
		public void setTermscore(int termscore) {
			this.termscore = termscore;
		}
		public String getTitle() {
			return title;
		}
		public void setTitle(String title) {
			this.title = title;
		}
		private String title;
		private String request;
		private int did;
		private String url;
		private int score;
		private String offer_product_id;
		private double offer_price;
		private int offer_seller_id;
		private double proximity;
		private double ca1;
		private double categoryweight;
		private double ca14;
		private double ca7;
		private int termscore;
	}
}
