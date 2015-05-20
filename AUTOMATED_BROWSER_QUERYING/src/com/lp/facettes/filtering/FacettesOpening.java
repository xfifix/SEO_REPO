package com.lp.facettes.filtering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;

import com.facettes.data.AdvancedFacettesInfo;
import com.facettes.data.URLFacettesData;
import com.facettes.utility.FacettesUtility;

public class FacettesOpening {


	public static void main(String[] args){
		URLFacettesData to_fetch = new URLFacettesData();

		String urlToSearch = "http://www.cdiscount.com/pret-a-porter/bebe-puericulture/voyages-bebe/sieges-auto/l-113177101.html";
//		// and the matching lf- http://www.cdiscount.com/lf-15255_3/sieges-auto_rose.html

		//String urlToSearch = "http://www.cdiscount.com/electromenager/lavage-sechage/achat-lave-linge/frontal-180/l-110010402.html";
		// and the matching lf- http://www.cdiscount.com/electromenager/lavage-sechage/achat-lave-linge/frontal-180/lf-5690_6-bosch.html

		to_fetch.setUrl(urlToSearch);

		String my_user_agent= "CdiscountBot-crawler";
		org.jsoup.nodes.Document doc = null;
		try {
			doc =  Jsoup.connect(urlToSearch)
					.userAgent(my_user_agent)
					.ignoreHttpErrors(true)
					.timeout(0)
					.get();
		}catch (IOException e){
			e.printStackTrace();
		}
		List<AdvancedFacettesInfo> standardFacettes = new ArrayList<AdvancedFacettesInfo>();
		if (doc != null){
			standardFacettes = FacettesUtility.extract_facettes_infos(doc, to_fetch);
		}
		System.out.println("Standard facettes number : "+standardFacettes.size());		
	}
}
