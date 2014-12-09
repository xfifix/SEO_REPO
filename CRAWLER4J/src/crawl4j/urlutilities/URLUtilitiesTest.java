package crawl4j.urlutilities;

public class URLUtilitiesTest {
	
	// we here follow Julien's rule
//	Voici le principe de hiérarchie pour les navid (première séquence de chiffres indiquée après identifiant type de page dansl’url ( ex : /v- , /l-, /f-)…
//			+ 2 chiffres pour chaque sous-strate ( car 99 nœuds max pourune même strate)
//			 
//			Root/Home : 1
//			Root-1/Home magasin = 1XX
//			Root-2/Rayon : 1XXXX
//			Root-3/Sous-rayon : 1XXXXXX
//			Root-4 : Sous-sous rayon : 1XXXXXXXX
//			Root-5 : 1XXXXXXXXXX

	
	private static String[] toprune = {
		"http://tracking.cdiscount.com/tracking/myImage.track?param=2KMk2vLvGYpjZH72sJiUQ0-mLAJwjq8MKR60M0-qrB1YDA-nqLnrPlw7JYxRJQymdNm2RXeSRWOOwbim0nqauG0XHRBlrnsTV9U_lWTM_MmFp03PhxNxuD0rlqfvC8kWh0Us2Vs1VghiPB47CbQRf1lY1YM5zQF4cgW2yXfUye5bth0IOaKTpc0obkbUaqwkVPtqcuOIAMgOEXxnvAEgFvWKET3Wb6njvQxqhLakt9x0X4zdt6u2MZ_f7f8OhBN4visNFCQxxcLb0XOphMnti41nWXuMbIgwbFaH2FB8N2oidrrqZxSYjEkRGVJD5-NrPiCdskF8b6DuIGKka8iVdz7yPDTnOnCzFCphz4AYg74",
		"http://s2.cdscdn.com/Css/cdsrwd/wl/rwd/block/button.css?LanguageCode=fr&SiteId=100",
		"http://avis.cdiscount.com/7513-fr_fr/wilk003501x5/review/21977376/inappropriate.htm?authsourcetype=__AUTHTYPE__&format=embedded&innerreturn=http%3A%2F%2Favis.cdiscount.com%2F.%2F7513-fr_fr%2Fwilk003501x5%2Freviews.htm%3Fformat%3Dembedded%26sort%3Drating&return=http%3A%2F%2Fwww.cdiscount.com%2Fau-quotidien%2Fhygiene-soin-beaute%2Fwilkinson-lames-hydro-5-x5%2Ff-12702100101-wilk003501x5.html&sessionparams=__BVSESSIONPARAMS__&submissionparams=__BVSUBMISSIONPARAMETERS__&submissionurl=http%3A%2F%2Fwww.cdiscount.com%2FBazaarVoiceSubmission.html&user=__USERID__",
		"http://pubads.g.doubleclick.net/gampad/clk?id=21879375&iu=%2F7190%2Fcdiscount"
	};
	

	private static String[] totest = 
		{
		"http://www.cdiscount.com/bricolage-chauffage/v-117-1.html",
		"http://www.cdiscount.com/jardin-animalerie/v-163-1.html"};
//		" http://www.cdiscount.com/informatique/v-107-0.html",
//		" http://www.cdiscount.com/high-tech/v-106-0.html",
//		" http://www.cdiscount.com/telephonie/v-144-0.html",
//		" http://www.cdiscount.com/photo-numerique/v-112-0.html",
//		" http://www.cdiscount.com/auto/v-133-0.html",
//		" http://www.cdiscount.com/pret-a-porter/v-113-3.html",
//		"http://www.cdiscount.com/juniors/peluches/plush-company-15726-peluche-karola-vache/f-1206506-plu8029956157264.html",
//		"http://www.cdiscount.com/au-quotidien/alimentaire/whirlpool-eco306/f-12701-whirleco306.html",
//		"http://www.cdiscount.com/pret-a-porter/derniers-arrivages/waooh-tee-shirt-col-v-et-ecrit-bleu/f-11331-mp00857468.html?mpos=15%7Cmp",
//		"http://www.cdiscount.com/jardin/plantes/seaweed-xtract-alguamix-100-ml/f-16301-cul3700688517537.html?mpos=774%7Cmp",
//		"http://www.cdiscount.com/jeux-pc-video-console/ps4/nba-2k15-ps4/f-1030401-5026555417488.html?mpos=22%7Ccd",
//		"http://www.cdiscount.com/au-quotidien/droguerie/kit-de-nettoyage-pour-acier-inoxydable/f-127060302-wpr8015250283600.html",
//		"http://www.cdiscount.com/au-quotidien/alimentaire/tisane-bio-ange-gardien-en-sachet-anti-refroi/f-1270105-flo3560467790557.html",
//		"http://www.cdiscount.com/le-sport/soins-du-sportif/ceinture-lombaire-reglable-support-protection-9/f-1211401-auc6913280719026.html?mpos=11%7Cmp",
//		"http://www.cdiscount.com/electromenager/lavage-sechage/whirlpool-aws6213-lave-linge/f-11001040401-whiaws6213.html?mpos=21%7Ccd",
//		"http://www.cdiscount.com/juniors/r-cuisine+maya.html",
//		"http://www.cdiscount.com/pret-a-porter/vetements-femme/vetements-de-marque/little-marcel/l-113029560-4.html",
//		"http://www.cdiscount.com/au-quotidien/droguerie/balai/f-127060401-dom3466000044203.html",
//		"http://www.cdiscount.com/bijouterie/parure-en-calebasse-rhea/f-126-auc3663089010581.html?mpos=19%7Cmp",
//		"http://www.cdiscount.com/m-8089-vantage.html",
//		"http://www.cdiscount.com/lf-76901_6/produits-minceur_oenobiol.html"};


	public static void main(String[] args){	
		
		
		for (int i=0; i<toprune.length;i++){
			String current = toprune[i];
			System.out.println("Coming URL : "+current);
			String pruned = URL_Utilities.drop_parameters(current);
			System.out.println("Pruned URL : "+pruned);
		}
		
		for (int i =0; i<totest.length;i++){
			String current = totest[i];
			System.out.println(current);
			String magasin = URL_Utilities.checkMagasin(current);
			System.out.println("Magasin " + magasin);
			String rayon = URL_Utilities.checkRayon(current);
			System.out.println("Rayon : "+rayon);
			String type = URL_Utilities.checkType(current);
			System.out.println("Type : "+type);			
			System.out.println("\n\n");	
		}
	}
}