package com.magasin.attributing;

public class FilteringUtilities {
	private static String[] real_magasin = {
        "informatique",//VRAI MAGASIN
        "musique-cd-dvd",
        "musique-instruments",//VRAI MAGASIN> arts et loisirs / culture
        //"9782818308318",
        "bricolage-chauffage",//VRAI MAGASIN> maison
        "culture-multimedia",
        //"traitement-de-l-air-de-l-eau",
        //"grues-treuils-palans",
        "cdiscount-pro",
        "dvd", //VRAI MAGASIN> culture
        "livres-bd",//VRAI MAGASIN> culture
        //"cdiscount",
        "jeux-educatifs",
        //"t-shirts-manches-courtes",
        "cadeaux-noel",
        //"apple-at-md826zm-a",
        //"point-de-vente",
        "juniors",//VRAI MAGASIN
        "jeux-pc-video-console", //VRAI MAGASIN> culture
        "high-tech",//VRAI MAGASIN
        //"sac-porte-travers-mandarina-duck-reference-j6t0",
        "vin-champagne",//VRAI MAGASIN
        "photo-numerique",//VRAI MAGASIN
        "animalerie", //VRAI MAGASIN> maison
        //"scarificateur-%C3%A9lectrique-combi-care-38-e-+-bac",
        "tout-a-moins-de-10-euros",
        //"lit-enfant-mi-hauteur-90-x-200-cm",
        //"op",
        //"9782918653400",
        //"fl%C3%A9chettes-ergo-soft",
        "bagages",//VRAI MAGASIN> pap
        "jardin-animalerie",//VRAI MAGASIN> maison
        "electromenager",//VRAI MAGASIN
        "le-sport",//VRAI MAGASIN
        "vin-alimentaire",//VRAI MAGASIN
        "cosmetique",
        "telephonie",//VRAI MAGASIN
        "arts-loisirs",//VRAI MAGASIN> culture
        //"filtration-de-l-eau-boissons-glacons",
        //"babygro-doudoune-doublee-polaire-bebe",
        "pret-a-porter",//VRAI MAGASIN
        "soldes-promotions",
        "outillage",
        "chaussures",
        "destockage",
        //"jean-diesel-safado-8u9-homme",
        "auto",//VRAI MAGASIN
        "Unknown",
        "maison",//VRAI MAGASIN
        "boutique-cadeaux",
        "salon-complet",
        "bijouterie",//VRAI MAGASIN> pap
        "au-quotidien",
        //"disques-durs",
        "jardin",
        "personnalisation-3d"
        //"mammouth---d%C3%A9terre-ton-dinosaure---dig-a-dino"
        };

	public static void main(String[] args){
		String totest="filtration-de-l-eau-boissons-glacons";
		check_proper_magasin(totest);
	}
	
	public static void check_proper_magasin(String totest){
		boolean found = false;
		for (int l=0;l<totest.length();l++){
			if (real_magasin[l].equals(totest)){
				found=true;
			}
		}
		if (!found){
			System.out.println("We here got a little problem");
		}
	}
}


