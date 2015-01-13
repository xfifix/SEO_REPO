package com.offer.utility;

public class OfferMatchingUtility {
	private static String semanticSeparator = "@";
	public static String[] parseSemanticHit(String semanticHit){
		return semanticHit.split(semanticSeparator);
	}
}
