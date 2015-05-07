package com.similarity.test;

import org.apache.commons.lang3.StringUtils;

public class LevenshteinTest {
	public static void main(String[] args){
		String test1 = "Salut les affreux jojos tout va bien";
		String test2 = "Salut les affreux jojos tout va bien";
		Integer result = StringUtils.getLevenshteinDistance(test1, test2);
		System.out.println("Restuls 1" + result);
		String ttest1 = "dans le bois est le loup, il a faim";
		String ttest2 = "le loup est dans le bois et il a faim";
		Integer tresult = StringUtils.getLevenshteinDistance(ttest1, ttest2);
		System.out.println("Restuls 2" + tresult);
		
	}

}
