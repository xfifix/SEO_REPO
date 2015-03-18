package com.similarity.test;

import java.util.ArrayList;
import java.util.Random;

public class RandomTest
{
	private Random randomGenerator;
	private ArrayList<Object> catalogue;

	public RandomTest ()
	{ 
		catalogue = new ArrayList<Object>();
		randomGenerator = new Random();
	}

	public Object anyItem()
	{
		int index = randomGenerator.nextInt(catalogue.size());
		Object item = catalogue.get(index);
		System.out.println("Managers choice this week" + item + "our recommendation to you");
		return item;
	}

	public static void main(String[] args){
		Random my_rand = new Random();
		while (true){
			int index = my_rand.nextInt(100);
			System.out.println("Index : "+index);
		}
	}
}