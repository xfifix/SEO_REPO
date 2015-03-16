package com.data.processing;

import java.io.FileInputStream;
import java.io.InputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

public class ParsingOutputFile {
	private static String input_file_path = "/home/sduprey/My_Data/My_Kriter_Data/similar_full_20150311144001.xml";
    public static void main (String argv []) {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        try {
            InputStream    xmlInput  =
                new FileInputStream(input_file_path);

            SAXParser      saxParser = factory.newSAXParser();
            SimilarSkusSAXHandler handler   = new SimilarSkusSAXHandler();
            saxParser.parse(xmlInput, handler);

            System.out.println("Number of products found" + handler.skus.size());
            
        } catch (Throwable err) {
            err.printStackTrace ();
        }
    }
}