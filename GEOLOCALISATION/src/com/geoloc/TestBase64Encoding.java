package com.geoloc;
import org.apache.commons.codec.binary.Base64;


public class TestBase64Encoding {
	public static void main(String[] args){
	
		String location = "Nantes,Pays de la Loire,France";
		int my_length = location.length();
		KeysCodec.populate();
		String codec = KeysCodec.getCodecs().get(my_length);
		
		// encode data on your side using BASE64
		byte[]   bytesEncoded = Base64.encodeBase64(location.getBytes());
		System.out.println("ecncoded value is " + new String(bytesEncoded ));

		String encoded_location=new String(bytesEncoded);
		
		// Decode data on other side, by processing encoded data
		byte[] valueDecoded= Base64.decodeBase64(bytesEncoded );
		System.out.println("Decoded value is " + new String(valueDecoded));
		
		String final_string = KeysCodec.stub+codec+encoded_location;
		System.out.println(final_string);
	}
}
	