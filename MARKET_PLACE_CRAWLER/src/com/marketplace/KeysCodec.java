package com.marketplace;
import java.util.HashMap;
import java.util.Map;


public class KeysCodec {

	
	public static Map<Integer, String> codecs = new HashMap<Integer, String>();
	
	public static String stub = "w+CAIQICI";
	public static Map<Integer, String> getCodecs() {
		return codecs;
	}

	public static void setCodecs(Map<Integer, String> codecs) {
		KeysCodec.codecs = codecs;
	}

	public static void populate(){
		//source
		//http://moz.com/ugc/geolocation-the-ultimate-tip-to-emulate-local-search
		codecs.put(4, "E");
		codecs.put(5, "F");
		codecs.put(6, "G");
		codecs.put(7, "H");
		codecs.put(8, "I");
		codecs.put(9, "J");
		codecs.put(10, "K");
		codecs.put(11, "L");
		codecs.put(12, "M");
		codecs.put(13, "N");
		codecs.put(14, "O");
		codecs.put(15, "P");
		codecs.put(16, "Q");
		codecs.put(17, "R");
		codecs.put(18, "S");
		codecs.put(19, "T");
		codecs.put(20, "U");
		codecs.put(21, "V");
		codecs.put(22, "W");
		codecs.put(23, "X");
		codecs.put(24, "Y");
		codecs.put(25, "Z");
		codecs.put(26, "a");
		codecs.put(27, "b");
		codecs.put(28, "c");
		codecs.put(29, "d");
		codecs.put(30, "e");
		codecs.put(31, "f");
		codecs.put(32, "g");
		codecs.put(33, "h");
		codecs.put(34, "i");
		codecs.put(35, "j");
		codecs.put(36, "k");
		codecs.put(37, "l");
		codecs.put(38, "m");
		codecs.put(39, "n");
		codecs.put(40, "o");
		codecs.put(41, "p");
		codecs.put(42, "q");
		codecs.put(43, "r");
		codecs.put(44, "s");
		codecs.put(45, "t");
		codecs.put(46, "u");
		codecs.put(47, "v");
		codecs.put(48, "w");
		codecs.put(49, "x");
		codecs.put(50, "y");
		codecs.put(51, "z");
		codecs.put(52, "0");
		codecs.put(53, "1");
		codecs.put(54, "2");
		codecs.put(55, "3");
		codecs.put(56, "4");
		codecs.put(57, "5");
		codecs.put(58, "6");
		codecs.put(59, "7");
		codecs.put(60, "8");
		codecs.put(61, "9");
		codecs.put(62, "--");
		codecs.put(63, "-");
		codecs.put(64, "-A");
		codecs.put(65, "-B");
		codecs.put(66, "-C");
		codecs.put(67, "-D");
		codecs.put(68, "-E");
		codecs.put(69, "-F");
		codecs.put(70, "-G");
		codecs.put(71, "-H");
		codecs.put(72, "-I");
		codecs.put(73, "-J");
		codecs.put(76, "-M");
		codecs.put(83, "-T");
		codecs.put(89, "-L");
	}
}
