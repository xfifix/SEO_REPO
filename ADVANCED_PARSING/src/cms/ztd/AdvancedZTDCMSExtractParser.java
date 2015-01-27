package cms.ztd;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AdvancedZTDCMSExtractParser {
	// we here just want to find duplicated balises
	//[ZTD:P2|V+L|SEO-KEY-V]
	//[SEO_KEY_V
	//private static String balise_to_find = "[ZTD:P2|V+L|SEO-KEY-V]";
	//private static Pattern balise_to_find_pattern = Pattern.compile(balise_to_find);
	private static String beginning_balise_to_find = "\\[SEO_KEY_V";	
	private static Pattern beginning_balise_to_find_pattern = Pattern.compile(beginning_balise_to_find);
	private static List<LineItem> items = new ArrayList<LineItem>();
	private static List<LineItem> bad_items = new ArrayList<LineItem>();
	private static LineItem current_item;

	public static void main(String[] args)  {
		//String fileName = "/home/sduprey/My_Data/My_CMS_Extract/refnat_export_ztd_2014_11_01_100_0.csv";
		String fileName = "/home/sduprey/My_Data/My_CMS_Extract/refnat_export_ztd_2015_01_22_100_0.csv";
		
		String outputPathFileName = "/home/sduprey/My_Data/My_Outgoing_Data/My_ZTD_CMS_Extract/refnat_export_ztd_2015_01_22_100_0_results.csv";
		//String fileName = "V:\\SEO\\Stefan\\balises_ztds\\refnat_export_ztd_2014_11_01_100_0.csv";
		//String outputPathFileName = "V:\\SEO\\Stefan\\balises_ztds\\refnat_export_ztd_2014_11_01_100_0_results.csv";
		try{
			parsing_file(fileName);
		} catch (IOException e){
			e.printStackTrace();
		}
		make_your_job();
		try {
			print_results(outputPathFileName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Trouble saving result flat file : "+outputPathFileName);
			e.printStackTrace();
		}
	}

	private static void print_results(String outputPathFileName) throws IOException{
		BufferedWriter writer = null;
		// we open the file
		writer=  new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPathFileName), "UTF-8"));	
		// we write the header
		//writer.write("DOC;MODE;NODEID;NODELABEL;NODE_POSITION;OCCURENCE;PART_ONE;PART_TWO\n");
		writer.write("DOC;MODE;NODEID;NODELABEL;NODE_POSITION;OCCURENCE\n");
		// we open the database
		for (LineItem baditem : bad_items){
			writer.write(baditem.getDocument()+baditem.getMode()+baditem.getNodeId()+baditem.getNodeLable()+baditem.getNodePosition()+baditem.getOccurences()+"\n");
		}
		writer.close();
	}

	private static void make_your_job(){
		for (LineItem item : items){
			String ZTD_to_check = item.getZTD();
			Integer balise_count = check_occurences(ZTD_to_check);
			if (balise_count >= 1){
				System.out.println(" ZTD : " + ZTD_to_check);
				System.out.println(" Count found : " + balise_count);
				item.setOccurences(balise_count);
				bad_items.add(item);
			}
		}
	}

	private static int check_occurences(String ZTD_to_check){
		Matcher m = beginning_balise_to_find_pattern.matcher(ZTD_to_check);
		int count = 0;
		while (m.find()){
			count +=1;
		}
		return count;	
	}

	private static void parsing_file(String fileName) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		String header = br.readLine();
		header = br.readLine();
		String line="";
		StringBuilder word_sentence=new StringBuilder();;
		int word_counter=0;
		int nb_quote = 0;
		boolean iseven = true;
		while ((line = br.readLine()) != null) {
			int char_counter=0;
			while (char_counter<line.length()){
				char current_char=line.charAt(char_counter);
				word_sentence.append(current_char);
				if (current_char == '\"'){
					nb_quote++;
					iseven= ((nb_quote & 1) == 0);
				}
				if (((current_char==';' )|| (char_counter == line.length()-1 ))  && iseven){
					word_counter++;
					checkWord(word_counter,word_sentence);
					// we had a very last word if the last line char is ,
					if ((current_char==';' )&&(char_counter == line.length()-1 )){
						word_counter++;
						checkWord(word_counter,word_sentence);
					}
					// testing breakpoint
					word_sentence = new StringBuilder();
				}
				char_counter++;
			}
		}

	}

	private static void checkWord(int word_counter, StringBuilder word_sentence){
		if (word_counter%7 == 1){
			current_item = new LineItem();
			current_item.setNodeId(word_sentence.toString());
		}
		if (word_counter%7 == 2){
			current_item.setNodeLable(word_sentence.toString());
		}
		if (word_counter%7 == 3){
			current_item.setNodePosition(word_sentence.toString());
		}
		if (word_counter%7 == 4){
			current_item.setDocument(word_sentence.toString());
		}
		if (word_counter%7 == 5){
			current_item.setMode(word_sentence.toString());
		}
		if (word_counter%7 == 6){
			current_item.setPartone(word_sentence.toString());
		}
		if (word_counter%7 == 0){
			current_item.setParttwo(word_sentence.toString());
			items.add(current_item);
		}
	}

	static class LineItem {
		private String nodeId;
		private String nodeLable;
		private String nodePosition;
		private String document;
		private String mode;
		private String partone;
		private String parttwo;
		private int occurences;
		public String getNodeId() {
			return nodeId;
		}
		public void setNodeId(String nodeId) {
			this.nodeId = nodeId;
		}
		public String getNodeLable() {
			return nodeLable;
		}
		public void setNodeLable(String nodeLable) {
			this.nodeLable = nodeLable;
		}
		public String getNodePosition() {
			return nodePosition;
		}
		public void setNodePosition(String nodePosition) {
			this.nodePosition = nodePosition;
		}
		public String getDocument() {
			return document;
		}
		public void setDocument(String document) {
			this.document = document;
		}
		public String getMode() {
			return mode;
		}
		public void setMode(String mode) {
			this.mode = mode;
		}
		public String getPartone() {
			return partone;
		}
		public void setPartone(String partone) {
			this.partone = partone;
		}
		public String getParttwo() {
			return parttwo;
		}
		public void setParttwo(String parttwo) {
			this.parttwo = parttwo;
		}		
		public String getZTD(){
			return this.partone+this.parttwo;
		}
		public int getOccurences() {
			return occurences;
		}
		public void setOccurences(int occurences) {
			this.occurences = occurences;
		}
	}
}