package com.referential.populating;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.WorkbookFactory;

public class PopulatingClass {
	public static int starting_relevant_rows=5;
	public static void main(String[] args) 
	{
		// Reading the property of our database
		Properties props = new Properties();
		FileInputStream in = null;      
		try {
			in = new FileInputStream("database.properties");
			props.load(in);

		} catch (IOException ex) {

			Logger lgr = Logger.getLogger(PopulatingClass.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);

		} finally {

			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				Logger lgr = Logger.getLogger(PopulatingClass.class.getName());
				lgr.log(Level.SEVERE, ex.getMessage(), ex);
			}
		}

		// the following properties have been identified
		String url = props.getProperty("db.url");
		String user = props.getProperty("db.user");
		String passwd = props.getProperty("db.passwd");

		String my_files_directory = "/home/sduprey/My_Data/My_MP_produits_en_ligne/datas_2014";

		File folder = new File(my_files_directory);
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				String filename =listOfFiles[i].getName();
				System.out.println("Processing File " + filename);
				try
				{
					// Instantiating the database
					Connection con = DriverManager.getConnection(url, user, passwd);
					PreparedStatement pst = null;
					ResultSet rs = null;
					// xls files variable
					// String to debug
					// String path_to_excel = "D:\\My_Mp_produits_en_ligne\\MP - Produits en ligne_2014-08-03-08-00-00.xls";
					FileInputStream file = new FileInputStream(listOfFiles[i]);

					// Create Workbook instance holding reference to .xlsx file
					// this format is explicitly for XLSX
					// XSSFWorkbook workbook = new XSSFWorkbook(file);

					// this format is explicitly for XLS
					org.apache.poi.ss.usermodel.Workbook workbook = WorkbookFactory.create(file);

					//Get first/desired sheet from the workbook
					// this format is explicitly for XLSX
					// XSSFSheet sheet = workbook.getSheetAt(0);

					// Extracting 3 pages : recapitulative, per magasin, per rayon
					String reporting_date="";
					for (int k=0;k<3;k++) {
						// this format is explicitly for XLS
						org.apache.poi.ss.usermodel.Sheet sheet = workbook.getSheetAt(k);
						System.out.println("Dealing with sheet" + sheet.getSheetName());
						//Iterate through each rows one by one
						Iterator<Row> rowIterator = sheet.iterator();
						int row_counter=0;

						while (rowIterator.hasNext()) 
						{
							int column_counter=0;
							row_counter++;
							Row row = rowIterator.next();
							//For each row, iterate through all the columns
							Iterator<Cell> cellIterator = row.cellIterator();
							if( row_counter<= starting_relevant_rows){
								// we here just want to extract the date
								while (cellIterator.hasNext()) 
								{
									column_counter++;
									Cell cell = cellIterator.next();

									switch (cell.getCellType()) 
									{
									case Cell.CELL_TYPE_NUMERIC:
										System.out.print(cell.getNumericCellValue() + "\t");
										break;
									case Cell.CELL_TYPE_STRING:
										System.out.print(cell.getStringCellValue() + "\t");
										break;
									}
									if (row_counter == 3 && column_counter == 3){
										String cellValue =cell.getStringCellValue();
										String[] splitString = (cellValue.split("\\s+"));
										System.out.println(splitString.length);// should be 14
										for (String my_string : splitString) {
											if (my_string.matches("\\d\\d/\\d\\d/\\d\\d$")){
												reporting_date=my_string;
											}
										}
									}
								}
							}else {
								// we insert each rows to the database
								String magasin = "TOTAL";
								String rayon = "TOTAL";
								int NB_SKUS_TOTAL=-1;
								int NB_SKUS_MP=-1;
								int NB_SKUS_CD=-1;
								int NB_SKUS_MIXTE=-1;
								int NB_OFFRE_EXCLU_MP=-1;
								if(cellIterator.hasNext()){
									if (k==0){
										column_counter++;
										Cell cell = cellIterator.next();
										NB_SKUS_TOTAL=(int) cell.getNumericCellValue();
										column_counter++;
										cell = cellIterator.next();
										NB_SKUS_MP=(int) cell.getNumericCellValue();
										column_counter++;
										cell = cellIterator.next();
										NB_SKUS_CD=(int) cell.getNumericCellValue();
										column_counter++;
										cell = cellIterator.next();
										NB_SKUS_MIXTE=(int) cell.getNumericCellValue();
										column_counter++;
										cell = cellIterator.next();
										NB_OFFRE_EXCLU_MP=(int) cell.getNumericCellValue();	
									} else if (k==1){
										column_counter++;
										Cell cell = cellIterator.next();
										magasin=cell.getStringCellValue();		
										column_counter++;					
										cell = cellIterator.next();
										NB_SKUS_TOTAL=(int) cell.getNumericCellValue();
										column_counter++;
										cell = cellIterator.next();
										NB_SKUS_MP=(int) cell.getNumericCellValue();
										column_counter++;
										cell = cellIterator.next();
										NB_SKUS_CD=(int) cell.getNumericCellValue();
										column_counter++;
										cell = cellIterator.next();
										NB_SKUS_MIXTE=(int) cell.getNumericCellValue();
										column_counter++;
										cell = cellIterator.next();
										NB_OFFRE_EXCLU_MP=(int) cell.getNumericCellValue();	
									}
									else if (k==2){
										column_counter++;
										Cell cell = cellIterator.next();
										magasin=cell.getStringCellValue();
										column_counter++;
										cell = cellIterator.next();
										rayon=cell.getStringCellValue();
										column_counter++;
										cell = cellIterator.next();
										NB_SKUS_TOTAL=(int) cell.getNumericCellValue();
										column_counter++;
										cell = cellIterator.next();
										NB_SKUS_MP=(int) cell.getNumericCellValue();
										column_counter++;
										cell = cellIterator.next();
										NB_SKUS_CD=(int) cell.getNumericCellValue();
										column_counter++;
										cell = cellIterator.next();
										NB_SKUS_MIXTE=(int) cell.getNumericCellValue();
										column_counter++;
										cell = cellIterator.next();
										NB_OFFRE_EXCLU_MP=(int) cell.getNumericCellValue();	
									}
									System.out.println("Inserting line number :"+row_counter);
									// INSERT INTO 
									String stm = "INSERT INTO MAGASIN_PRODUITS(MAGASIN,RAYON,NB_SKUS_TOTAL,"
											+ "NB_SKUS_MP,NB_SKUS_CD,NB_SKUS_MIXTE,NB_OFFRE_EXCLU_MP"
											+ ",REPORT_DATE)"
											+ " VALUES(?,?,?,?,?,?,?,?)";
									pst = con.prepareStatement(stm);
									pst.setString(1,magasin);
									pst.setString(2,rayon);
									pst.setInt(3,NB_SKUS_TOTAL);
									pst.setInt(4,NB_SKUS_MP);
									pst.setInt(5,NB_SKUS_CD);
									pst.setInt(6,NB_SKUS_MIXTE);
									pst.setInt(7,NB_OFFRE_EXCLU_MP);
									//						SimpleDateFormat sdf = new SimpleDateFormat(
									//								"yyyy-MM-dd"); 
									SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yy"); 
									Date  date = sdf.parse(reporting_date);
									java.sql.Date sqlDate = new java.sql.Date(date.getTime());
									pst.setDate(8,sqlDate);
									pst.executeUpdate();
								}
							}
						}
					}
					file.close();
				} 
				catch (Exception e) 
				{
					e.printStackTrace();
				}
			}
		}


	}
}
