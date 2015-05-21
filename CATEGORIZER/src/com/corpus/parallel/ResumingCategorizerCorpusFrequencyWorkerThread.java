package com.corpus.parallel;

import java.sql.SQLException;
import java.util.List;

import com.corpus.CategorizerCorpusFrequencyManager;
import com.data.DataEntry;

public class ResumingCategorizerCorpusFrequencyWorkerThread implements Runnable {

	private CategorizerCorpusFrequencyManager manager = new CategorizerCorpusFrequencyManager();

	private  List<DataEntry> my_skus_to_compute;


	public ResumingCategorizerCorpusFrequencyWorkerThread(List<DataEntry>  to_fetch) throws SQLException{
		manager = new CategorizerCorpusFrequencyManager();
		this.my_skus_to_compute = to_fetch;
	}

	public void run() {
		int total_size = my_skus_to_compute.size();
		int loop_counter=0;
		for (DataEntry entry : this.my_skus_to_compute){
			System.out.println(Thread.currentThread()+" Processing entry SKU : "+entry.getIDENTIFIANT_PRODUIT()+" number : "+loop_counter+" over : "+total_size);
			manager.updateEntry(entry);
			manager.flagSkuInTFIDF(entry.getIDENTIFIANT_PRODUIT());
			loop_counter++;
		}
	}

}
