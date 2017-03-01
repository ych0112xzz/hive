package hive.jdbc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class ReadTopServer {
	public static void main(String[] args){
		try {
			BufferedWriter bw= new BufferedWriter(new OutputStreamWriter(new FileOutputStream("C:\\Users\\ych0112xzz\\Desktop\\ych.txt",false)));
			BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\ych0112xzz\\Downloads\\000000_0"));
			String s ="";
			long rank = 0l;
			double per;
			double sumper = 0d;
			while((s=br.readLine()) != null){
				String recs[] = s.split("\t");
				long rec=Long.parseLong(recs[1]);
				per =(double)(rec/5661335071.0d);
				if(rank<10){
					
					System.out.println(rec);
					System.out.println(per);
				}
				
				
				
				sumper = per+sumper;
				rank=rank+1l;
				//System.out.println(String.valueOf(rank)+"\t"+String.valueOf(sumper)+"\n");
				bw.write(String.valueOf(rank)+"\t"+String.valueOf(sumper)+"\n");
			}
			br.close();
			bw.close();
			System.out.println("seccess");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			
		}
		
	}

}
