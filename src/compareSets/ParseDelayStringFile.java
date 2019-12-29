package compareSets;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.TreeMap;

import util.bloom.Apache.Hash.hashing.LongHashFunction;

public class ParseDelayStringFile {

	public static int seed2 = 543211;
	public static LongHashFunction LongHashFunction4PKeyHash=LongHashFunction.xx(seed2);
	
	
	
	public static Hashtable<Long,Double> parseStringFloatPairs(String file) throws FileNotFoundException{	
		Hashtable<Long,Double> ht = new Hashtable<Long,Double>();
		Scanner sc = new Scanner(new File(file));
		 while (sc.hasNext()) {	    	  
	          String str=sc.nextLine();
	          if(str.contains("TCP ")){
	        	 
	        	  double delay = Double.parseDouble(str.substring(0, 16));
	        	  String id = str.substring(29);
	        	  Long key = LongHashFunction4PKeyHash.hashChars(id);
	        	  if(ht.containsKey(key)){
	        		  System.err.print("duplicate keys found: "+str);
	        	  }else{
	        		  ht.put(key, delay);
	        	  }
	          }
		 
		 }
		 return ht;
	}
	
	/**
	 * id time delay
	 * @param file
	 * @throws Exception 
	 */
	public static void sortedHashTableByKey(String file) throws Exception{
		TreeMap<Double,String> ht = new TreeMap<Double,String>();
		Scanner sc = new Scanner(new File(file));
		 while (sc.hasNext()) {	    	  
	          String str=sc.nextLine();
	          int count=0;
	          long id=-1;
	          double ts=-1;
	          double delay=-1;
	          StringTokenizer st=new StringTokenizer(str);
	          while (st.hasMoreElements() ){
	          	  String rec=st.nextToken();
	          	  if(count==0){
	          		  id = Long.parseLong(rec);
	          	  }else if(count ==1){
	          		  ts = Double.parseDouble(rec);
	          	  }else if(count==2){
	          		  delay = Double.parseDouble(rec);
	          	  }
	          	  count++;
	          }
	          if(id!=-1){
	        	  if(ht.containsKey(ts)){
	        		  System.err.print("duplicate keys found: "+str);
	        	  }else{
	        		  ht.put(ts,id+" "+delay);
	        	  }
	          }
		 
		 }//end scan
		 String inter= "SortTraceExpRDA1128CommonDelay2";
		 
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(inter,
					true));
			for(Entry<Double, String> entry : ht.entrySet()) {
				 Double key = entry.getKey();
				  String value = entry.getValue();

				  bufferedWriter.append(key + " " + value+"\n");
				  bufferedWriter.flush();
				}
			bufferedWriter.close();
	}
	
	public static void parsePairTrace() throws Exception{
		String file1="/Users/quanyongf/soft/RDASoft/TraceExpRDA1128/tcpdump/tAt2";
		String file2="/Users/quanyongf/soft/RDASoft/TraceExpRDA1128/tcpdump/tAt3";
		
		Hashtable<Long,Double> F1=parseStringFloatPairs(file1);
		Hashtable<Long,Double> F2=parseStringFloatPairs(file2);
		
		Hashtable<Double,Double> common=new Hashtable<Double,Double>();
		String testIntersection = "TraceExpRDA1128CommonDelay2";
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(testIntersection,
				true));
		
		
		Iterator<Entry<Long, Double>> ier = F1.entrySet().iterator();
		while(ier.hasNext()){
			Entry<Long, Double> item=ier.next();
			if(F2.containsKey(item.getKey())){
				System.out.println("$: "+item.getValue());
				double t1=item.getValue();
				double t2=F2.get(item.getKey());
				//common.put(Math.min(t1,t2),Math.abs(t1-t2));
				bufferedWriter.append(item.getKey()+" "+Math.min(t1,t2)+" "+Math.abs(t1-t2)+"\n");
			}
		}
		bufferedWriter.flush();
		bufferedWriter.close();
		
	}
	
	public static void main(String[] args){
		
		 try {
			if(false){
				parsePairTrace();
			}
			sortedHashTableByKey("/Users/quanyongf/soft/RDASoft/TraceExpRDA1128CommonDelay2");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
