package edu.NUDT.RDA.parallel;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import util.async.Util;
import util.bloom.Apache.BloomFilter;
import util.bloom.Apache.Hash.Hash;
import util.bloom.Apache.Key;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class ParallelBFQuery {

	
	  static ExecutorService parallelBFr=null;
	  
	  Semaphore BFOps=null;
	  int parallelism = 5;
	  int BFNum=0;
	  ConcurrentHashMap<Integer,BloomFilter> bf=null;
	  static ConcurrentHashMap<Integer,Set<Key>> hashKeys=new ConcurrentHashMap<Integer,Set<Key>>();
	  //BloomFilter[]bf=null;
	  //BloomFilter[]bf2=null;
	  //static Set<Key> NodeA_NeighborQuery;
	  
	  private double BFTime=0;
	  AtomicIntegerArray count=null;
	  int dividePart = -1;
	  boolean initialized= false;
	  
	  public void init(){
		  
		  ThreadFactory BFrFactory = new ThreadFactoryBuilder().setNameFormat("BF-%d")
			      .build();
			    parallelBFr = Executors.newFixedThreadPool(10, BFrFactory);
			   
	  }
	  
	    public ParallelBFQuery(int bfNum) {
	   
	    	if(!initialized){
	    		init();
	    		initialized=true;
	    	}
	    	//bf = new BloomFilter[bfNum];
	    	//bf2 = new BloomFilter[bfNum];
	    	
	    	bf = new ConcurrentHashMap<Integer,BloomFilter>();
	    	// hashKeys =new ConcurrentHashMap<Integer,Set<Key>>();
	    	
	    	BFTime=0;
	    	count=new AtomicIntegerArray(bfNum);
	    	BFNum=bfNum;
	    	
	    	for(int i=0;i<bfNum;i++){
	    		count.set(i, 0);
	    	}
	    	parallelism = bfNum;
	    	
	        dividePart= 1;
	    	
	  
	    BFOps = new Semaphore(parallelism);
	  }
	

	    public void buildBF(){
	    	
			
	    	
	    	int n=100000;
			
	    	List<Integer> totalIDs = Util.generateRandomIntegers(10*n);
	    	
			int ratio=4;
			
			int sm1=ParallelBFInsert.getTotalM0(n);
						
			//int kk=Math.max(2,(int)Math.round(Math.log(2)*(sm1/n)));
			int kk=ParallelBFInsert.getTotalK0();
			
			//reduce the number of hash functions
			//
			if(parallelism>1){
				kk=Math.max(1, (int)Math.round(kk/(0.0+parallelism)));
				//kk=1;
			}
			
			
			//int bfNum = parallelism;
			
	    for(int i=0;i<BFNum;i++){
			bf.put(i, new BloomFilter(sm1,kk,Hash.MURMUR_HASH));
			 hashKeys.put(i, new HashSet<Key>());
			 
			List<Key> NodeA_Neighbor = Util.getSubSet4Keys(totalIDs, n);
			bf.get(i).add(NodeA_Neighbor);			
			NodeA_Neighbor.clear();
			
			boolean et=hashKeys.get(i).addAll(Util.getSubSet4Keys(totalIDs, n));
			//System.out.println("Added? "+et);
	    }
	   
	    }
	    
    

		public void clean(){
	    	this.bf.clear();
	    	//this.hashKeys.clear();
	    	
	    }
	   public void parallelBF1Count(){
		   
	         try {
	        	 
	        	 BFTime=0;
	        	 
	        	 long startTime = System.currentTimeMillis();
	             
	             for (int i = 0; i < parallelism; i++) {
	               BFOps.acquire(1);
	               //int start=i*dividePart;
	               //int end = (i+1)*dividePart-1;
	               //if(end>=BFNum){
	            	//   end=BFNum;
	              // }	               
	               parallelBFr.execute(new BFOp(i,hashKeys.get(i)));
	               
	             }
	             BFOps.acquire(parallelism);
	             BFOps.release(parallelism);
	             //BFTime= (System.currentTimeMillis() - startTime);
	             //BFTime=BFTime/(0.0+hashKeys.get(0).size());
	             //System.out.println("$: parallel: "+BFTime);
	             //System.out.println(count.toString());
	           } catch (Exception e) {
	             e.printStackTrace();
	           }
	   }

	   
	   public double sequentialTest(Set<Key> keyList){
		 
		   long T=System.currentTimeMillis();
		   for(int indexBF =0;indexBF<BFNum;indexBF++){	   
	   Iterator<Key> ier = (keyList.iterator());
		while(ier.hasNext()){
			
			Key key = ier.next();
			
			
			//long T= System.currentTimeMillis();
			
		    int[] h = bf.get(indexBF).hash.hash(key);
		    //bf.get(indexBF).hash.clear();
		    
		    
		    for(int j = 0; j < bf.get(indexBF).nbHash; j++) {
		    	//System.out.println("$: "+h[i]+", "+bits.get(h[i]));
		      if(!bf.get(indexBF).bits.get(h[j])) {			  
		      }
		    }
		}
		   }
		long T2=System.currentTimeMillis()-T;
		double T3=T2/(0.0+hashKeys.get(0).size());
		System.out.println("$: sequential: "+T2);
		String result = "$: sequential: "+T2;
		write("testParallelBTQuery", result);
		return T3;
}
	   
	   
	   
	   public double sequentialTest0(Set<Key> keyList){
		   //long startTime = System.currentTimeMillis();
		   long t=0;
		   for(int indexBF =0;indexBF<BFNum;indexBF++){
			 BloomFilter BloomF = bf.get(indexBF);
			 //count.set(indexBF,BloomF.getNumTrueBits());
			 
			double time=BloomF.getQueryTime(keyList);
			 t+=time;
		   }
           System.out.println("$: sequential: "+t);
           //System.out.println(count.toString());
           return t;
	   }
	   
	   class BFOp implements Runnable{
		   
		   int indexStart=-1;
		   int indexEnd=-1;
		   int indexBF=-1;
		   //Set<Key> keyList;
		   public BFOp(int index,Set<Key> _keyList){
			  // this.indexStart=start;
			   //this.indexEnd=end;
			   indexBF=index;
			   //Set<Key> keyList=_keyList;
		   }
		   
		   public void run() {
			      try {
			    	 
			    	  performBFQuery();
			    	  
			    	  
			      } finally {
			    	  BFOps.release();
			    	  
			      }
			    }

		 private void performBFQuery() {
			 //BloomFilter BloomF = bf.get(indexBF);
			 //BloomFilter BloomF2 = bf2.get(indexBF);
			// for(int indexBF =indexStart; indexBF<=indexEnd;indexBF++){
			 Set<Key> keyList = hashKeys.get(indexBF);
			 
				 if(keyList==null||keyList.isEmpty()){
					 System.err.println("Error: "+indexBF);
				 }
				 Iterator<Key> ier = (keyList.iterator());
				 double terminated = keyList.size()/(0.0+parallelism);
				 double upperbound=terminated;
				 long startTime = System.currentTimeMillis();
				 
					while(terminated>0&&ier.hasNext()){
						terminated--;
						
						Key key = ier.next();
						
						
						//long T= System.currentTimeMillis();
						
					    int[] h = bf.get(indexBF).hash.hash(key);
					    //bf.get(indexBF).hash.clear();			    
					    for(int j = 0; j < bf.get(indexBF).nbHash; j++) {
					    	//System.out.println("$: "+h[i]+", "+bits.get(h[i]));
					      if(!bf.get(indexBF).bits.get(h[j])) {			  
					      }
					    }
					}
					long T2=System.currentTimeMillis()-startTime;
					double T3=T2;
			    	  System.out.println("Thread-"+indexBF+" "+T3);
			    	  String result="Thread-"+indexBF+" "+startTime+" "+T3;			    			  
			    	  write("testParallelBTQuery", result);
			    	  
			// }
		 }
		 
	   }

	   public void write(String testParallel,String result){
		   
		   BufferedWriter bufferedWriter = null;
			try{
			
				bufferedWriter = new BufferedWriter(new FileWriter(testParallel,true));
				bufferedWriter.append(result);
				bufferedWriter.newLine();
				bufferedWriter.flush();
				bufferedWriter.close();
			}catch(Exception e){
				e.printStackTrace();
			}
				
		   
	   }

		 public static void  main(String[]args){
			 int bfNum = 5;
			 int parallel=5;
			 
			 
			 try {
				 
				 for(int rpt=0;rpt<10;rpt++){
				 ParallelBFQuery bf = new ParallelBFQuery(1); 
				 bf.buildBF();
				 bf.sequentialTest(hashKeys.get(0));	
				 //bf.clean();
				 for(int i=1;i<=4;i++){
				 
					 bf = new ParallelBFQuery(i); 
					 bf.buildBF();
							 				 
					 bf.parallelBF1Count();
					 //bf.clean();
				 }
				 }
				 
				parallelBFr.shutdown();
				 
				 
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 }
	   
}
