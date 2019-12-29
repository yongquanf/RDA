package edu.NUDT.RDA.parallel;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import util.async.Util;
import util.bloom.Apache.BloomFilter;
import util.bloom.Apache.Hash.Hash;
import util.bloom.Apache.Key;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class ParallelBF {

	
	  ExecutorService parallelBFr;
	  Semaphore BFOps=null;
	  int parallelism = 5;
	  int BFNum=0;
	  ConcurrentHashMap<Integer,BloomFilter> bf=null;
	  ConcurrentHashMap<Integer,BloomFilter> bf2=null;
	  //BloomFilter[]bf=null;
	  //BloomFilter[]bf2=null;
	  
	  private long BFTime=0;
	  AtomicIntegerArray count=null;
	  int dividePart = -1;
	  
	    public ParallelBF(int bfNum,int parallel) {
	   
	    	//bf = new BloomFilter[bfNum];
	    	//bf2 = new BloomFilter[bfNum];
	    	
	    	bf = new ConcurrentHashMap<Integer,BloomFilter>();
	    	bf2 = new ConcurrentHashMap<Integer,BloomFilter>();
	    	
	    	BFTime=0;
	    	count=new AtomicIntegerArray(bfNum);
	    	BFNum=bfNum;
	    	
	    	for(int i=0;i<bfNum;i++){
	    		count.set(i, 0);
	    	}
	    	parallelism = parallel;
	    	
	        dividePart= BFNum/parallelism;
	    	
	   ThreadFactory BFrFactory = new ThreadFactoryBuilder().setNameFormat("BF-%d")
	      .build();
	    parallelBFr = Executors.newFixedThreadPool(parallelism, BFrFactory);
	   
	    BFOps = new Semaphore(parallelism);
	  }
	
	    public void buildBF(){
	    	
			
	    	
	    	int n=100000;
			
	    	List<Integer> totalIDs = Util.generateRandomIntegers(10*n);
	    	
			int ratio=4;
			
			int sm1=ratio*n;
						
			int kk=Math.max(2,(int)Math.round(Math.log(2)*(sm1/n)));
			
			//int bfNum = parallelism;
			
	    for(int i=0;i<BFNum;i++){
			bf.put(i, new BloomFilter(sm1,kk,Hash.MURMUR_HASH));
			
			List<Key> NodeA_Neighbor = Util.getSubSet4Keys(totalIDs, n);
			
			bf.get(i).add(NodeA_Neighbor);
			
			bf2.put(i, new BloomFilter(sm1,kk,Hash.MURMUR_HASH));
			NodeA_Neighbor = Util.getSubSet4Keys(totalIDs, n);
			
			bf2.get(i).add(NodeA_Neighbor);
	    }
	    }
	    
    
	   public void parallelBF1Count(){
		   
	         try {
	        	 
	        	 BFTime=0;
	             long startTime = System.currentTimeMillis();
	             for (int i = 0; i < parallelism; i++) {
	               BFOps.acquire(1);
	               int start=i*dividePart;
	               int end = (i+1)*dividePart-1;
	               if(end>=BFNum){
	            	   end=BFNum;
	               }	               
	               parallelBFr.execute(new BFOp(start,end));
	             }
	             BFOps.acquire(parallelism);
	             BFOps.release(parallelism);
	             BFTime= (System.currentTimeMillis() - startTime);
	             System.out.println("$: parallel: "+BFTime);
	             System.out.println(count.toString());
	           } catch (InterruptedException e) {
	             e.printStackTrace();
	           }
	   }

	   public void sequentialTest(){
		   long startTime = System.currentTimeMillis();
		   for(int indexBF =0;indexBF<BFNum;indexBF++){
			 BloomFilter BloomF = bf.get(indexBF);
			 BloomFilter BloomF2 = bf2.get(indexBF);
			 BloomF.and(BloomF2);
			 count.set(indexBF,BloomF.getNumTrueBits());
		   }
		   
		   long t= (System.currentTimeMillis() - startTime);
           System.out.println("$: sequential: "+t);
           System.out.println(count.toString());
	   }
	   
	   class BFOp implements Runnable{
		   
		   int indexStart=-1;
		   int indexEnd=-1;
		   
		   public BFOp(int start,int end){
			   this.indexStart=start;
			   this.indexEnd=end;
		   }
		   
		   public void run() {
			      try {
			    	  performBFAND();
			      } finally {
			    	  BFOps.release();
			      }
			    }

		 private void performBFAND() {
			 //BloomFilter BloomF = bf.get(indexBF);
			 //BloomFilter BloomF2 = bf2.get(indexBF);
			 for(int indexBF =indexStart; indexBF<=indexEnd;indexBF++){
				 bf.get(indexBF).and(bf2.get(indexBF));
				 count.set(indexBF, bf.get(indexBF).getNumTrueBits());
			 }
		 }
		 
	   }


		 public static void  main(String[]args){
			 int bfNum = 20;
			 int parallel=5;
			 ParallelBF bf = new ParallelBF(bfNum,parallel); 
			 		 
			 try {
				 for(int i=0;i<10;i++){
				 
					 bf.buildBF();
					 bf.sequentialTest(); 				 				 
					 bf.parallelBF1Count();
				
				 }
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 }
	   
}
