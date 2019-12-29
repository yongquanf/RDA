package edu.NUDT.RDA.parallel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import edu.NUDT.RDA.parallel.ParallelSBFInsert2.BFOp;
import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.POut;
import edu.harvard.syrah.prp.Stat;
import util.async.MathUtil;
import util.async.Writable;
import util.bloom.Apache.BloomFilter;
import util.bloom.Apache.Key;
import util.bloom.Apache.Hash.hashing.LongHashFunction;
import util.bloom.Exist.FineComb;
import util.bloom.Exist.IBLTEntry;
import util.bloom.RDA.SimpleRDATableEntry;

//
//Invertible Bloom Lookup Table implementation
//References:
//
//"What's the Difference? Efficient Set Reconciliation
//without Prior Context" by Eppstein, Goodrich, Uyeda and
//Varghese
//
//"Invertible Bloom Lookup Tables" by Goodrich and
//Mitzenmacher
//


public class ParallelSimpleRDATable  implements Writable ,Serializable{

	static Log log=new Log(ParallelSimpleRDATable.class);

	//hash function
	public static LongHashFunction[] LongHashFunction4PosHash;
	
	//number of hash functions
	 public static int N_HASH = 2;
	//scale the bucket size
	public static double scaleBucketFactor =1;
	
	public int valueSize=0;
	//public SimpleRDATableEntry[] hashTable = null;
	
	//int expectedNumEntries=0;
	
	public static  int seedSign=128721;
	static Random RandSign=null;
	
	public double sampleProbability=1;
	public int requiredLeadingZeros=0;

	/**
	 * parallel
	 */
	private static ExecutorService parallelBFr;
	public static int parallelism = 1;
	public static Semaphore BFDecodeOps;
	public static Semaphore BFOps;
	
	public static Semaphore BFDecodeOneThreadOps;
	/**
	 * concurrent tables
	 */
	public ConcurrentHashMap<Integer,SimpleRDATableEntry[]> bf = new ConcurrentHashMap<Integer,SimpleRDATableEntry[]>();
	public int bucketPerBank=-1; 
	 /**
	  * record pure items
	  */
	static ConcurrentHashMap<Integer,Integer> pureCells = new ConcurrentHashMap<Integer,Integer>();
	
	
	public static Random getSingleton(){
		if(RandSign==null){
			RandSign=new Random(seedSign);
			return RandSign;
		}else{
			return RandSign;
		}
		
	}
	/**
	 * init
	 */
	public static boolean inited = false;

	  public static void init(int threads){
		  
		 // pureCells.clear();
		  BFOps = new Semaphore(threads);
		  BFDecodeOps= new Semaphore(threads);
		  BFDecodeOneThreadOps = new Semaphore(1);
		  
		  ThreadFactory BFrFactory = new ThreadFactoryBuilder().setNameFormat("BF-%d")
			      .build();
			    parallelBFr = Executors.newFixedThreadPool(5, BFrFactory);			    
		 inited = true;			   
	  }
	  
	  
	
	public ParallelSimpleRDATable(int HashFunctions){
		
		parallelism = HashFunctions;
		N_HASH = parallelism;
		bucketPerBank=-1;
		sampleProbability=1;
		
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHash[i] = LongHashFunction.xx(i);
		}
		
	}
	
    
	
	/**
	 * 
	 * @param _expectedNumEntries
	 * @param _ValueSize
	 */
	public ParallelSimpleRDATable(int _expectedNumEntries, int _ValueSize,int HashFunctions){
		
		parallelism = HashFunctions;
		N_HASH = parallelism;
		
		this.valueSize=_ValueSize;
	    // 1.5x expectedNumEntries gives very low probability of
	    // decoding failure
		//int nEntries=_expectedNumEntries + _expectedNumEntries/2;
	    //while (N_HASH * (nEntries/N_HASH) != nEntries) ++nEntries;
		//this.expectedNumEntries = _expectedNumEntries;
		
		int bucketsPerBank = _expectedNumEntries/N_HASH;
		bucketPerBank = bucketsPerBank ;
		//create banks
		for(int iH=0;iH<HashFunctions;iH++){
		SimpleRDATableEntry[] hashTable = new SimpleRDATableEntry[bucketsPerBank];				
		for(int i=0;i<bucketsPerBank;i++){
			hashTable[i]=new SimpleRDATableEntry();//.setElementAt(, i);
		}
		//store
		bf.put(iH, hashTable);
		}
		
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHash[i] = LongHashFunction.xx(i);
		}
		
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHash[i] = LongHashFunction.xx(i);
		}
		////System.out.println("expectedNumEntries: "+expectedNumEntries+",HT Size: "+hashTable.length);

		//////System.out.println("HT Size: "+hashTable.length);
	}
	
	/**
	 * 
	 * @param _expectedNumEntries
	 * @param _ValueSize
	 */
	public ParallelSimpleRDATable(int _expectedNumEntries,int HashFunctions){
		
		parallelism = HashFunctions;
		N_HASH = parallelism;
		
		int _ValueSize = 8;
		this.valueSize=_ValueSize;
		//expectedNumEntries = _expectedNumEntries;
	    // 1.5x expectedNumEntries gives very low probability of
	    // decoding failure
		//int nEntries=_expectedNumEntries + _expectedNumEntries/2;
	   // while (N_HASH * (nEntries/N_HASH) != nEntries) ++nEntries;
	    
	    //nEntries= Math.max(_expectedNumEntries, _expectedNumEntries/2)
		int bucketsPerBank = _expectedNumEntries/N_HASH;
		bucketPerBank = bucketsPerBank ;
		
		//create banks
		for(int iH=0;iH<HashFunctions;iH++){
		SimpleRDATableEntry[] hashTable = new SimpleRDATableEntry[bucketsPerBank];				
		for(int i=0;i<bucketsPerBank;i++){
			hashTable[i]=new SimpleRDATableEntry();//.setElementAt(, i);
		}
		//store
		bf.put(iH, hashTable);
		}		
		
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHash[i] = LongHashFunction.xx(i);
		}
	    
		sampleProbability=1;
		////System.out.println("expectedNumEntries: "+expectedNumEntries+",HT Size: "+hashTable.length);
	}
	
	
	
	
	public ParallelSimpleRDATable(int _expectedNumEntries,int NumEntries,int valueSize,int HashFunctions){
		

		this.valueSize=valueSize;
		//this.expectedNumEntries = _expectedNumEntries;
		
		int bucketsPerBank = _expectedNumEntries/N_HASH;
		bucketPerBank = bucketsPerBank ;
		
		//create banks
		for(int iH=0;iH<HashFunctions;iH++){
		SimpleRDATableEntry[] hashTable = new SimpleRDATableEntry[bucketsPerBank];				
		for(int i=0;i<bucketsPerBank;i++){
			hashTable[i]=new SimpleRDATableEntry();//.setElementAt(, i);
		}
		//store
		bf.put(iH, hashTable);
		}
		
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHash[i] = LongHashFunction.xx(i);
		}
		////System.out.println("expectedNumEntries: "+expectedNumEntries+",HT Size: "+hashTable.length);
		sampleProbability=1;
		//////System.out.println("HT Size: "+hashTable.length);
	}
	
	
	
	public ParallelSimpleRDATable(int _expectedNumEntries,double _sampleProbability,int HashFunctions){
			
		parallelism = HashFunctions;
		N_HASH = parallelism;
		
		int _ValueSize = 8;
		this.valueSize=_ValueSize;
		//expectedNumEntries = _expectedNumEntries;
	    // 1.5x expectedNumEntries gives very low probability of
	    // decoding failure
		//int nEntries=_expectedNumEntries + _expectedNumEntries/2;
	   // while (N_HASH * (nEntries/N_HASH) != nEntries) ++nEntries;
	    
	    //nEntries= Math.max(_expectedNumEntries, _expectedNumEntries/2)
		int bucketsPerBank = _expectedNumEntries/N_HASH;
		bucketPerBank = bucketsPerBank ;
		
		//create banks
		for(int iH=0;iH<HashFunctions;iH++){
		SimpleRDATableEntry[] hashTable = new SimpleRDATableEntry[bucketsPerBank];				
		for(int i=0;i<bucketsPerBank;i++){
			hashTable[i]=new SimpleRDATableEntry();//.setElementAt(, i);
		}
		//store
		bf.put(iH, hashTable);
		}
		
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHash[i] = LongHashFunction.xx(i);
		}
		
		sampleProbability=_sampleProbability;
		requiredLeadingZeros=(int)Math.round(Math.log(_sampleProbability)/Math.log(0.5))%64;
		////System.out.println("expectedNumEntries: "+expectedNumEntries+",HT Size: "+hashTable.length);
	}
	
	
	/**
	 * O(d^{-k}) failure rate, (k+1)d buckets
	 * @param PacketNum
	 * @param NumHash
	 * @param loss
	 * @param reorder
	 * @return
	 */
	public static long computeNumberBuckets(long PacketNum,int NumHash,
			double loss,double reorder){
		return Math.round(scaleBucketFactor*(2*PacketNum*(NumHash)*(loss+reorder)));
	}
	
	/**
	 * vary the ratio
	 * @param scaleFactor
	 * @param PacketNum
	 * @param NumHash
	 * @param loss
	 * @param reorder
	 * @return
	 */
	public static long computeNumberBuckets(double scaleFactor,long TotalPacket,
			double sumLossReorder){
		return Math.round(2*TotalPacket*sumLossReorder*scaleFactor);
	}
	
	/**
	 * keySum=I[index]
	 * h[keySum] == index, return true
	 * @param index
	 * @return
	 */
	public static boolean EntryIsPure(int indexBank,int indexInBank,SimpleRDATableEntry[] table){

		
		//SimpleRDATableEntry[] table = bf.get(indexBank);			
		SimpleRDATableEntry entry = table[indexInBank];
		//fail cases
		if(entry ==null||entry.empty()){
			return false;
		}else if(entry.Counter!=1 || entry.Counter!=-1){
			return false;
		}
		
		long keySum = entry.keySum;
		
		int bucketsPerHash = table.length;
//		int indexPortion = indexInBank;
		//int offset = index - indexPortion*bucketsPerHash;
		
//		int startEntry = indexPortion*bucketsPerHash;
		//byte[] kVec=IBLTEntry.ToByteArray(keySum);
		
		//int[] h=IBLTEntry.hashC.hash(new Key(kVec),indexBank);
		int h = hashPos(keySum, indexBank);
		
		int derivedIndex =  (h%bucketsPerHash);
		if(derivedIndex == indexInBank){
			return true;
		}else{
			return false;
		}
			
	}

	
	/**
	 * set hash function
	 * @param numHash
	 */
	public void setHash(int numHash){
		this.N_HASH = numHash;
		
	}
	
	/**
	 * 
	 * @param id
	 * @param index
	 * @return
	 */
	public static int hashPos(long id,int index){
		long longHash=LongHashFunction4PosHash[index].hashLong(id);//,N_HASHCHECK);
		return (int) Math.abs(longHash%Integer.MAX_VALUE);
	}

	
	/**
	 * copy a table
	 * @return
	 */
	public ParallelSimpleRDATable copyTable(){
			
		ParallelSimpleRDATable a = new ParallelSimpleRDATable(this.bucketPerBank*this.N_HASH,this.N_HASH);
		
		Iterator<Entry<Integer, SimpleRDATableEntry[]>> ier = this.bf.entrySet().iterator();
		while(ier.hasNext()){
			Entry<Integer, SimpleRDATableEntry[]> nxt = ier.next();
			Integer id = nxt.getKey();
			
			SimpleRDATableEntry[] value = nxt.getValue();
			
			SimpleRDATableEntry[] hashTable = new SimpleRDATableEntry[value.length];			
			int count=0;
			while(count<value.length){
				SimpleRDATableEntry aa = value[count].clone();
				hashTable[count] = aa;
				count++;
			}
			bf.put(id, hashTable);
		}
		
		
		

		return a;
		
	}
		
	public void insert(long id,double TS){
		insert(id,TS,1);
	}
	
	/**
	 * insert items
	 * @param TS
	 * @param k
	 */
	public void insert(long id,double TS,long Counter){
		_insert(id,TS,Counter);
	}
	
	
	/**
	 * compute the cell index using the hash function
	 * @param k
	 * @return
	 */
	
	public int hash2Cell(long k){
	
	long tmp = FineComb.sHash64.hash(ByteBuffer.allocate(8).putLong(k).array())%64;
	int r = Long.numberOfLeadingZeros(tmp);
	return r;
	}
	
	
	public boolean InsertSample(long id,double TS,long Counter){
	int index=hash2Cell(id);
	if(index>=requiredLeadingZeros){
		_insert(id,TS,Counter);
		//System.out.println("inserted!");
		return true;
	}else{
		return false;
	}
	}
	
	/**
	 * interface to insert an item
	 * @param k
	 * @param v
	 */
//	public void insert(long id,double TS,long Counter){
//		_insert(id,TS,Counter);
//		
//	}
	
	 class BFInsert implements Runnable{
		   
		// private volatile boolean running = true;
		  // int indexStart=-1;
		   //int indexEnd=-1;
		   int indexBF=-1;
		   long id;
		   double TS;
		   long Counter;
		   //Set<Key> keyList;
		   public BFInsert(int index,long _id,double _TS,long _Counter){
			  // this.indexStart=start;
			   //this.indexEnd=end;
			   indexBF=index;
			   id = _id;
			   TS = _TS;
			   Counter = _Counter;
		   }
		   
//		   public void terminate() {
//		        running = false;
//		    }
		   public void run() {
			   //while (running) {
			    	  //log.main("begin: "+indexBF);
			    	  SimpleRDATableEntry[] TBPart = bf.get(indexBF);			    	
						byte[] kVec=IBLTEntry.ToByteArray(id);
						
						int h = hashPos(id, indexBF);
						//int[] h=IBLTEntry.hashC.hash(new Key(kVec),indexBF);
						
						SimpleRDATableEntry entry = TBPart[h%TBPart.length];
						entry.Counter+=Counter;
						entry.TS+=TS;
						entry.keySum ^= id;
						
						//BFOps.release(); 
						//running = false;			  
						//log.main("complete: "+indexBF);
			   		//}
			    }

	 }
	
	 void _insert(long id,double TS,long Counter){
		
		 try {        	 
        	// long startTime = System.currentTimeMillis();
			//BFOps.release(parallelism);
             //log.main("parallelism: "+parallelism);    
             for (int i = 0; i < parallelism; i++) {
             //  BFOps.acquire(1);               
               parallelBFr.execute(new BFInsert(i,id,TS,Counter));
               
             }       
             
           //  BFOps.acquire(parallelism);
             //log.main("complete: ");
            // BFOps.release(parallelism);
             //log.main("finish: "+parallelism);
           } catch (Exception e) {
             e.printStackTrace();
           }		 		
	}
	
	 class BFInsertBatch implements Runnable{
		   
		// private volatile boolean running = true;
		  // int indexStart=-1;
		   //int indexEnd=-1;
		   int indexBF=-1;
		   long Counter;
		   //ConcurrentHashMap<Integer,Set<Pair<Long,Double>>> hashKeys;
		   //Set<Key> keyList;
		   public BFInsertBatch(int index,long _Counter){
			  // this.indexStart=start;
			   //this.indexEnd=end;
			   indexBF=index;
			   //hashKeys= _hashKeys;
			   Counter = _Counter;
		   }
		   
//		   public void terminate() {
//		        running = false;
//		    }
		   public void run() {
			   //while (running) {
			    	  //log.main("begin: "+indexBF);
			    	  SimpleRDATableEntry[] TBPart = bf.get(indexBF);	
			    	 Hashtable<Long, Double> sets = hashKeys.get(indexBF);

			    	//   log.main("set: "+sets.size()+", TBPart: "+TBPart.length);				   		
			     Iterator<Entry<Long, Double>> ier = sets.entrySet().iterator();
			    	  while(ier.hasNext()){
			    		 Entry<Long, Double> nxt = ier.next();
			    		 long id = nxt.getKey();
			    		  double TS = nxt.getValue();
			    		//  log.main("#: "+id+", "+TS);
						byte[] kVec=IBLTEntry.ToByteArray(id);
						
						//int[] h=IBLTEntry.hashC.hash(new Key(kVec),indexBF);
						int h = hashPos(id, indexBF);
						
						SimpleRDATableEntry entry = TBPart[h%TBPart.length];
						entry.Counter+=Counter;
						entry.TS+=TS;
						entry.keySum ^= id;
			    	  }
			    	  bf.put(indexBF, TBPart);
					  BFOps.release(); 
						//running = false;			  
						//log.main("complete: "+indexBF);
			   		//}
			    }

	 }
	 void _insert(long Counter){
			
		 try {        	 
        	// long startTime = System.currentTimeMillis();
			 // BFOps.release(parallelism);
             //log.main("parallelism: "+parallelism);    
             for (int i = 0; i < parallelism; i++) {
               BFOps.acquire(1);               
               parallelBFr.execute(new BFInsertBatch(i,Counter));
               
             }       
             
             BFOps.acquire(parallelism);
             //log.main("complete: ");
             BFOps.release(parallelism);
             //log.main("finish: "+parallelism);
           } catch (Exception e) {
             e.printStackTrace();
           }		 		
	}
	 
	 /**
	  * seq insert
	  * @param id
	  * @param TS
	  * @param Counter
	  */
	 public void sequentialInsert(long id,double TS,long Counter){
		 
		 for(int indexBF=0;indexBF<N_HASH;indexBF++){
		  SimpleRDATableEntry[] TBPart = bf.get(indexBF);			    	
			byte[] kVec=IBLTEntry.ToByteArray(id);
			
			//int[] h=IBLTEntry.hashC.hash(new Key(kVec),indexBF);
			int h = hashPos(id, indexBF);
			
			int index = h%TBPart.length;
			SimpleRDATableEntry entry = TBPart[index];
			entry.Counter+=Counter;
			entry.TS+=TS;
			entry.keySum ^= id;
			bf.get(indexBF)[index] = entry;
		 }
	 }
	 /**
	  * conditionally remove
	  * @param id
	  * @param ts
	  * @param Counter
	 * @return 
	  */
		public boolean eraseSample(Long id, double ts, int Counter) {
			// TODO Auto-generated method stub
			int index=hash2Cell(id);
			if(index>=requiredLeadingZeros){
				 erase(id,ts,Counter);
				//System.out.println("inserted!");
				 return true;
				
			}else{
				return false;
			}
		}
	 /**
	  * erase items
	  * @param TS
	  * @param k
	  */
	 public void erase(long id, double TS,long Counter){
		// _e(id,TS,Counter);		
		 _insert(id,-TS,-Counter);
	 }
	 

	 class BFScanPure implements Runnable{
		   
		   int indexBF=-1;
		   //ConcurrentHashMap<Integer,Integer> _pureCells;
		   //Set<Key> keyList;
		   public BFScanPure(int index){
			  // this.indexStart=start;
			   //this.indexEnd=end;
			   indexBF=index;
			  // _pureCells = _pureCellsI;
		   }
		   
		   public void run() {
			      try {
			    	SimpleRDATableEntry[] TBPart = bf.get(indexBF);
			    	for(int i=0;i<TBPart.length;i++){		  				  
						 //SimpleRDATableEntry entry = TBPart[i];
						 	//////System.out.println("$Decode: "+entry.toString());
						  if( ParallelSimpleRDATable.EntryIsPure(indexBF,i,TBPart)){
							  //log.main("find pured!");
							  pureCells.put(i, indexBF);
							  //////System.out.print(" pure!!!\n");							
						  }
						  
					  }

			      } finally {
			    	  BFDecodeOps.release();
			    	  
			      }
			    }

	 }
	 
	/**
	 * decode ids
	 * @param positive
	 * @param negative
	 * @return
	 */
	public boolean decodeIDs(HashSet<Long> positive,HashSet<Long> negative){
		
		//scan pure cells
		BFDecodeOps.release(parallelism);
		//long time1 = System.nanoTime();
			while(true){  
				try {        	 
		        	// long startTime = System.currentTimeMillis();
	             for (int i = 0; i < parallelism; i++) {
	               BFDecodeOps.acquire(1);               
	               parallelBFr.execute(new BFScanPure(i));
	               
	             }
	             BFDecodeOps.acquire(parallelism);
	             //delete pured items
	             int nErased = 0;
	             if(!pureCells.isEmpty()){
					 Iterator<Entry<Integer, Integer>> ier = pureCells.entrySet().iterator();
						SimpleRDATableEntry entry;
						while(ier.hasNext()){
							Entry<Integer, Integer> nxt = ier.next();
							Integer bucketId = nxt.getKey();
							Integer indexBF = nxt.getValue();
							entry = bf.get(indexBF)[bucketId];
							if(entry.Counter==1){
								positive.add(entry.keySum);
							}else{
								negative.add(entry.keySum);
							}
							nErased++;
							_insert(entry.keySum,-entry.TS,-entry.Counter);
						}
						pureCells.clear();
					}
	             //terminate
	             if(nErased<=0){
	            	 break;
	             }
	             BFDecodeOps.release(parallelism);
	           } catch (Exception e) {
	             e.printStackTrace();
	           }		 	
				
		
		}
		if(false){
			//	float decodeDelay=(System.nanoTime()-time1)/1000000.0f;
		//	log.main("decodeDelay: "+decodeDelay+" +: "+positive.size()+" -: "+negative.size());
		
			Enumeration<SimpleRDATableEntry[]> ier = bf.elements();
			while(ier.hasMoreElements()){
				SimpleRDATableEntry[] nxt = ier.nextElement();
				for(int i=0;i<nxt.length;i++){
					if(!nxt[i].empty()){
						return false;
					}
				}
			}
			return true;
		}
		return true;
	}
			

	public boolean decodeIDsSequential(HashSet<Long> positive,HashSet<Long> negative){
		
		//scan pure cells
		long time1 = System.nanoTime();
			while(true){  
				try {        	 
		        	
					//scan each array
					for(int indexBF=0;indexBF<N_HASH;indexBF++){
					SimpleRDATableEntry[] TBPart = bf.get(indexBF);
			    	for(int i=0;i<TBPart.length;i++){		  				  
						 //SimpleRDATableEntry entry = TBPart[i];
						 	//////System.out.println("$Decode: "+entry.toString());
						  if( ParallelSimpleRDATable.EntryIsPure(indexBF,i,TBPart)){
							  //log.main("find pured!");
							  pureCells.put(i, indexBF);
							  //////System.out.print(" pure!!!\n");							
						  }
						  
					  }
					}
	             //delete pured items
	             int nErased = 0;
	             if(!pureCells.isEmpty()){
					 Iterator<Entry<Integer, Integer>> ier = pureCells.entrySet().iterator();
						SimpleRDATableEntry entry;
						while(ier.hasNext()){
							Entry<Integer, Integer> nxt = ier.next();
							Integer bucketId = nxt.getKey();
							Integer indexBF = nxt.getValue();
							entry = bf.get(indexBF)[bucketId];
							if(entry.Counter==1){
								positive.add(entry.keySum);
							}else{
								negative.add(entry.keySum);
							}
							nErased++;
							_insert(entry.keySum,-entry.TS,-entry.Counter);
						}
						pureCells.clear();
					}
	             //terminate
	             if(nErased<=0){
	            	 break;
	             }
	             
	           } catch (Exception e) {
	             e.printStackTrace();
	           }		 	
				
		
		}
		if(false){
				float decodeDelay=(System.nanoTime()-time1)/1000000.0f;
			log.main("decodeDelay: "+decodeDelay+" +: "+positive.size()+" -: "+negative.size());
		
			Enumeration<SimpleRDATableEntry[]> ier = bf.elements();
			while(ier.hasMoreElements()){
				SimpleRDATableEntry[] nxt = ier.nextElement();
				for(int i=0;i<nxt.length;i++){
					if(!nxt[i].empty()){
						return false;
					}
				}
			}
			return true;
		}
		return true;
	}
	
	 // Subtract two IBLTs
	public ParallelSimpleRDATable subtract(ParallelSimpleRDATable other){
	    // IBLT's must be same params/size:
	    //assert(valueSize == other.valueSize);
	    //assert(hashTable.length == other.hashTable.length);
		
	    ParallelSimpleRDATable result = this.copyTable();
	   
	    ////System.out.println("$subtractSize: "+size);
	    for (int id = 0; id < N_HASH; id++) {	    	 	    	
	       SimpleRDATableEntry[] e1A = result.bf.get(id);
	       SimpleRDATableEntry[] e2A = other.bf.get(id);
	       
	       for(int i=0;i<e1A.length;i++){
	      	        
	    	   e1A[i] = e1A[i].XORHashTable(e2A[i]);        	      
	       }
	       result.bf.put(id, e1A);
	    }

	    return result;
	}

	
	
	/**
	 * remove loss and reordered packets
	 * @param ids, decoded ids
	 * @param TSTable, id, timestamp
	 */
	public void RepairTBF(HashSet<Long> ids,Hashtable<Long,Double> TSTable){
		Iterator<Long> ier = ids.iterator();
		while(ier.hasNext()){
			long id = ier.next();
			if(TSTable.containsKey(id)){
				double ts = TSTable.get(id);				
				erase(id,ts,1);
			}
		}
	}
	
	/**
	 * remove loss and reordered packets
	 * @param ids, decoded ids
	 * @param TSTable, id, timestamp
	 */
	public void RepairTBF(HashSet<Long> ids,
			Hashtable<Long,Double> TSTable,
			Hashtable<Long,byte[]> valueTable){
		Iterator<Long> ier = ids.iterator();
		while(ier.hasNext()){
			long id = ier.next();
			if(TSTable.containsKey(id)&&valueTable.containsKey(id)){
				double ts = TSTable.get(id);
				byte[] value = valueTable.get(id);
				erase(id,ts,1);
			}else{
				////System.out.println("empty");
			}
		}
	}
	
	/**
	 * repair the percent of items
	 * @param ids
	 * @param TSTable
	 * @param valueTable
	 * @param percent
	 */
	public void RepairTBF(HashSet<Long> ids,Hashtable<Long,Double> TSTable,
			double percent){
		Iterator<Long> ier = ids.iterator();
		Random r =new Random(System.currentTimeMillis());
		while(ier.hasNext()){
			long id = ier.next();
			if(r.nextDouble()<=percent){
				
				if(TSTable.containsKey(id)){
					double ts = TSTable.get(id);				
					erase(id,ts,1);
				}else{
					////System.out.println("id: "+id+" not available");
					continue;
				}
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public void repair(ParallelSimpleRDATable blSender, double percentRepaired) {
		// TODO Auto-generated method stub
		
	}
	/**
	 * hashKeys
	 */
	static ConcurrentHashMap<Integer,Hashtable<Long,Double>> hashKeys=new ConcurrentHashMap<Integer,Hashtable<Long,Double>>();
	/**
	 * test
	 * @param args
	 */
	public static void main(String[] args){
		
		/**
		 * packet trace
		 */
		Random r = new Random(System.currentTimeMillis());
		long initialTS=r.nextLong()%100000;
		long _startID=r.nextLong()%100000;
		double scale=0.133;
		double shape=0.8;

		double reorderProb=0.1;	
		double dropProb = 0.1;
		
		PacketTraceGenerator pg= new PacketTraceGenerator(scale,shape,dropProb,reorderProb,initialTS,_startID);
		
		//params
		int NumHash = 2;
		int _expectedNumEntries =1000;			
		int totalPackets=100000;
		
		ParallelSimpleRDATable.init(NumHash);
		long totalDelays=0;
		
		
		ParallelSimpleRDATable bl =new ParallelSimpleRDATable(_expectedNumEntries,NumHash);		

		int repeatedNum = 10;
		double[] results1=new double[repeatedNum];
		double[] results2=new double[repeatedNum];
		
		for(int repeat = 0;repeat <repeatedNum;repeat++){
		//parallel
		 bl=new ParallelSimpleRDATable(_expectedNumEntries,NumHash);				
		 hashKeys.clear();
		 
		 
		 totalDelays=0;
		//bl=new ParallelSimpleRDATable(_expectedNumEntries,NumHash);		
		//batch
		
		for(int j=0;j<NumHash;j++){
			Hashtable<Long, Double> set = new Hashtable<Long,Double>();			
			hashKeys.put(j,set);
		}
		//total packets
		
		for(int i=0;i<totalPackets;i++){
			//	log.main("#i: "+i);
			double[] rec = pg.NextWeibullPacket();
			//replicate to each thread
			for(int j=0;j<NumHash;j++){
				Hashtable<Long, Double> set0 = hashKeys.get(j);
				set0.put((long)rec[0], rec[1]);
				hashKeys.put(j,set0);
			}
		}		
		 long t1 = System.currentTimeMillis();
		 bl._insert(1);
		totalDelays+=(System.currentTimeMillis()-t1);
		
		//decode
		HashSet<Long> positive = new HashSet<Long>();
		HashSet<Long> negative = new HashSet<Long>();
		 long t3 = System.currentTimeMillis();
		  bl.decodeIDs(positive, negative);
		 long t4= (System.currentTimeMillis()-t3);
		//hashKeys.clear();
		log.main("Parallel#: "+totalPackets+", Entries: "+_expectedNumEntries+", Threads: "+NumHash+", delay: "+totalDelays+", decodeDelay: "+t4);
		results1[repeat]=totalDelays;
		
		
		
		
		}
		
		//double c2=0;
		for(int repeat = 0;repeat <repeatedNum;repeat++){
			totalDelays=0;
			bl=new ParallelSimpleRDATable(_expectedNumEntries,NumHash);
		for(int i=0;i<totalPackets;i++){
		//	log.main("#i: "+i);
		double[] rec = pg.NextWeibullPacket();
		 long t1 = System.currentTimeMillis();;
		 bl.sequentialInsert((long)rec[0], rec[1],1);
		totalDelays+=(System.currentTimeMillis()-t1);
		}
		
		HashSet<Long> positive = new HashSet<Long>();
		HashSet<Long> negative = new HashSet<Long>();
		 long t3 = System.currentTimeMillis();
		  bl.decodeIDsSequential(positive, negative);
		 long t4= (System.currentTimeMillis()-t3);
		
		bl=null;
		log.main("seq#: "+totalPackets+", Entries: "+_expectedNumEntries+", delay: "+totalDelays+", decodeDelay: "+t4);
		results2[repeat]=totalDelays;
		}
		Stat st1 = new Stat(results1);
		Stat st2 = new Stat(results2);
		st1.calculate();
		st2.calculate();
		log.main("c1: "+POut.toString(results1)+//+", stat: "+POut.toString(st1..getCDF())+
				"c2: "+POut.toString(results2));//+", stat2: "+POut.toString(st2.getCDF()));
		
		}





}