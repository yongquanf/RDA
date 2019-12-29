package util.bloom.RDA;

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
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

import edu.harvard.syrah.prp.Log;
import util.async.MathUtil;
import util.async.Writable;
import util.bloom.Apache.Key;
import util.bloom.Apache.Hash.hashing.LongHashFunction;
import util.bloom.Exist.FineComb;
import util.bloom.Exist.IBLTEntry;


public class RDATable  implements Writable ,Serializable{

	public static Log log = new Log(RDATable.class);
	//number of hash functions
	 public static int N_HASH = 2;
	 
	//hash function for position
	public static LongHashFunction[] LongHashFunction4PosHash;
	
	//keycheck
	public static int seed2 = 54321;
	public static LongHashFunction LongHashFunction4PKeyHash=LongHashFunction.xx(seed2);
	
	
	//scale the bucket size
	public static double scaleBucketFactor =1;
	
	public int valueSize=0;
	public RDATableEntry[] hashTable = null;
	
	int expectedNumEntries=0;
	
	public static  int seedSign=128721;
	static Random RandSign=null;
	
	public double sampleProbability=1;
	public int requiredLeadingZeros=0;
	
	//create a lock on the data
		public Semaphore BFOps=null;
	
	
	public static Random getSingleton(){
		if(RandSign==null){
			RandSign=new Random(seedSign);
			return RandSign;
		}else{
			return RandSign;
		}
		
	}
	
	class Pair<T1,T2>{
		T1 id;
		T2 byteArray;
		public Pair(T1 a,T2 b){
			id=a;
			this.byteArray=b;
		}
	}
	
	public RDATable(){
		hashTable = null;
		sampleProbability=1;
		
		//semaphore
		BFOps =  new Semaphore(1);
		
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHash[i] = LongHashFunction.xx(i);
		}
	}
	
	/**
	 * 
	 * @param _expectedNumEntries
	 * @param _ValueSize
	 */
	public RDATable(int _expectedNumEntries){

		//semaphore
				BFOps =  new Semaphore(1);
				
		int _ValueSize = 8;
		this.valueSize=_ValueSize;
		expectedNumEntries = _expectedNumEntries;
	    // 1.5x expectedNumEntries gives very low probability of
	    // decoding failure
		//int nEntries=_expectedNumEntries + _expectedNumEntries/2;
	   // while (N_HASH * (nEntries/N_HASH) != nEntries) ++nEntries;
	    
	    //nEntries= Math.max(_expectedNumEntries, _expectedNumEntries/2)
	    
	    hashTable = new RDATableEntry[expectedNumEntries];
	    
		for(int i=0;i<expectedNumEntries;i++){
			hashTable[i]=new RDATableEntry();//.setElementAt(, i);
		}
		sampleProbability=1;
		
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHash[i] = LongHashFunction.xx(i);
		}
		
		////System.out.println("expectedNumEntries: "+expectedNumEntries+",HT Size: "+hashTable.length);
	}
	
	
	
	
	public RDATable(int _expectedNumEntries,int NumEntries,int valueSize){
		
		//semaphore
				BFOps =  new Semaphore(1);
		
		this.valueSize=valueSize;
		this.expectedNumEntries = _expectedNumEntries;
		
		hashTable = new RDATableEntry[NumEntries];
		for(int i=0;i<NumEntries;i++){
			hashTable[i]=new RDATableEntry();//.setElementAt(, i);
		}
		
		////System.out.println("expectedNumEntries: "+expectedNumEntries+",HT Size: "+hashTable.length);
		sampleProbability=1;
		
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHash[i] = LongHashFunction.xx(i);
		}
		//////System.out.println("HT Size: "+hashTable.length);
	}
	
	
	
	public RDATable(int _expectedNumEntries,double _sampleProbability){

		//semaphore
				BFOps =  new Semaphore(1);
				
		int _ValueSize = 8;
		this.valueSize=_ValueSize;
		expectedNumEntries = _expectedNumEntries;
	    // 1.5x expectedNumEntries gives very low probability of
	    // decoding failure
		//int nEntries=_expectedNumEntries + _expectedNumEntries/2;
	   // while (N_HASH * (nEntries/N_HASH) != nEntries) ++nEntries;
	    
	    //nEntries= Math.max(_expectedNumEntries, _expectedNumEntries/2)
	    
		hashTable = new RDATableEntry[expectedNumEntries];
		for(int i=0;i<expectedNumEntries;i++){
			hashTable[i]=new RDATableEntry();//.setElementAt(, i);
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
	 * set hash function
	 * @param numHash
	 */
	public void setHash(int numHash){
		this.N_HASH = numHash;
		
	}
	

	/**
	 * 
	 * @param _expectedNumEntries
	 * @param _ValueSize
	 */
	public RDATable(int _expectedNumEntries, int _ValueSize){
		
		//semaphore
				BFOps =  new Semaphore(1);
		
		this.valueSize=_ValueSize;
	    // 1.5x expectedNumEntries gives very low probability of
	    // decoding failure
		//int nEntries=_expectedNumEntries + _expectedNumEntries/2;
	    //while (N_HASH * (nEntries/N_HASH) != nEntries) ++nEntries;
		this.expectedNumEntries = _expectedNumEntries;
		
		hashTable = new RDATableEntry[expectedNumEntries];
		for(int i=0;i<expectedNumEntries;i++){
			hashTable[i]=new RDATableEntry();//.setElementAt(, i);
		}
		
		////System.out.println("expectedNumEntries: "+expectedNumEntries+",HT Size: "+hashTable.length);

		//////System.out.println("HT Size: "+hashTable.length);
	}
	
	/**
	 * copy a table
	 * @return
	 */
	public RDATable copyTable(){
		RDATable a = new RDATable();
		a.hashTable = new RDATableEntry[this.hashTable.length];
		a.valueSize=this.valueSize;
		a.expectedNumEntries=this.expectedNumEntries;
		int count=0;
		while(count<this.hashTable.length){
			RDATableEntry aa = this.hashTable[count].clone();
			a.hashTable[count] = aa;
			count++;
		}
		return a;
		
	}
	
	/**
	 * get a RDA from the array
	 * @param you
	 * @return
	 */
	public static RDATable getRDATable(RDATableEntry[] you){
		RDATable a = new RDATable();
		a.hashTable = new RDATableEntry[you.length];

		a.expectedNumEntries=you.length;
		int count=0;
		while(count<you.length){
			RDATableEntry aa = you[count].clone();
			a.hashTable[count] = aa;
			count++;
		}
		return a;
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
	
	long tmp = LongHashFunction4PKeyHash.hashLong(k);
		//long tmp = FineComb.sHash64.hash(ByteBuffer.allocate(8).putLong(k).array())%64;
	int r = Long.numberOfLeadingZeros(tmp);
	return r;
	}
	
/*	public int[] hash2Cell0(long k){
		
		//index, number of leading zeros
		int []r=new int[2];
		
		byte[] kVec=IBLTEntry.ToByteArray(k);
		int[] h=IBLTEntry.hashC.hash(new Key(kVec),seedSign);
		r[0] = h[0]%(hashTable.length/N_HASH);
		r[1] = Integer.numberOfLeadingZeros(h[0]);
		return r;
	}*/
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
	 void _insert(long id,double TS,long Counter){
		
		//int sign;
		//assert(v.length ==valueSize);
		
		
		try {
			BFOps.acquire();
			
			 int index;
				//byte[] kVec=IBLTEntry.ToByteArray(id);
				int bucketsPerHash = hashTable.length/N_HASH;
			for(int i=0;i<N_HASH;i++){
				int startEntry = i*bucketsPerHash;
				
				//int[] h=IBLTEntry.hashC.hash(id,i);
				int h = hashPos(id, i);
						
				index=startEntry+(h%bucketsPerHash);
				RDATableEntry entry = hashTable[index];
				//if(TS>0){
					//add
				entry.Counter+=Counter;
				//}else{//erase
				//	entry.Counter--;
				//}
				entry.TS+=TS;
				
				/*
				int[] xx = IBLTEntry.hashC.hash(new Key(kVec),seedSign);
				int aa = xx[0]%2;
				if(aa==0){
				entry.STS+=TS;
				}else{
				entry.STS-=TS;
				}
				*/
				entry.keySum ^= id;
				//h=HashTableEntry.hashC.hash(new Key(kVec),HashTableEntry.N_HASHCHECK);
				
				
				entry.keyCheck ^=LongHashFunction4PKeyHash.hashLong(id);
				
				//FineComb.sHash64.hash(ByteBuffer.allocate(8).putLong(id).array());
				//if(entry.empty()){
				//	entry.valueSum=ByteBuffer.allocate(entry.valueArrayLen).putLong(0).array();
				//}else{
				//entry.addValue(v);
				hashTable[index]=entry;
				//h=null;
			}
			//kVec=null;
			BFOps.release();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	 
	 /**
	  * erase item
	  * @param TS
	  * @param k
	  * @param v
	  */
//	public void erease(long id, double TS,long Counter){
//		_insert(id,-TS,-Counter);
//		
//	}
	
    // Returns true if a result is definitely found or not
    // found. If not found, result will be empty.
    // Returns false if overloaded and we don't know whether or
    // not k is in the table.
//	public byte[] get(long k){
//		 byte[] v=null;
//	    byte[] kVec = IBLTEntry.ToByteArray(k);
//	    int bucketsPerHash = hashTable.length/N_HASH;
//	    for(int i=0;i<N_HASH;i++){
//	    	int startEntry = i*bucketsPerHash;
//	    	int[]h = IBLTEntry.hashC.hash(new Key(kVec),i);
//	    	RDATableEntry entry = hashTable[startEntry+(h[0]%bucketsPerHash)];
//	    	if(entry.empty()){
//	    		 // Definitely not in table. Leave
//	            // result empty, return true.
//	    		////System.out.println("empty");
//	    		return null;
//	    	}else if(entry.isPure()){
//	    		if(entry.keySum==k){
//	    			 // Found!
//	    			byte[] vv = entry.valueSum;
//	    			v=new byte[vv.length];
//	    			
//	    			for(int vi=0;vi<v.length;vi++){
//	    				v[vi]=vv[vi];
//	    			}
//	    			////System.out.println("found! pure");
//	    			return v;
//	    		}else{
//		    		// Definitely not in table.
//		    		////System.out.println("not in");
//		    		return null;
//		    	}
//	    	}    		        
//	    }
//	    
//	    /*
//	     * 	
//	     * 
//	     */
//	    
//	 // Don't know if k is in table or not; "peel" the IBLT to try to find
//        // it, peel blind!!!delete these items permanantly
//	    RDATable peeled = this;
//	    int nErased = 0;
//	    for(int i=0;i<peeled.hashTable.length;i++){
//	    	RDATableEntry entry = peeled.hashTable[i];
//	    	if(entry.isPure()){
//	    		if(entry.keySum==k){
//	    			//found!
//	    			byte[] vv = entry.valueSum;
//	    			v=new byte[vv.length];
//	    			for(int vi=0;vi<vv.length;vi++){
//	    				v[vi]=vv[vi];
//	    			}
//	    			////System.out.println("Found, peeled");
//	    			return v;
//	    		}
//	    		nErased++;
//	    		peeled._insert(entry.keySum,-entry.TS,-entry.Counter,entry.valueSum);
//	    	}
//	    }
//	    if (nErased > 0) {
//	        // Recurse with smaller IBLT
//	        return peeled.get(k);
//	    }
//	    ////System.out.println("not in after peel");
//	    return null;
//	}
	
    // Adds entries to the given sets:
    //  positive is all entries that were inserted
    //  negative is all entreis that were erased but never added (or
    //   if the IBLT = A-B, all entries in B that are not in A)
    // Returns true if all entries could be decoded, false otherwise.
//	public boolean decode(HashSet<Pair<Long,byte[]>> positive,HashSet<Pair<Long,byte[]>> negative){
//		
//		  RDATable peeled = this.copyTable();
//		  int nErased = 0;
//		  while(nErased > 0){
//			  nErased = 0;
//			  for(int i=0;i<peeled.hashTable.length;i++){
//				 RDATableEntry entry = peeled.hashTable[i];
//				  if(entry.isPure()){
//					  if(entry.Counter==1){
//						  positive.add(new Pair<Long,byte[]>(entry.keySum,entry.valueSum));
//					  }else{
//						  negative.add(new Pair<Long,byte[]>(entry.keySum,entry.valueSum));
//					  }
//					  
//					nErased++;
//			    	peeled._insert(entry.keySum,-entry.TS,-entry.Counter,entry.valueSum);
//
//				  }
//				  
//			  }
//			  
//		  }//end
//		    // If any buckets for one of the hash functions is not empty,
//		    // then we didn't peel them all:
//		    for (int i = 0; i < peeled.hashTable.length/N_HASH; i++) {
//		        if (peeled.hashTable[i].empty() != true) return false;
//		    }
//		    return true; 
//	}

	/**
	 * decode ids
	 * @param positive
	 * @param negative
	 * @return
	 */
	public boolean decodeIDs(HashSet<Long> positive,HashSet<Long> negative){
		
		  RDATable peeled = this.copyTable();
		  int nErased = 0;
		  //////System.out.println("peeled: "+peeled.hashTable.length);
		  while(true){
			  nErased = 0;
			  for(int i=0;i<peeled.hashTable.length;i++){
				 RDATableEntry entry = peeled.hashTable[i];
				 	//////System.out.println("$Decode: "+entry.toString());
				  if(entry.isPure()){
					  //////System.out.print(" pure!!!\n");
					  if(entry.Counter==1){
						  positive.add(entry.keySum);
					  }else{
						  negative.add(entry.keySum);
					  }
					  
					nErased++;
			    	peeled._insert(entry.keySum,-entry.TS,-entry.Counter);

				  }
				  
			  }
			  
			  if(nErased<=0){
				  break;
			  }
		  }//end
		  
		  //////System.out.println("IDs: "+positive.size()+", you: "+negative.size());
		    // If any buckets for one of the hash functions is not empty,
		    // then we didn't peel them all:
		    for (int i = 0; i < peeled.hashTable.length; i++) {
		    	
		        if (peeled.hashTable[i].empty() != true){
		        	////System.out.println("peel: "+peeled.hashTable[i].toString());
		        	return false;}
		    }
		    return true; 
	}
	
	/**
	 * decode ids
	 * @param positive
	 * @param negative
	 * @return
	 */
	
	public int decodeIDs(){
		
		  RDATable peeled = this.copyTable();
		  //////System.out.println("peeled: "+peeled.hashTable.length);
		  Queue<Integer> indexPureBuckets = new  LinkedList<Integer>();
		  for(int i=0;i<peeled.hashTable.length;i++){
			  RDATableEntry entry = peeled.hashTable[i];
			 	//////System.out.println("$Decode: "+entry.toString());
			  if(entry.isPure()){
				  indexPureBuckets.add(i);
			  }//add index
			  }
		  
		  int totalCount=0;
		  while(!indexPureBuckets.isEmpty()){
			  Integer index = indexPureBuckets.remove();
			  RDATableEntry entry = peeled.hashTable[index];
			  totalCount++;			
				peeled._insert(entry.keySum,-entry.TS,-entry.Counter);
				//get the indexes
				Vector<Integer> newPositions = getHashLocation(entry.keySum,index);
				if(!newPositions.isEmpty()){
					indexPureBuckets.addAll(newPositions);
				}
		  }//iterate the buckets
		  
		  return totalCount;
	}
	
	
	public long[] decodeIDsTime(){
		
		  RDATable peeled = this.copyTable();
		  //////System.out.println("peeled: "+peeled.hashTable.length);
		  Queue<Integer> indexPureBuckets = new  LinkedList<Integer>();
		  		  
		  long time1=System.nanoTime();
		  for(int i=0;i<peeled.hashTable.length;i++){
			  RDATableEntry entry = peeled.hashTable[i];
			 	//////System.out.println("$Decode: "+entry.toString());
			  if(entry.isPure()){
				  indexPureBuckets.add(i);
			  }//add index
			  }
		  
		  int totalCount=0;
		  while(!indexPureBuckets.isEmpty()){
			  Integer index = indexPureBuckets.remove();
			  RDATableEntry entry = peeled.hashTable[index];
			  totalCount++;			
				peeled._insert(entry.keySum,-entry.TS,-entry.Counter);
				//get the indexes
				Vector<Integer> newPositions = getHashLocation(entry.keySum,index);
				if(!newPositions.isEmpty()){
					indexPureBuckets.addAll(newPositions);
				}
		  }//iterate the buckets
		  
		  long time3 = System.nanoTime();
			
			long delay1=time3-time1;
			long[] results={totalCount,delay1};
		  return results;
	}
	/**
	 * get pure positions
	 * @param id
	 * @param removeIndex
	 * @return
	 */
	private Vector<Integer> getHashLocation(long id,int removeIndex) {
		// TODO Auto-generated method stub
		Vector<Integer> indexes=new Vector<Integer>();
		 int index;
			//byte[] kVec=IBLTEntry.ToByteArray(id);
			int bucketsPerHash = hashTable.length/N_HASH;
			for(int i=0;i<N_HASH;i++){
				int startEntry = i*bucketsPerHash;
				
				//int[] h=IBLTEntry.hashC.hash(new Key(kVec),i);
				int h = hashPos(id, i);
				
				index=startEntry+(h%bucketsPerHash);
				RDATableEntry entry;
				if(index == removeIndex){
					continue;
				}else
				entry= hashTable[index];
				if(entry.empty()){
					continue;
				}
				if(entry.isPure()){
					indexes.add(index);
				}
				
			}
			return indexes;
	}

	public Set<Long> decodeAllIDs(){
		  Set<Long> ids = new HashSet<Long>();
		  RDATable peeled = this.copyTable();
		  int nErased = 0;
		  //////System.out.println("peeled: "+peeled.hashTable.length);
		  //int totalCount=0;
		  while(true){
			  nErased = 0;
			  for(int i=0;i<peeled.hashTable.length;i++){
				 RDATableEntry entry = peeled.hashTable[i];
				 	//////System.out.println("$Decode: "+entry.toString());
				  if(entry.isPure()){
					  //////System.out.print(" pure!!!\n");
					// totalCount++;
					  ids.add(entry.keySum);
					nErased++;
			    	peeled._insert(entry.keySum,-entry.TS,-entry.Counter);

				  }
				  
			  }
			  
			  if(nErased<=0){
				  break;
			  }
		  }//end
		  
		  return ids;
		  //return totalCount;
	}
	
	 // Subtract two IBLTs
	public RDATable subtractIBLT(RDATable other){
	    // IBLT's must be same params/size:
	    //assert(valueSize == other.valueSize);
	    //assert(hashTable.length == other.hashTable.length);
		
	    RDATable result = this.copyTable();
	    int size=result.hashTable.length;
	    ////System.out.println("$subtractSize: "+size);
	    for (int i = 0; i < size; i++) {
	        RDATableEntry e1 = result.hashTable[i];
	        RDATableEntry e2 = other.hashTable[i];
	        //e1.Counter-=e2.Counter;
	       // e1.TS -= e2.TS;
	        //e1.STS-=e2.STS;
	        //e1.keySum ^= e2.keySum;
	        //e1.keyCheck ^= e2.keyCheck;
	        
	        RDATableEntry newItem = e1.XORHashTable(e2);
	        
//	        if (e1.empty()) {
//	            e1.valueSum=ByteBuffer.allocate(e1.valueArrayLen).putLong(0).array();
//	        }
//	        else {
	        
	       // }
	        result.hashTable[i]= newItem;
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
	
	/**
	 * average TS
	 * @param subtractTBF, the subtraction TBF after repair;
	 * @param repairedTBF, one TBF participates in the subtraction
	 * @return
	 */
	public double getAvgTS(RDATable subtractTBF,RDATable repairedTBF){
		
		int c=0;
		double sum=0;
		for(int i=0;i<subtractTBF.hashTable.length;i++){
			//use only buckets that have insertions
			long t = repairedTBF.hashTable[i].getNumItems();
			if(t>0){
				sum+=Math.abs(subtractTBF.hashTable[i].TS);
				c+=t;
			}
		}
		return sum/c;
	}
	
	/**
	 * average score
	 * @param RReceiver
	 * @return
	 */
	public double getAvgTS(RDATable RReceiver){
		RDATable subtractTBF = this.subtractIBLT(RReceiver);		
		int c=0;
		double sum=0;
		int total=0;
		int good=0;
		int size=subtractTBF.hashTable.length;
		//////System.out.println("$size: "+size+", receiverSize: "+RReceiver.hashTable.length);
		for(int i=0;i<size;i++){
			//useful
			if(subtractTBF.hashTable[i].Counter==0&&
					!RReceiver.hashTable[i].empty()&&!this.hashTable[i].empty()&&
					this.hashTable[i].Counter!=0&&
			this.hashTable[i].keyCheck==RReceiver.hashTable[i].keyCheck&&
					this.hashTable[i].keySum==RReceiver.hashTable[i].keySum){
			//use only buckets that have insertions
				
			long t = Math.abs(RReceiver.hashTable[i].getNumItems());
			//////System.out.println("$size:"+t);
			if(t>0){
				sum+=Math.abs(subtractTBF.hashTable[i].TS);
				c+=t;
			}
			good++;
			total++;
			}else{
				if(this.hashTable[i].keySum!=RReceiver.hashTable[i].keySum){
					total++;
				}
//				System.err.println("Incorrect: "+
//			this.hashTable[i].keyCheck+"="+RReceiver.hashTable[i].keyCheck+","+
//						this.hashTable[i].keySum+"="+RReceiver.hashTable[i].keySum+","+
//						subtractTBF.hashTable[i].Counter);
			}
		}
		
		log.main("AvgUnUsed: "+(total-good+0.0)/total);
		if(c>0){
		return sum/c;
		}else{
			return -1;
		}
	}
	/**
	 * 
	 * @param subtractTBF
	 * @param repairedTBF
	 * @return
	 */
	public double getSTD(RDATable other11,double avg){
		double aa=(getFScoreByCollapse(other11,avg));
		//double bb=Math.pow(avg,2);
		//////System.out.println("fScore: "+aa+",avgSquare: "+bb);		
		return aa;
	}
	
	/**
	 * collapse the bank, must remove all empty banks
	 * @return
	 */
	public RDATable collapseAdjacentBank(){
		
		//process
		//divide to N_HASH parts
		//for each part, skip the empty buckets
		//for a pair, construct the adjacent pair
		
		int halvedSizePart=this.hashTable.length/N_HASH;		
		//store the collapsed result
		RDATable CollapseTable = new RDATable();
		Vector<RDATableEntry> collapsedRDA = new Vector<RDATableEntry>();
		CollapseTable.N_HASH=this.N_HASH;
		
		//global index
		int start=0;
		//index of the part
		int indexHash=1;
		while(indexHash<=N_HASH){
		for(int i=start;i<indexHash*halvedSizePart;i++){
			
			//skip
			if((i*2+1)>=indexHash*halvedSizePart||this.hashTable[i*2].empty()||this.hashTable[i*2+1].empty()){
				continue;
			}
			RDATableEntry entry = new RDATableEntry();
			entry.TS=this.hashTable[i*2].TS-this.hashTable[i*2+1].TS;
			entry.Counter=this.hashTable[i*2].Counter+this.hashTable[i*2+1].Counter;
			entry.keySum=this.hashTable[i*2].keySum^this.hashTable[i*2+1].keySum;
			entry.keyCheck=this.hashTable[i*2].keyCheck^this.hashTable[i*2+1].keyCheck;
			collapsedRDA.add(entry);
		}
		indexHash++;
		start+=halvedSizePart;
		
		}
		//copy
		CollapseTable.hashTable=new RDATableEntry[collapsedRDA.size()];
		CollapseTable.expectedNumEntries = CollapseTable.hashTable.length;
		for(int i=0;i<CollapseTable.hashTable.length;i++){
			CollapseTable.hashTable[i]=collapsedRDA.get(i).clone();
		}
		collapsedRDA.clear();
		return CollapseTable;
	}
	
	
	public RDATable[] CoordinatedCollapseAdjacentBank(RDATable you,
			Vector<Integer> intervals,
			double[]branchAvg){
		
		//process
		//divide to N_HASH parts
		//for each part, skip the empty buckets
		//for a pair, construct the adjacent pair
		
		//counterSum[0] = 0;
		
		//int halvedSizePart=this.hashTable.length/N_HASH;		
		//store the collapsed result
		RDATable[] aa = new RDATable[2];
		aa[0] = new RDATable();
		aa[1] = new RDATable();
		RDATable  CollapseTableMe = aa[0];
		
		Vector<RDATableEntry> meVec = new Vector<RDATableEntry>();
		Vector<RDATableEntry> youVec = new Vector<RDATableEntry>();
		
		CollapseTableMe.N_HASH=this.N_HASH;
		CollapseTableMe.expectedNumEntries=this.expectedNumEntries;
		CollapseTableMe.requiredLeadingZeros=this.requiredLeadingZeros;
		RDATable CollapseTableYou = aa[1];
		CollapseTableYou.N_HASH=this.N_HASH;
		CollapseTableYou.expectedNumEntries=this.expectedNumEntries;
		CollapseTableYou.requiredLeadingZeros=this.requiredLeadingZeros;
		int interval=0;
		//global index
		int start=0;
		//index of the part
		int indexHash=1;
		int indexAInTable, indexBInTable;
		
		int bucketsPerHash = you.hashTable.length/N_HASH;
		
		int[] sign = new int[ bucketsPerHash];
		for(int i=0;i< bucketsPerHash;i++){
			if(i%2==0){
			sign[i] =1;
			}else{
			sign[i] =-1;	
			}
		}
		
		List<Integer>  computeIndexes=new ArrayList<Integer>();
		List<Integer>  SignIndexes=new ArrayList<Integer>();
		//iterate
		for(int iii=0;iii<N_HASH;iii++){
			
			int counterPackets =0;
			
			int startEntry = iii*bucketsPerHash;
			
			int endEntry=bucketsPerHash+startEntry;
			
						
			 computeIndexes.clear();
			 SignIndexes.clear();
			 int signAtom = 1;
			for(int i=startEntry;i<endEntry;i++){
								
				if(this.hashTable[i].Counter!=you.hashTable[i].Counter||
						//this.hashTable[i].keySum!=you.hashTable[i].keySum||
						this.hashTable[i].keyCheck!=you.hashTable[i].keyCheck){
					continue;
				}else{
					 computeIndexes.add(i);
					 SignIndexes.add(signAtom);
					// System.out.println("i: "+(i-startEntry)+", sign: "+signAtom);
					 signAtom*=-1;
					//sum
					branchAvg[iii]+=Math.abs(this.hashTable[i].TS-you.hashTable[i].TS);
					//avg
					counterPackets+=this.hashTable[i].Counter;
					
				}
				//}
			}
			//average latency
			branchAvg[iii]/=counterPackets;
			
			
			if(computeIndexes.isEmpty()){
				continue;
			}
			
			//remove the singleton item
			int ItemSize0=computeIndexes.size();
			if(computeIndexes.size()%2==1){
				ItemSize0--;
			}
			//add at the same position
			int halfSize=ItemSize0/2;
		for(int ii=0;ii<halfSize;ii++){
			indexAInTable=computeIndexes.get(ii*2);
			indexBInTable=computeIndexes.get(ii*2+1);
			
			int signA = SignIndexes.get(ii*2);
			int signB = SignIndexes.get(ii*2+1);
			
			//skip
			RDATableEntry entry = new RDATableEntry();
			entry.TS=signA*this.hashTable[indexAInTable].TS + signB*this.hashTable[indexBInTable].TS;
			entry.Counter=this.hashTable[indexAInTable].Counter+this.hashTable[indexBInTable].Counter;
			entry.keySum=this.hashTable[indexAInTable].keySum^this.hashTable[indexBInTable].keySum;
			entry.keyCheck=this.hashTable[indexAInTable].keyCheck^this.hashTable[indexBInTable].keyCheck;
			meVec.add(entry);
			//CollapseTableMe.hashTable.add(entry);
			
			RDATableEntry YouEntry = new RDATableEntry();
			YouEntry.TS=signA*you.hashTable[indexAInTable].TS + signB*you.hashTable[indexBInTable].TS;
			YouEntry.Counter=you.hashTable[indexAInTable].Counter+you.hashTable[indexBInTable].Counter;
			YouEntry.keySum=you.hashTable[indexAInTable].keySum^you.hashTable[indexBInTable].keySum;
			YouEntry.keyCheck=you.hashTable[indexAInTable].keyCheck^you.hashTable[indexBInTable].keyCheck;
			youVec.add(YouEntry);
			//CollapseTableYou.hashTable.add(YouEntry);
			
			//counterSum[0]+=YouEntry.Counter;
		}
		interval=youVec.size();//CollapseTableYou.hashTable.length;
		intervals.add(interval);
		}
		//copy
		CollapseTableMe.hashTable = new RDATableEntry[meVec.size()];
		CollapseTableYou.hashTable = new RDATableEntry[youVec.size()];
		CollapseTableMe.expectedNumEntries = CollapseTableMe.hashTable.length;
		CollapseTableYou.expectedNumEntries = CollapseTableYou.hashTable.length;
		for(int i=0;i<CollapseTableMe.expectedNumEntries;i++){
			CollapseTableMe.hashTable[i]=meVec.get(i).clone();
			CollapseTableYou.hashTable[i]=youVec.get(i).clone();
		}
		
		return aa;
	}
	
//	public RDATable[] CoordinatedCollapseAdjacentBank(
//			RDATable you,Vector<Integer> intervals){
//		
//		//process
//		//divide to N_HASH parts
//		//for each part, skip the empty buckets
//		//for a pair, construct the adjacent pair
//		
//		//int halvedSizePart=this.hashTable.length/N_HASH;		
//		//store the collapsed result
//		RDATable[] aa = new RDATable[2];
//		aa[0] = new RDATable();
//		aa[1] = new RDATable();
//		RDATable  CollapseTableMe = aa[0];
//		CollapseTableMe.N_HASH=this.N_HASH;
//		RDATable CollapseTableYou = aa[1];
//		CollapseTableYou.N_HASH=this.N_HASH;
//		
//		int interval=0;
//		//global index
//		int start=0;
//		//index of the part
//		int indexHash=1;
//		int indexAInTable, indexBInTable;
//		
//		int bucketsPerHash = hashTable.length/N_HASH;
//		List<Integer>  computeIndexes=new ArrayList<Integer>();
//		//iterate
//		for(int iii=0;iii<N_HASH;iii++){
//			int startEntry = iii*bucketsPerHash;
//			
//			int endEntry=bucketsPerHash+startEntry;
//			
//			 computeIndexes.clear();
//			for(int i=startEntry;i<endEntry;i++){
//				if(this.hashTable[i].empty()||you.hashTable[i].empty()||
//						this.hashTable[i].Counter!=you.hashTable[i].Counter||
//						this.hashTable[i].keySum!=you.hashTable[i].keySum){
//					continue;
//				}else{
//					 computeIndexes.add(i);
//				}
//			}
//			if(computeIndexes.isEmpty()){
//				continue;
//			}
//			int ItemSize=computeIndexes.size();
//			//add at the same position
//			
//		for(int ii=0;ii<ItemSize;ii++){
//			if(ii*2+1>=ItemSize){
//				break;
//			}
//			indexAInTable=computeIndexes.get(ii*2);
//			indexBInTable=computeIndexes.get(ii*2+1);
//			
//			
//			//skip
//			RDATableEntry entry = new RDATableEntry();
//			entry.TS=this.hashTable[indexAInTable].TS-this.hashTable[indexBInTable].TS;
//			entry.Counter=this.hashTable[indexAInTable].Counter+this.hashTable[indexBInTable].Counter;
//			entry.keySum=this.hashTable[indexAInTable].keySum^this.hashTable[indexBInTable].keySum;
//			entry.keyCheck=this.hashTable[indexAInTable].keyCheck^this.hashTable[indexBInTable].keyCheck;
//			CollapseTableMe.hashTable.add(entry);
//			
//			RDATableEntry YouEntry = new RDATableEntry();
//			YouEntry.TS=you.hashTable[indexAInTable].TS-you.hashTable[indexBInTable].TS;
//			YouEntry.Counter=you.hashTable[indexAInTable].Counter+you.hashTable[indexBInTable].Counter;
//			YouEntry.keySum=you.hashTable[indexAInTable].keySum^you.hashTable[indexBInTable].keySum;
//			YouEntry.keyCheck=you.hashTable[indexAInTable].keyCheck^you.hashTable[indexBInTable].keyCheck;
//			CollapseTableYou.hashTable.add(YouEntry);
//			
//		}
//		interval=CollapseTableYou.hashTable.length;
//		intervals.add(interval);
//		}
//		return aa;
//	}
	
	private List<Integer> getNonEmptyIndexes(int start, int end, RDATable you) {
		// TODO Auto-generated method stub
		List<Integer> ll=new ArrayList<Integer>();
		for(int i=start;i<end;i++){
			if(this.hashTable[i].empty()||you.hashTable[i].empty()||
					this.hashTable[i].Counter!=you.hashTable[i].Counter){
				continue;
			}else{
				ll.add(i);
			}
		}
		return ll;
	}

	/**
	 * collapse the bank, to derive the standard deviation
	 * @param other11
	 * @return
	 */
	
	public double getFScoreByCollapseEachBankAverage(RDATable other11,double avg){
		
		Vector<Integer> intervalHashs = new Vector<Integer>();
		double[] counterSum = {0};
		RDATable[] out=CoordinatedCollapseAdjacentBank(other11,intervalHashs,counterSum);
			//do not collapse
			RDATable me = out[0];
			RDATable you = out[1];
							
			double TASum=0;
			double TBSum=0;
			//double squaredDiff=0;
			double allSquaredSum=0;
			double totalSumCounter=0;
			//double S= 0;
			double oneRound=0;
			int start=0;int end=0;
			//int count = 0;
			//double v0;
			double stdOneRound=0;
			//separate each hash procedure
			for(int indexHash=0;indexHash<N_HASH;indexHash++){
				//////System.out.println("Table: "+you.hashTable.length+", "+me.hashTable.length);
				//////System.out.println("s: "+start+", end: "+end+", total: "+intervalHashs.get(intervalHashs.size()-1));
				
				
				end=Math.min(intervalHashs.get(indexHash),you.hashTable.length);
				//reset for each partition hashed
				if(end==0){
					break;
				}
				
				totalSumCounter=0;
				allSquaredSum=0;
				for(int i=start;i<end;i++){
					if(you.hashTable[i].empty()||
							me.hashTable[i].empty()||
							me.hashTable[i].Counter==0){
						System.err.println("empty");
						continue;
					}
					if(me.hashTable[i].keySum!=you.hashTable[i].keySum||
							me.hashTable[i].Counter!=you.hashTable[i].Counter){
						//System.err.println("unequal! ");
						continue;
					}
					
				//	if(me.hashTable[i].Counter!=0&&
					//		me.hashTable[i].keySum==you.hashTable[i].keySum&&
					//		me.hashTable[i].Counter==you.hashTable[i].Counter){
					totalSumCounter+=(me.hashTable[i].Counter);	
							TASum=me.hashTable[i].TS;
							TBSum=you.hashTable[i].TS;
							//System.out.print((TBSum - TASum)+",^2:"+(Math.pow(TBSum - TASum,2))+",#: "+me.hashTable[i].Counter+"\n");
							allSquaredSum+=Math.pow(TBSum - TASum,2);	
							
				//	}
					
				}//end a round
				stdOneRound+=Math.abs(allSquaredSum/totalSumCounter - Math.pow(avg,2));

				//next round
				start=end;
			}
			

				//double stdValue= Math.abs(allSquaredSum/totalSumCounter - Math.pow(avg,2));
			double stdValue=  stdOneRound/N_HASH;
	
			
			//double v=count>0?oneRound/count:-1;
			//////System.out.print( "avg: "+v+"\n");
			
			intervalHashs.clear();
			intervalHashs=null;
			me.clear();
			you.clear();
			me=null;
			you=null;
			
			return stdValue;

		}
	
	
	
	public double getFScoreByCollapse(RDATable other11,double avg000){
		
		Vector<Integer> intervalHashs = new Vector<Integer>();
		//branch average
		double[] BranchAvg = new double[this.N_HASH];
		for(int i=0;i<this.N_HASH;i++){
			BranchAvg[i]=0;
		}
		RDATable[] out=CoordinatedCollapseAdjacentBank(other11,intervalHashs,BranchAvg);
		if(intervalHashs.isEmpty()){
			return -1;
		}
			//do not collapse
			RDATable me = out[0];
			RDATable you = out[1];
							
			double TASum=0;
			double TBSum=0;
			//double squaredDiff=0;
			double allSquaredSum=0;
			double totalSumCounter=0;
			//double S= 0;
			double oneRound=0;
			int start=0;int end=0;
			//int count = 0;
			//double v0;
			//separate each hash procedure
			/*for(int i=0;i<you.hashTable.length;i++){
				if(you.hashTable[i].empty()||
						me.hashTable[i].empty()||
						me.hashTable[i].Counter==0){
					System.err.println("empty");
					continue;
				}
				if(me.hashTable[i].keySum!=you.hashTable[i].keySum||
						me.hashTable[i].Counter!=you.hashTable[i].Counter||
						me.hashTable[i].keyCheck!=you.hashTable[i].keyCheck){
					System.err.println("unequal! ");
					continue;
				}
				
				totalSumCounter+=(me.hashTable[i].Counter);	
				TASum=me.hashTable[i].TS;
				TBSum=you.hashTable[i].TS;
				//System.out.print((TBSum - TASum)+",^2:"+(Math.pow(TBSum - TASum,2))+",#: "+me.hashTable[i].Counter+"\n");
				allSquaredSum+=Math.pow(TBSum - TASum,2);	
			}
			*/
			
			List<Double> eachValue = new ArrayList<Double>();
 			double avgDouble=0;
			double sumAllSquared=0;
			double countAll=0;
			double avgSquaredAll=0;
			double countAllItems=0;
			double countUnequalItems = 0;
			for(int indexHash=0;indexHash<N_HASH;indexHash++){
				//////System.out.println("Table: "+you.hashTable.length+", "+me.hashTable.length);
				//////System.out.println("s: "+start+", end: "+end+", total: "+intervalHashs.get(intervalHashs.size()-1));
				
				
				//end = intervalHashs.get(indexHash);
				end=Math.min(intervalHashs.get(indexHash),you.hashTable.length);
				//reset for each partition hashed
				if(end==0||start==end){
					break;
				}
				
				allSquaredSum = 0;
				totalSumCounter = 0;
				
				for(int i=start;i<end;i++){
					/*if(you.hashTable[i].empty()||
							me.hashTable[i].empty()||
							me.hashTable[i].Counter==0){
						System.err.println("empty");
						continue;
					}
					*/
					countAllItems++;
					if(me.hashTable[i].keySum!=you.hashTable[i].keySum||
							me.hashTable[i].Counter!=you.hashTable[i].Counter||
							me.hashTable[i].keyCheck!=you.hashTable[i].keyCheck){
						//System.err.println("unequal! ");
						countUnequalItems++;
						continue;
					}
					
				//	if(me.hashTable[i].Counter!=0&&
					//		me.hashTable[i].keySum==you.hashTable[i].keySum&&
					//		me.hashTable[i].Counter==you.hashTable[i].Counter){
					totalSumCounter+=(me.hashTable[i].Counter);	
							TASum=me.hashTable[i].TS;
							TBSum=you.hashTable[i].TS;
							//System.out.print((TBSum - TASum)+",^2:"+(Math.pow(TBSum - TASum,2))+",#: "+me.hashTable[i].Counter+"\n");
							allSquaredSum+=Math.pow(TBSum - TASum,2);	
							
				//	}
					
				}//end a round
				
				if(totalSumCounter==0){
					 continue;
				}
				
				sumAllSquared+=allSquaredSum;
				countAll+=totalSumCounter;
				avgSquaredAll+=BranchAvg[indexHash];
				double stdValue= Math.abs(allSquaredSum/totalSumCounter - Math.pow(BranchAvg[indexHash],2));
				eachValue.add(stdValue);
				avgDouble+=stdValue;
				//System.out.print(" "+stdValue);
			}

			avgDouble/=N_HASH;
			//double median = math.percentile(eachValue,.5);
			//double avgDouble0 = Math.abs( sumAllSquared/countAll - Math.pow(avgSquaredAll/N_HASH,2));

			//System.out.println("\n===========\n"+avgDouble+", "+avgDouble0+"\n===========\n");
				//double stdValue= Math.abs(allSquaredSum/totalSumCounter - Math.pow(avg,2));
			eachValue.clear();
			log.main("StdUnequalFrac: "+countUnequalItems/(countAllItems+0.0));
			
			//double v=count>0?oneRound/count:-1;
			//////System.out.print( "avg: "+v+"\n");
			
			intervalHashs.clear();
			intervalHashs=null;
			me.clear();
			you.clear();
			me=null;
			you=null;
			
			return avgDouble;

		}
		
	MathUtil math =new MathUtil(100);
	
	public double getFScoreByCollapseSeparate(RDATable other11,double avg000){
		
		Vector<Integer> intervalHashs = new Vector<Integer>();
		//branch average
		double[] BranchAvg = new double[this.N_HASH];
		for(int i=0;i<this.N_HASH;i++){
			BranchAvg[i]=0;
		}
		RDATable[] out=CoordinatedCollapseAdjacentBank(other11,intervalHashs,BranchAvg);
			//do not collapse
			RDATable me = out[0];
			RDATable you = out[1];
							
			double TASum=0;
			double TBSum=0;
			//double squaredDiff=0;
			double allSquaredSum=0;
			double totalSumCounter=0;
			//double S= 0;
			double oneRound=0;
			int start=0;int end=0;
			//int count = 0;
			//double v0;
			//separate each hash procedure
			/*for(int i=0;i<you.hashTable.length;i++){
				if(you.hashTable[i].empty()||
						me.hashTable[i].empty()||
						me.hashTable[i].Counter==0){
					System.err.println("empty");
					continue;
				}
				if(me.hashTable[i].keySum!=you.hashTable[i].keySum||
						me.hashTable[i].Counter!=you.hashTable[i].Counter||
						me.hashTable[i].keyCheck!=you.hashTable[i].keyCheck){
					System.err.println("unequal! ");
					continue;
				}
				
				totalSumCounter+=(me.hashTable[i].Counter);	
				TASum=me.hashTable[i].TS;
				TBSum=you.hashTable[i].TS;
				//System.out.print((TBSum - TASum)+",^2:"+(Math.pow(TBSum - TASum,2))+",#: "+me.hashTable[i].Counter+"\n");
				allSquaredSum+=Math.pow(TBSum - TASum,2);	
			}
			*/
			
			double avgDouble=0;
			for(int indexHash=0;indexHash<N_HASH;indexHash++){
				//////System.out.println("Table: "+you.hashTable.length+", "+me.hashTable.length);
				//////System.out.println("s: "+start+", end: "+end+", total: "+intervalHashs.get(intervalHashs.size()-1));
				
				
				end=Math.min(intervalHashs.get(indexHash),you.hashTable.length);
				//reset for each partition hashed
				if(end==0){
					break;
				}
				
				allSquaredSum = 0;
				totalSumCounter = 0;
				
				for(int i=start;i<end;i++){
					if(you.hashTable[i].empty()||
							me.hashTable[i].empty()||
							me.hashTable[i].Counter==0){
						System.err.println("empty");
						continue;
					}
					if(me.hashTable[i].keySum!=you.hashTable[i].keySum||
							me.hashTable[i].Counter!=you.hashTable[i].Counter||
							me.hashTable[i].keyCheck!=you.hashTable[i].keyCheck){
						System.err.println("unequal! ");
						continue;
					}
					
				//	if(me.hashTable[i].Counter!=0&&
					//		me.hashTable[i].keySum==you.hashTable[i].keySum&&
					//		me.hashTable[i].Counter==you.hashTable[i].Counter){
					totalSumCounter+=(me.hashTable[i].Counter);	
							TASum=me.hashTable[i].TS;
							TBSum=you.hashTable[i].TS;
							//System.out.print((TBSum - TASum)+",^2:"+(Math.pow(TBSum - TASum,2))+",#: "+me.hashTable[i].Counter+"\n");
							allSquaredSum+=Math.pow(TBSum - TASum,2);	
							
				//	}
					
				}//end a round
				
				double stdValue= Math.abs(allSquaredSum/totalSumCounter - Math.pow(indexHash,2));
				avgDouble+=stdValue;
				//next round
				start=end;
		
			}

					avgDouble/=N_HASH;
			

				//double stdValue= Math.abs(allSquaredSum/totalSumCounter - Math.pow(avg,2));
			
	
			
			//double v=count>0?oneRound/count:-1;
			//////System.out.print( "avg: "+v+"\n");
			
			intervalHashs.clear();
			intervalHashs=null;
			me.clear();
			you.clear();
			me=null;
			you=null;
			
			return avgDouble;

		}
		
	
public double getFScoreByCollapse0(RDATable other11,double avg){
		
	Vector<Integer> intervalHashs = new Vector<Integer>();
	double[] counterSum = {0};
	RDATable[] out=CoordinatedCollapseAdjacentBank(other11,intervalHashs,counterSum);
		//do not collapse
		RDATable me = out[0];
		RDATable you = out[1];
						
		double TASum=0;
		double TBSum=0;
		double squaredDiff=0;
		double S= 0;
		double oneRound=0;
		int start=0;int end=0;
		int count = 0;
		double v0;
		//separate each hash procedure
		for(int indexHash=0;indexHash<N_HASH;indexHash++){
			//////System.out.println("Table: "+you.hashTable.length+", "+me.hashTable.length);
			//////System.out.println("s: "+start+", end: "+end+", total: "+intervalHashs.get(intervalHashs.size()-1));
			
			
			end=Math.min(intervalHashs.get(indexHash),you.hashTable.length);
			//reset for each partition hashed
			if(end==0){
				break;
			}
			S=0;
			squaredDiff=0;
			for(int i=start;i<end;i++){
				if(you.hashTable[i].empty()||
						me.hashTable[i].empty()||
						me.hashTable[i].Counter==0){
					System.err.println("empty");
					continue;
				}
				if(me.hashTable[i].keySum!=you.hashTable[i].keySum||
						me.hashTable[i].Counter!=you.hashTable[i].Counter){
					//System.err.println("unequal! ");
					continue;
				}
				
			//	if(me.hashTable[i].Counter!=0&&
				//		me.hashTable[i].keySum==you.hashTable[i].keySum&&
				//		me.hashTable[i].Counter==you.hashTable[i].Counter){
						S+=(me.hashTable[i].Counter);	
						TASum=me.hashTable[i].TS;
						TBSum=you.hashTable[i].TS;
						//System.out.print((TBSum - TASum)+",^2:"+(Math.pow(TBSum - TASum,2))+",#: "+me.hashTable[i].Counter+"\n");
						squaredDiff+=Math.pow(TBSum - TASum,2);	
						
			//	}
				
			}//end a round
			
			if(S>0){
				v0=squaredDiff/S - Math.pow(avg,2);;
			oneRound+=Math.abs(v0);
			////System.out.print(v0+" ");
			count++;
			}else{
				
			//System.err.println("no suitable numbers");	
			}
			//next round
			start=end;
		}
		
		double v=count>0?oneRound/count:-1;
		//////System.out.print( "avg: "+v+"\n");
		
		intervalHashs.clear();
		intervalHashs=null;
		me.clear();
		you.clear();
		me=null;
		you=null;
		
		return v;
//		for(int i=0;i<me.hashTable.length;i++){
//			////////System.out.println("keySum: "+me.hashTable[i].keySum+", "
//		//+you.hashTable[i].keySum);
//			if(me.hashTable[i].Counter!=0&&
//					me.hashTable[i].keySum==you.hashTable[i].keySum&&
//					me.hashTable[i].Counter==you.hashTable[i].Counter){
//					S+=me.hashTable[i].Counter;	
//			}
//		}
//		//////System.out.println("S: "+S);		
//		/**
//		 * 
//						me.hashTable[i].keyCheck==you.hashTable[i].keyCheck&&
//						
//		 */
//		double v;
//		for(int i=0;i<me.hashTable.length;i++){
//			
//			if(you.hashTable[i].empty()||me.hashTable[i].empty()||me.hashTable[i].Counter==0){
//				System.err.println("empty");
//				continue;
//			}
//			if(me.hashTable[i].keySum!=you.hashTable[i].keySum||
//					me.hashTable[i].Counter!=you.hashTable[i].Counter){
//				System.err.println("unequal! ");
//				continue;
//			}
//			//only choose good pair
//				//if(me.hashTable[i].keySum==you.hashTable[i].keySum&&
//				//		me.hashTable[i].Counter==you.hashTable[i].Counter){					
//					TASum=me.hashTable[i].TS;
//					TBSum=you.hashTable[i].TS;
//					v=TBSum - TASum;
//					//////System.out.print(v+",#: "+me.hashTable[i].Counter+"\n");
//					squaredDiff+=Math.pow(v,2);		
//					//////System.out.print("$: "+squaredDiff+"\n");	
//				
//			//}//find the usable column
//		}
//		////System.out.print("squaredDiff: "+squaredDiff+",S: "+S+",ratio: "+(squaredDiff/(S))+"\n");	
//		return squaredDiff/(S);//squaredDiff/(N_HASH*S);
	}
	
	
//	public double getFScoreByCollapseUseSignedTS(RDATable other11){
//		
//		//do not collapse
//		RDATable me = this;//this.collapseAdjacentBank();
//		RDATable you = other11;//other11.collapseAdjacentBank();
//						
//		double TASum=0;
//		double TBSum=0;
//		double squaredDiff=0;
//		int S=0;
//		
//		for(int i=0;i<me.hashTable.length;i++){
//			////System.out.println("keySum: "+me.hashTable[i].keySum+", "+you.hashTable[i].keySum);
//			if(me.hashTable[i].Counter!=0&&
//					me.hashTable[i].keySum==you.hashTable[i].keySum&&
//					me.hashTable[i].Counter==you.hashTable[i].Counter){
//					S+=me.hashTable[i].Counter;	
//			}
//		}
//		////System.out.println("S: "+S);		
//		/**
//		 * 
//						me.hashTable[i].keyCheck==you.hashTable[i].keyCheck&&
//						
//		 */
//		double v;
//		for(int i=0;i<me.hashTable.length;i++){
//			
//			if(you.hashTable[i].empty()||me.hashTable[i].empty()||me.hashTable[i].Counter==0){
//				System.err.println("empty");
//				continue;
//			}
//			if(me.hashTable[i].keySum!=you.hashTable[i].keySum||
//					me.hashTable[i].Counter!=you.hashTable[i].Counter){
//				System.err.println("unequal! ");
//				continue;
//			}
//			//only choose good pair
//				if(me.hashTable[i].keySum==you.hashTable[i].keySum&&
//						me.hashTable[i].Counter==you.hashTable[i].Counter){					
//					TASum=me.hashTable[i].STS;
//					TBSum=you.hashTable[i].STS;
//					v=TBSum - TASum;
//					//////System.out.print(v+",#: "+me.hashTable[i].Counter+"\n");
//					squaredDiff+=Math.pow(v,2);		
//					//////System.out.print("$: "+squaredDiff+"\n");	
//				
//			}//find the usable column
//		}
//		////System.out.print("squaredDiff: "+squaredDiff+",S: "+S+",ratio: "+(squaredDiff/(S))+"\n");	
//		return squaredDiff/(S);//squaredDiff/(N_HASH*S);
//	}
	
	
	/**
	 * dump the table
	 * @return
	 */
   public String dumpTable(){
	   StringBuffer sb = new StringBuffer();
	   sb.append("count keySum keyCheckMatch\n");
	   for (int i=0;i<hashTable.length;i++){
		   RDATableEntry entry = hashTable[i];
		   sb.append("# items: "+entry.getNumItems()+" "+entry.keySum + " ");
		   
		   byte[] kvec = IBLTEntry.ToByteArray(entry.keySum);
		   //int[] hh = HashTableEntry.hashC.hash(new Key( kvec),HashTableEntry.N_HASHCHECK) ;
		   		   
		  // sb.append(FineComb.sHash64.hash(ByteBuffer.allocate(8).putLong(entry.keySum).array())==entry.keyCheck? "true" : "false");
		   sb.append("\n");
		
	   }
	   return sb.toString();
   }

   /**
    * remove all entries with empty buckets
    * @return
    */
//	public void squeeze() {
//		// TODO Auto-generated method stub
//		Iterator<RDATableEntry> ier = this.hashTable.iterator();
//		while(ier.hasNext()){
//			RDATableEntry t = ier.next();
//			if(t.empty()){
//				//////System.out.println("remove: "+t.toString());
//				ier.remove();
//			}
//		}
//	}

	public void resetN_HASH() {
		// TODO Auto-generated method stub
		N_HASH=2;
	}

	public void setRDAHash(int NumHash){
		N_HASH = NumHash;
	}
	
	public void clear() {
		// TODO Auto-generated method stub
		try {
			BFOps.acquire();
			for(int i=0;i<hashTable.length;i++){
				hashTable[i].reset();
			}
			BFOps.release();
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//this.hashTable=null;
		//this.hashTable=null;
	}

	/**
	 * good packets, averaged by the number of hash functions
	 * @param RReceiver
	 * @return
	 */
	public double getGoodPackets(RDATable RReceiver) {
		// TODO Auto-generated method stub
		double good=0;
		
		RDATable subtractTBF = this.subtractIBLT(RReceiver);		
		int c=0;
		double sum=0;
		int size=subtractTBF.hashTable.length;
		//////System.out.println("$size: "+size+", receiverSize: "+RReceiver.hashTable.length);
		for(int i=0;i<size;i++){
			//useful
			if(!RReceiver.hashTable[i].empty()&&!this.hashTable[i].empty()&&
			this.hashTable[i].keyCheck==RReceiver.hashTable[i].keyCheck&&
					this.hashTable[i].keySum==RReceiver.hashTable[i].keySum){
			//use only buckets that have insertions
				
				good+= Math.abs(RReceiver.hashTable[i].getNumItems());
			//////System.out.println("$size:"+t);
		}
			
	}
		//averaged by the number of hash functions
		return good/N_HASH;
}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}



}