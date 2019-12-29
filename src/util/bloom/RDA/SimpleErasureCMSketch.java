package util.bloom.RDA;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

import edu.harvard.syrah.prp.Log;
import util.async.MathUtil;
import util.async.Writable;
import util.bloom.Apache.Key;
import util.bloom.Apache.Hash.hashing.LongHashFunction;
import util.bloom.Exist.FineComb;
import util.bloom.Exist.IBLTEntry;

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


public class SimpleErasureCMSketch  implements Writable ,Serializable{

	static Log log=new Log(SimpleErasureCMSketch.class);

	
	//number of hash functions
	 public volatile static int N_HASH = 2;
	 

		
		//keycheck
		public static int seed2 = 54321;
		public static LongHashFunction LongHashFunction4PKeyHash=LongHashFunction.xx(seed2);
		
	 
	 
	//scale the bucket size
	public volatile static double scaleBucketFactor =1;
	
	public volatile int valueSize=0;
	public SimpleErasureCMSketchEntry[] hashTable = null;
	
	//hash function
	public  static LongHashFunction[] LongHashFunction4PosHash;
	//sign calculate
	public  static LongHashFunction[] LongHashFunction4PosHashSign;
	
	//create a lock on the data
	public Semaphore BFOps=null;
	
	volatile int expectedNumEntries=0;
	
	public static  int seedSign=128721;
	static Random RandSign=null;
	
	public double sampleProbability=1;
	public int requiredLeadingZeros=0;
	
	public static Random getSingleton(){
		if(RandSign==null){
			RandSign=new Random(seedSign);
			return RandSign;
		}else{
			return RandSign;
		}
		
	}

	//stash
	public Hashtable<Long,Double> stashCache=new Hashtable<Long,Double>();
    //size
	public int stashSize = 100;

	/**
	 * set the stash size
	 * @param size
     */
	public void setStashSet(int size){
		this.stashSize = size;
	}

	/**
	 * reset
	 */
	public void resetStashCache(){
		this.stashCache.clear();
	}

	/**
	 *
	 * @param id
	 * @param value
     */
	public boolean insert2Stash(long id,double value){
		if(stashCache.size()<this.stashSize){
			stashCache.put(id,value);
			return true;
		}else{
			return false;
		}
	}

	/**
	 * map to a stash
	 * @param kvSets
	 * @return
     */
	public boolean insertKeyValueSet2Stash(Hashtable<Long,Double> kvSets){

		Iterator<Map.Entry<Long,Double>> ier = kvSets.entrySet().iterator();
		boolean rec=true;
		while(ier.hasNext()){
			Map.Entry<Long,Double> tmp = ier.next();
			rec = insert2Stash(tmp.getKey(),tmp.getValue());

		}
	 return rec;

	}
	
	class Pair<T1,T2>{
		T1 id;
		T2 byteArray;
		public Pair(T1 a,T2 b){
			id=a;
			this.byteArray=b;
		}
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
	 * sign predict
	 * @param id
	 * @param index
	 * @return
	 */
	public static int hashSign(long id,int index){
		long longHash=LongHashFunction4PosHash[index].hashLong(id);//,N_HASHCHECK);
		int signValue = (int)longHash%2;
		if (signValue==0){
			return -1;
		}else{
			return 1;
		}
	}
	
	public SimpleErasureCMSketch(){
		hashTable = null;
		sampleProbability=1;
		//semaphore
		BFOps =  new Semaphore(1);
		
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHash[i] = LongHashFunction.xx(i);
		}
		
	}
	
	/**
	 * keySum=I[index]
	 * h[keySum] == index, return true
	 * @param index
	 * @return
	 */
	
	public boolean EntryIsPure(int index){
		SimpleErasureCMSketchEntry entry = hashTable[index];
		//fail cases
		if(entry ==null||entry.empty()){
			return false;
		}
 
		
		long keySum = entry.keySum;		
		int bucketsPerHash = hashTable.length/N_HASH;
		
		int offsetInBank = index%bucketsPerHash;
		int indexPortion = (index - offsetInBank)/bucketsPerHash;
		//int indexPortion = Math.floorDiv(index, bucketsPerHash);
		//int offset = index - indexPortion*bucketsPerHash;
		
		int startEntry = indexPortion*bucketsPerHash;
		
		//byte[] kVec=IBLTEntry.ToByteArray(keySum);
		
		int h = hashPos(keySum,indexPortion);
		
		int offset = (h%bucketsPerHash);
		//int derivedIndex = offset;
		
		//log.main(index+", "+bucketsPerHash+", "+startEntry+", "+offset+", "+derivedIndex);

		boolean idHashSameBucket = (startEntry+offset) == index;

		boolean valueFieldIsZero=(Math.abs(entry.normalValueSum)-Math.abs(entry.signedValueSum))==0;

		if(idHashSameBucket&&valueFieldIsZero){
			//log.main("decode true: "+entry.toString());
			return true;
		}else{
			return false;
		}
			
	}
	
	public static boolean EntryIsPure(SimpleErasureCMSketch peeled,int index){
		SimpleErasureCMSketchEntry entry = peeled.hashTable[index];
		//fail cases
		if(entry ==null||entry.empty()){
			return false;
		}
		
		
		long keySum = entry.keySum;		
		int bucketsPerHash = peeled.hashTable.length/peeled.N_HASH;
		
		int offsetInBank = index%bucketsPerHash;
		int indexPortion = (index - offsetInBank)/bucketsPerHash;
		//int indexPortion = Math.floorDiv(index, bucketsPerHash);
		//int offset = index - indexPortion*bucketsPerHash;
		
		//int startEntry = indexPortion*bucketsPerHash;
		
		//byte[] kVec=IBLTEntry.ToByteArray(keySum);
		
		
		///int[] h=IBLTEntry.hashC.hash(new Key(kVec),indexPortion);
		int h = hashPos(keySum,indexPortion);
		
		int offset = (h%bucketsPerHash);
		//int derivedIndex = offset;


		boolean idHashSameBucket =offset == offsetInBank;

		boolean valueFieldIsZero=(Math.abs(entry.normalValueSum)-Math.abs(entry.signedValueSum))==0;



		//log.main(index+", "+bucketsPerHash+", "+startEntry+", "+offset+", "+derivedIndex);
		if(idHashSameBucket&&valueFieldIsZero){
			//log.main("decode true: "+entry.toString());
			return true;
		}else{
			return false;
		}
			
	}
	
	/**
	 * 
	 * @param _expectedNumEntries
	 */
	public SimpleErasureCMSketch(int _expectedNumEntries){

		BFOps = new Semaphore(1);
		 
		int _ValueSize = 8;
		this.valueSize=_ValueSize;
		expectedNumEntries = _expectedNumEntries;
	    // 1.5x expectedNumEntries gives very low probability of
	    // decoding failure
		//int nEntries=_expectedNumEntries + _expectedNumEntries/2;
	   // while (N_HASH * (nEntries/N_HASH) != nEntries) ++nEntries;
	    
	    //nEntries= Math.max(_expectedNumEntries, _expectedNumEntries/2)
	    
	    hashTable = new SimpleErasureCMSketchEntry[expectedNumEntries];
		for(int i=0;i<expectedNumEntries;i++){
			hashTable[i]=new SimpleErasureCMSketchEntry();//.setElementAt(, i);
		}
		sampleProbability=1;
		
		long seed1=124546;
		
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHash[i] = LongHashFunction.xx(seed1+i);
		}
		long seed2 = 4552235;
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHashSign[i] = LongHashFunction.xx(seed2+i);
		}
		
		////System.out.println("expectedNumEntries: "+expectedNumEntries+",HT Size: "+hashTable.length);
	}


    /**
	 * constructor
	 * @param _expectedNumEntries
	 * @param NumEntries
	 * @param valueSize
     */
	public SimpleErasureCMSketch(int _expectedNumEntries,int NumEntries,int valueSize){
		
		BFOps = new Semaphore(1);
		
		this.valueSize=valueSize;
		this.expectedNumEntries = _expectedNumEntries;
		
		hashTable = new SimpleErasureCMSketchEntry[NumEntries];
		for(int i=0;i<NumEntries;i++){
			hashTable[i]=new SimpleErasureCMSketchEntry();//.setElementAt(, i);
		}
		
		////System.out.println("expectedNumEntries: "+expectedNumEntries+",HT Size: "+hashTable.length);
		sampleProbability=1;
		long seed1=124546;
		
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHash[i] = LongHashFunction.xx(seed1+i);
		}
		long seed2 = 4552235;
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHashSign[i] = LongHashFunction.xx(seed2+i);
		}
		//////System.out.println("HT Size: "+hashTable.length);
	}


	/**
	 * constructor
	 * @param _expectedNumEntries
	 * @param _sampleProbability
     */
	public SimpleErasureCMSketch(int _expectedNumEntries,double _sampleProbability){

		BFOps = new Semaphore(1);
		
		int _ValueSize = 8;
		this.valueSize=_ValueSize;
		expectedNumEntries = _expectedNumEntries;
	    // 1.5x expectedNumEntries gives very low probability of
	    // decoding failure
		//int nEntries=_expectedNumEntries + _expectedNumEntries/2;
	   // while (N_HASH * (nEntries/N_HASH) != nEntries) ++nEntries;
	    
	    //nEntries= Math.max(_expectedNumEntries, _expectedNumEntries/2)
	    
		hashTable = new SimpleErasureCMSketchEntry[expectedNumEntries];
		for(int i=0;i<expectedNumEntries;i++){
			hashTable[i]=new SimpleErasureCMSketchEntry();//.setElementAt(, i);
		}
		
		 
		long seed1=124546;
		
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHash[i] = LongHashFunction.xx(seed1+i);
		}
		long seed2 = 4552235;
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHashSign[i] = LongHashFunction.xx(seed2+i);
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
	public SimpleErasureCMSketch(int _expectedNumEntries, int _ValueSize){
		
		BFOps = new Semaphore(1);
		
		this.valueSize=_ValueSize;
	    // 1.5x expectedNumEntries gives very low probability of
	    // decoding failure
		//int nEntries=_expectedNumEntries + _expectedNumEntries/2;
	    //while (N_HASH * (nEntries/N_HASH) != nEntries) ++nEntries;
		this.expectedNumEntries = _expectedNumEntries;
		
		hashTable = new SimpleErasureCMSketchEntry[expectedNumEntries];
		for(int i=0;i<expectedNumEntries;i++){
			hashTable[i]=new SimpleErasureCMSketchEntry();//.setElementAt(, i);
		}
		
		long seed1=124546;
		
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHash[i] = LongHashFunction.xx(seed1+i);
		}
		long seed2 = 4552235;
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHashSign[i] = LongHashFunction.xx(seed2+i);
		}
		
		////System.out.println("expectedNumEntries: "+expectedNumEntries+",HT Size: "+hashTable.length);

		//////System.out.println("HT Size: "+hashTable.length);
	}
	
	/**
	 * copy a table
	 * @return
	 */
	public SimpleErasureCMSketch copyTable(){
		SimpleErasureCMSketch a = new SimpleErasureCMSketch();
		a.hashTable = new SimpleErasureCMSketchEntry[hashTable.length];
		a.valueSize=this.valueSize;
		a.expectedNumEntries=this.expectedNumEntries;
		int count=0;
		while(count<hashTable.length){
			a.hashTable[count] = hashTable[count].clone();
			count++;
		}
		return a;
		
	}
	
	/**
	 * get a RDA from the array
	 * @param you
	 * @return
	 */
	public static SimpleErasureCMSketch getRDATable(SimpleErasureCMSketchEntry[] you){
		SimpleErasureCMSketch a = new SimpleErasureCMSketch();
		a.hashTable = new SimpleErasureCMSketchEntry[you.length];

		a.expectedNumEntries=you.length;
		int count=0;
		while(count<you.length){
			SimpleErasureCMSketchEntry aa = you[count].clone();
			a.hashTable[count] = aa;
			count++;
		}
		return a;
	}
	
	/**
	 * insert items
	 * @param id
	 * @param TS
	 */
	public void insert(long id,double TS){
		_insert(id,TS);
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

	
	public boolean InsertSample(long id,double TS){
	int index=hash2Cell(id);
	if(index>=requiredLeadingZeros){
		_insert(id,TS);
		//System.out.println("inserted!");
		return true;
	}else{
		return false;
	}
	}
	
	/**
	 * interface to insert an item
	 * @param id
	 * @param TS
	 */
//	public void insert(long id,double TS,long Counter){
//		_insert(id,TS,Counter);
//		
//	}
	 void _insert(long id,double TS){
		
		//int sign;
		//assert(v.length ==valueSize);
		
		
		//request permission
		try {
			BFOps.acquire();
			
			 int index;
				//byte[] kVec=IBLTEntry.ToByteArray(id);
				int bucketsPerHash = hashTable.length/N_HASH;
			for(int i=0;i<N_HASH;i++){
				int startEntry = i*bucketsPerHash;
				
				//int[] h=IBLTEntry.hashC.hash(new Key(kVec),i);
				int h = hashPos(id, i);
								
				index=startEntry+(h%bucketsPerHash);
				SimpleErasureCMSketchEntry entry = hashTable[index];
				//sign value of the identifier
				int signValue = hashSign(id,i);

				//count min and count sketch,
				entry.normalValueSum+=TS;
				entry.signedValueSum+=signValue*TS;
				
				//hash xor
				entry.keySum ^= id;

				hashTable[index]=entry;

			}
			
			//kVec=null;
			//release
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
	 * @return 
	  */
		public boolean eraseSample(Long id, double ts) {
			// TODO Auto-generated method stub
			int index=hash2Cell(id);
			if(index>=requiredLeadingZeros){
				 erase(id,ts);
				//System.out.println("inserted!");
				 return true;
				
			}else{
				return false;
			}
		}
	 /**
	  * erase items
	  * @param id
	  * @param TS
	  */
	 public void erase(long id, double TS){
		// _e(id,TS,Counter);		
		 _insert(id,-TS);
	 }
	 

	/**
	 * decode ids
	 * @param decodedItems
	 * @return
	 */
	public boolean decodeIDs(HashSet<Long> decodedItems){
		
		  //SimpleRDATable peeled = this.copyTable();
		 // int nErased = 0;
		  //////System.out.println("peeled: "+peeled.hashTable.length);
		  HashSet<Integer> tobeRemoved = new HashSet<Integer>();
		  HashSet<Integer> leftIDs = new HashSet<Integer>();
		  for(int i=0;i<hashTable.length;i++){
			  leftIDs.add(i);
		  }
		  while(true){
			  //nErased = 0;
			  //current ids
			  tobeRemoved.clear();
			  Iterator<Integer> ier = leftIDs.iterator();
			  while(ier.hasNext()){
				  //id
				  int i = ier.next();
				  //check
				  SimpleErasureCMSketchEntry entry = hashTable[i];
					 boolean state = EntryIsPure(i);
					 	//////System.out.println("$Decode: "+entry.toString());
					  if(state&&(leftIDs.contains(i))){
						  tobeRemoved.add(i);					  
						  //log.main("find pured!");
						  //////System.out.print(" pure!!!\n");

						  decodedItems.add(entry.keySum);

						  //change id
				    	_insert(entry.keySum,-entry.normalValueSum);
					  }else if(state&&(!tobeRemoved.contains(i))){
						  log.warn("repeat removing: "+i+", "+entry.keySum);
					  }
			  }
			  //delete now
			  if(!tobeRemoved.isEmpty()){
				  leftIDs.removeAll(tobeRemoved);
			  }else{
				  break;
			  }
		  }//end
		  
		  //////System.out.println("IDs: "+positive.size()+", you: "+negative.size());
		    // If any buckets for one of the hash functions is not empty,
		    // then we didn't peel them all:
		    //for (int i = 0; i < hashTable.length; i++) {
		    	
		        if (!leftIDs.isEmpty()){
		        	////System.out.println("peel: "+peeled.hashTable[i].toString());
		        	leftIDs.clear();
		        	return false;
		        	}
		    //}
		    return true; 
	}
	
	/**
	 * decode all ids
	 * @return
	 */
	public Hashtable<Long,Double> decodeAllIDs(){


		/**
		 * remove stash first
		 */
		if(!stashCache.isEmpty()){
			Iterator<Map.Entry<Long,Double>> ier = stashCache.entrySet().iterator();
			while(ier.hasNext()){
				Map.Entry<Long,Double> tmp = ier.next();
				//erase from the table
				erase(tmp.getKey(),tmp.getValue());
			}
		}

		/**
		 * then try to decode
		 */
		Hashtable<Long,Double> ids = new Hashtable<Long,Double>();
		 SimpleErasureCMSketch peeled = this.copyTable();
		  int nErased = 0;
		  //////System.out.println("peeled: "+peeled.hashTable.length);
		  //int totalCount=0;
		  while(true){
			  nErased = 0;
			  for(int i=0;i<peeled.hashTable.length;i++){
				 SimpleErasureCMSketchEntry entry = peeled.hashTable[i];
				 	//////System.out.println("$Decode: "+entry.toString());
				  if(EntryIsPure(peeled,i)){
					  //////System.out.print(" pure!!!\n");
					// totalCount++;
					ids.put(entry.keySum,(double)entry.normalValueSum);
					//delete from the table
					nErased++;
			    	peeled._insert(entry.keySum,-entry.normalValueSum);

				  }
				  
			  }
			  
			  if(nErased<=0){
				  break;
			  }
		  }//end
		  
		  return ids;
		  //return totalCount;
	}
	/**
	 * decode ids
	 * @return
	 */
	
	public int decodeIDs(){
		
		  SimpleErasureCMSketch peeled = this.copyTable();
		  //////System.out.println("peeled: "+peeled.hashTable.length);
		  Queue<Integer> indexPureBuckets = new  LinkedList<Integer>();
		  for(int i=0;i<peeled.hashTable.length;i++){
			  //SimpleErasureCMSketchEntry entry = peeled.hashTable[i];
			 	//////System.out.println("$Decode: "+entry.toString());
			  if(EntryIsPure(peeled,i)){
				  indexPureBuckets.add(i);
			  }//add index
			  }
		  
		  int totalCount=0;
		  while(!indexPureBuckets.isEmpty()){
			  Integer index = indexPureBuckets.remove();
			  SimpleErasureCMSketchEntry entry = peeled.hashTable[index];
			  totalCount++;			
				peeled._insert(entry.keySum,-entry.normalValueSum);
				//get the indexes
				Vector<Integer> newPositions = getHashLocation(entry.keySum,index);
				if(!newPositions.isEmpty()){
					indexPureBuckets.addAll(newPositions);
				}
		  }//iterate the buckets
		  
		  return totalCount;
	}
	
	
	public long[] decodeIDsTime(){
		
		  SimpleErasureCMSketch peeled = this.copyTable();
		  //////System.out.println("peeled: "+peeled.hashTable.length);
		  Queue<Integer> indexPureBuckets = new  LinkedList<Integer>();
		  		  
		  long time1=System.nanoTime();
		  for(int i=0;i<peeled.hashTable.length;i++){
			  //SimpleErasureCMSketchEntry entry = peeled.hashTable[i];
			 	//////System.out.println("$Decode: "+entry.toString());
			  if(EntryIsPure(peeled,i) ){//entry.isPure()
				  indexPureBuckets.add(i);
			  }//add index
			  }
		  
		  int totalCount=0;
		  while(!indexPureBuckets.isEmpty()){
			  Integer index = indexPureBuckets.remove();
			  SimpleErasureCMSketchEntry entry = peeled.hashTable[index];
			  totalCount++;			
				peeled._insert(entry.keySum,-entry.normalValueSum);
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
				int h = hashPos(id,i);
				
				index=startEntry+(h%bucketsPerHash);
				SimpleErasureCMSketchEntry entry;
				if(index == removeIndex){
					continue;
				}else
				entry= hashTable[index];
				if(entry.empty()){//entry.empty()
					continue;
				}
				if(EntryIsPure(this,index)){
					indexes.add(index);
				}
				
			}
			return indexes;
	}

	public int decodeIDs00(){
		
		  SimpleErasureCMSketch peeled = this.copyTable();
		  int nErased = 0;
		  //////System.out.println("peeled: "+peeled.hashTable.length);
		  int totalCount=0;
		  while(true){
			  nErased = 0;
			  for(int i=0;i<peeled.hashTable.length;i++){
				 SimpleErasureCMSketchEntry entry = peeled.hashTable[i];
				 	//////System.out.println("$Decode: "+entry.toString());
				  if(EntryIsPure(peeled,i)){//entry.isPure()
					  //////System.out.print(" pure!!!\n");
					 totalCount++;
					nErased++;
			    	peeled._insert(entry.keySum,-entry.normalValueSum);

				  }
				  
			  }
			  
			  if(nErased<=0){
				  break;
			  }
		  }//end
		  
		  return totalCount;
	}

	/**
	 * subtract
	 * @param other
	 * @return
     */
	public SimpleErasureCMSketch subtractIBLT(SimpleErasureCMSketch other){
	    // IBLT's must be same params/size:
	    //assert(valueSize == other.valueSize);
	    //assert(hashTable.length == other.hashTable.length);
		
	    SimpleErasureCMSketch result = this.copyTable();
	    int size=result.hashTable.length;
	    ////System.out.println("$subtractSize: "+size);
	    for (int i = 0; i < size; i++) {
	        SimpleErasureCMSketchEntry e1 = result.hashTable[i];
	        SimpleErasureCMSketchEntry e2 = other.hashTable[i];
	        //e1.Counter-=e2.Counter;
	       // e1.TS -= e2.TS;
	        //e1.STS-=e2.STS;
	        //e1.keySum ^= e2.keySum;
	        //e1.keyCheck ^= e2.keyCheck;
	        
	        SimpleErasureCMSketchEntry newItem = e1.XORHashTable(e2);
	        
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
	 * merge two
	 * @param other
	 * @return
     */
	public SimpleErasureCMSketch mergeIBLT(SimpleErasureCMSketch other){
		// IBLT's must be same params/size:
		//assert(valueSize == other.valueSize);
		//assert(hashTable.length == other.hashTable.length);

		SimpleErasureCMSketch result = this.copyTable();
		int size=result.hashTable.length;
		////System.out.println("$subtractSize: "+size);
		for (int i = 0; i < size; i++) {
			SimpleErasureCMSketchEntry e1 = result.hashTable[i];
			SimpleErasureCMSketchEntry e2 = other.hashTable[i];
			//e1.Counter-=e2.Counter;
			// e1.TS -= e2.TS;
			//e1.STS-=e2.STS;
			//e1.keySum ^= e2.keySum;
			//e1.keyCheck ^= e2.keyCheck;

			SimpleErasureCMSketchEntry newItem = e1.MergeHashTable(e2);

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
				erase(id,ts);
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
				erase(id,ts);
			}else{
				////System.out.println("empty");
			}
		}
	}
	
	/**
	 *
	 * @param ids
	 * @param TSTable
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
					erase(id,ts);
				}else{
					////System.out.println("id: "+id+" not available");
					continue;
				}
			}
		}
	}

	/**
	 * constant
	 */
	public static int minSketchChoice = 1;

	public static int medianSketchChoice = 2;

	/**
	 * portal for the query process, first exact, then approximate
	 * @param id
	 * @param choiceOfType, 1, min, 2, median
	 * @return
     */
	public double queryItemPortal(long id,int choiceOfType){


		//step1: decode;
		Hashtable<Long,Double> decodedKeyValues=decodeAllIDs();

	   //stash first
	   if(stashCache.containsKey(id)){
		   return stashCache.get(id);
	   }
       else if(decodedKeyValues.containsKey(id)){
		   return decodedKeyValues.get(id);
	   }else{
		   if(choiceOfType==minSketchChoice){
			   return getMinBucketByArray(id);
		   }else if(choiceOfType==medianSketchChoice){
			   return getMedianBucketByArray(id);
		   }else{
			   //not supported
			   return -1;
		   }

	   }

	}

	
	/**
	 * min query by the table
	 * @return
	 */
	public double getMinBucketByArray(long id) {

		int index;
		//byte[] kVec=IBLTEntry.ToByteArray(id);
		int bucketsPerHash = hashTable.length / N_HASH;

		/**
		 * cache the results
		 */
		ArrayList<Double> results = new ArrayList<Double>();

		for (int i = 0; i < N_HASH; i++) {
			int startEntry = i * bucketsPerHash;

			//int[] h=IBLTEntry.hashC.hash(new Key(kVec),i);
			int h = hashPos(id, i);

			index = startEntry + (h % bucketsPerHash);
			SimpleErasureCMSketchEntry entry = hashTable[index];

			results.add((double)entry.normalValueSum);

		}
		return Collections.min(results);

	}

	/**
	 * median query by the table
	 * @param id
	 * @return
     */
	public double getMedianBucketByArray(long id) {

		int index;
		//byte[] kVec=IBLTEntry.ToByteArray(id);
		int bucketsPerHash = hashTable.length / N_HASH;

		/**
		 * cache the results
		 */
		ArrayList<Double> results = new ArrayList<Double>(1);

		for (int i = 0; i < N_HASH; i++) {
			int startEntry = i * bucketsPerHash;

			//int[] h=IBLTEntry.hashC.hash(new Key(kVec),i);
			int h = hashPos(id, i);

			index = startEntry + (h % bucketsPerHash);
			SimpleErasureCMSketchEntry entry = hashTable[index];

			results.add((double)entry.signedValueSum*hashSign(id,i));

		}
		 Collections.sort(results);
		int medianPos = (int)Math.round(results.size()/2.0);
		return results.get(medianPos);

	}



		
	MathUtil math =new MathUtil(100);
	

	
	/**
	 * dump the table
	 * @return
	 */
   public String dumpTable(){
	   StringBuffer sb = new StringBuffer();
	   sb.append("count keySum keyCheckMatch\n");
	   for (int i=0;i<hashTable.length;i++){
		   SimpleErasureCMSketchEntry entry = hashTable[i];
		   sb.append("# items: "+entry.getNumItems()+" "+entry.keySum + " ");
		   
		   //byte[] kvec = IBLTEntry.ToByteArray(entry.keySum);
		   //int[] hh = HashTableEntry.hashC.hash(new Key( kvec),HashTableEntry.N_HASHCHECK) ;
		   		   
		   //sb.append(FineComb.sHash64.hash(ByteBuffer.allocate(8).putLong(entry.keySum).array())==entry.keyCheck? "true" : "false");
		   sb.append("\n");
		
	   }
	   return sb.toString();
   }



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
	}

	/**
	 * good packets, averaged by the number of hash functions
	 * @return
	 */
	public double getGoodBuckets() {
		// TODO Auto-generated method stub
		double good=0;
		
		SimpleErasureCMSketch subtractTBF = this;
		int c=0;
		double sum=0;
		int size=subtractTBF.hashTable.length;
		//////System.out.println("$size: "+size+", receiverSize: "+RReceiver.hashTable.length);
		for(int i=0;i<size;i++){
			//useful
			if(!this.hashTable[i].empty()&&EntryIsPure(i)){
			//use only buckets that have insertions
				
				good+= 1;
			//////System.out.println("$size:"+t);
		}
			
	}
		//averaged by the number of hash functions
		return good;
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