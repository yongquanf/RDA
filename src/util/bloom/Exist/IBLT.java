package util.bloom.Exist;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Vector;

import util.bloom.Apache.Key;
import util.bloom.Apache.Hash.hashing.LongHashFunction;

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

public class IBLT {

	public int N_HASH = 4;
	
	public LongHashFunction[] LongHashFunction4PosHash;
	
	public int valueSize;
	public Vector<IBLTEntry> hashTable;
	
	class Pair<T1,T2>{
		T1 id;
		T2 byteArray;
		public Pair(T1 a,T2 b){
			id=a;
			this.byteArray=b;
		}
	}
	
	public IBLT(int _expectedNumEntries, int _ValueSize){
		this.valueSize=_ValueSize;
	    // 1.5x expectedNumEntries gives very low probability of
	    // decoding failure
		int nEntries=_expectedNumEntries + _expectedNumEntries/2;
	    while (N_HASH * (nEntries/N_HASH) != nEntries) ++nEntries;
	    hashTable = new Vector<IBLTEntry>(nEntries);
		for(int i=0;i<nEntries;i++){
			hashTable.add(new IBLTEntry());
		}
		
		for(int i=0;i<N_HASH;i++){
			LongHashFunction4PosHash[i]=LongHashFunction.xx(i);
		}
	}
	
	/**
	 * 
	 * @param id
	 * @param index
	 * @return
	 */
	public int hashPos(long id,int index){
		long longHash=LongHashFunction4PosHash[index].hashLong(id);//,N_HASHCHECK);
		return (int) Math.abs(longHash%Integer.MAX_VALUE);
	}
	
	
	/**
	 * interface to insert an item
	 * @param k
	 * @param v
	 */
	public void insert(long k,byte[]v){
		_insert(1,k,v);
		
	}
	 void _insert(int plusOrMinus,long k,byte[] v){
		assert(v.length ==valueSize);
		//byte[] kVec=IBLTEntry.ToByteArray(k);
		int bucketsPerHash = hashTable.size()/N_HASH;
		for(int i=0;i<N_HASH;i++){
			int startEntry = i*bucketsPerHash;
			//position
			int h=hashPos(k,i);
			IBLTEntry entry = hashTable.get(startEntry+(h%bucketsPerHash));
			entry.count+=plusOrMinus;
			entry.keySum ^= k;
			//h=IBLTEntry.hashC.hash(new Key(kVec),IBLTEntry.N_HASHCHECK);
			entry.keyCheck ^=IBLTEntry.LongHashFunction4PKeyHash.hashLong(k);
			//if(entry.empty()){
			//	entry.valueSum=ByteBuffer.allocate(entry.valueArrayLen).putLong(0).array();
			//}else{
			entry.addValue(v);
			//}
		}
		
	}
	
	public void erease(long k,byte[] v){
		_insert(-1,k,v);
		
	}
	
    // Returns true if a result is definitely found or not
    // found. If not found, result will be empty.
    // Returns false if overloaded and we don't know whether or
    // not k is in the table.
	public byte[] get(long k){
		 byte[] v=null;
	    byte[] kVec = IBLTEntry.ToByteArray(k);
	    int bucketsPerHash = hashTable.size()/N_HASH;
	    for(int i=0;i<N_HASH;i++){
	    	int startEntry = i*bucketsPerHash;
	    	//int[]h = IBLTEntry.hashC.hash(new Key(kVec),i);
	    	int h=hashPos(k,i);
	    	IBLTEntry entry = hashTable.get(startEntry+(h%bucketsPerHash));
	    	if(entry.empty()){
	    		 // Definitely not in table. Leave
	            // result empty, return true.
	    		//System.out.println("empty");
	    		return null;
	    	}else if(entry.isPure()){
	    		if(entry.keySum==k){
	    			 // Found!
	    			byte[] vv = entry.valueSum;
	    			v=new byte[vv.length];
	    			
	    			for(int vi=0;vi<v.length;vi++){
	    				v[vi]=vv[vi];
	    			}
	    			//System.out.println("found!");
	    			return v;
	    		}else{
		    		// Definitely not in table.
		    		//System.out.println("not in");
		    		return null;
		    	}
	    	}	    		        
	    }
	    
	 // Don't know if k is in table or not; "peel" the IBLT to try to find
        // it:
	    IBLT peeled = this;
	    int nErased = 0;
	    for(int i=0;i<peeled.hashTable.size();i++){
	    	IBLTEntry entry = peeled.hashTable.get(i);
	    	if(entry.isPure()){
	    		if(entry.keySum==k){
	    			//found!
	    			byte[] vv = entry.valueSum;
	    			v=new byte[vv.length];
	    			for(int vi=0;vi<v.length;vi++){
	    				v[vi]=vv[vi];
	    			}
	    			//System.out.println("Found");
	    			return v;
	    		}
	    		nErased++;
	    		peeled._insert(-entry.count,entry.keySum,entry.valueSum);
	    	}
	    }
	    if (nErased > 0) {
	        // Recurse with smaller IBLT
	        return peeled.get(k);
	    }
	    //System.out.println("not in after peel");
	    return null;
	}
	
    // Adds entries to the given sets:
    //  positive is all entries that were inserted
    //  negative is all entreis that were erased but never added (or
    //   if the IBLT = A-B, all entries in B that are not in A)
    // Returns true if all entries could be decoded, false otherwise.
	public boolean listEntries(HashSet<Pair<Long,byte[]>> positive,HashSet<Pair<Long,byte[]>> negative){
		
		  IBLT peeled = this;
		  int nErased = 0;
		  while(nErased > 0){
			  nErased = 0;
			  for(int i=0;i<peeled.hashTable.size();i++){
				  IBLTEntry entry = peeled.hashTable.get(i);
				  if(entry.isPure()){
					  if(entry.count==1){
						  positive.add(new Pair<Long,byte[]>(entry.keySum,entry.valueSum));
					  }else{
						  negative.add(new Pair<Long,byte[]>(entry.keySum,entry.valueSum));
					  }
					  
					nErased++;
			    	peeled._insert(-entry.count,entry.keySum,entry.valueSum);

				  }
				  
			  }
			  
		  }//end
		    // If any buckets for one of the hash functions is not empty,
		    // then we didn't peel them all:
		    for (int i = 0; i < peeled.hashTable.size()/N_HASH; i++) {
		        if (peeled.hashTable.get(i).empty() != true) return false;
		    }
		    return true; 
	}


	 // Subtract two IBLTs
	public IBLT subtractIBLT(IBLT other){
	    // IBLT's must be same params/size:
	    assert(valueSize == other.valueSize);
	    assert(hashTable.size() == other.hashTable.size());
		
	    IBLT result = this;

	    for (int i = 0; i < hashTable.size(); i++) {
	        IBLTEntry e1 = result.hashTable.get(i);
	        IBLTEntry e2 = other.hashTable.get(i);
	        e1.count -= e2.count;
	        e1.keySum ^= e2.keySum;
	        e1.keyCheck ^= e2.keyCheck;
//	        if (e1.empty()) {
//	            e1.valueSum=ByteBuffer.allocate(e1.valueArrayLen).putLong(0).array();
//	        }
//	        else {
	            e1.addValue(e2.valueSum);
	       // }
	    }

	    return result;
	}

	/**
	 * dump the table
	 * @return
	 */
   public String dumpTable(){
	   StringBuffer sb = new StringBuffer();
	   sb.append("count keySum keyCheckMatch\n");
	   for (int i=0;i<hashTable.size();i++){
		   IBLTEntry entry = hashTable.get(i);
		   sb.append(entry.count+" "+entry.keySum + " ");
		   
		   //byte[] kvec = IBLTEntry.ToByteArray(entry.keySum);
		   
		   //int[] hh = IBLTEntry.hashC.hash(new Key( kvec),IBLTEntry.N_HASHCHECK) ;
		   		   
		   sb.append(IBLTEntry.LongHashFunction4PKeyHash.hashLong(entry.keySum)==entry.keyCheck? "true" : "false");
		   sb.append("\n");
		
	   }
	   return sb.toString();
   }

}
