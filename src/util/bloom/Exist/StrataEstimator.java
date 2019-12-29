package util.bloom.Exist;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.ListUtils;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.POut;
import edu.harvard.syrah.prp.SortedList;

import util.bloom.Apache.HashFunction;
import util.bloom.Apache.Key;

public class StrataEstimator {

	static Log log =new Log(StrataEstimator.class);
	
	/**
	 * store the strata
	 */
	public Hashtable<Integer,InvertedBloomFilter> filters;
	/**
	 * hash function H_z
	 */
	public HashFunction hashC;
	/**
	 * configure
	 */
	int vectorSize;
	int nbHash;
	int hashType;

	/**
	 * constructor
	 * @param vectorSize
	 * @param hashType
	 */
	public StrataEstimator(int vectorSize_, int nbHash_,int hashType_){
		vectorSize = vectorSize_;
	    hashType = hashType_;
	    nbHash=nbHash_;
	    hashC = new HashFunction(vectorSize_, 1, hashType_,1);
	
	    filters = new Hashtable<Integer,InvertedBloomFilter>(2);
	}
	
	/**
	 * encode a list of nodes
	 * @param keys
	 */
	  public void encode(Collection<Key> keys){
		    if(keys == null) {
		      throw new IllegalArgumentException("Collection<Key> may not be null");
		    }
		    for(Key key: keys) {
		      encode(key);
		    }
		  }//end add()
	  
	/**
	 * encode the data
	 * @param y
	 */
	public void encode(Key key){
		
		int[] h2 = hashC.hash(key);
		
		int NumTrailingZeros=Integer.numberOfTrailingZeros(h2[0]);
		h2=null;
		
		log.debug("NumTrailingZeros="+NumTrailingZeros+" nhash: "+nbHash+
				" "+vectorSize);
		
		if(filters!=null){
			if(!filters.containsKey(NumTrailingZeros)){
				filters.put(NumTrailingZeros,new InvertedBloomFilter(
					vectorSize,nbHash,hashType) );
			}
			filters.get(NumTrailingZeros).add(key);
		}else{
			log.warn("null Strata");
		}
	}
	
	/**
	 * decode the strata, to find the estimated size of d
	 * @return
	 * @throws IOException 
	 */
	public int decode(Hashtable<Integer,InvertedBloomFilter> filters2,BufferedWriter bw) throws IOException{
		int count=0;
		//Set<Integer> allInts=new HashSet<Integer>(2);
		//allInts.addAll(filters.keySet());
		//allInts.addAll(filters2.keySet());
		
		List<Integer> sList=ListUtils.union(new ArrayList(filters.keySet()), new ArrayList(filters2.keySet()));
		//new ArrayList<Integer>(allInts.size());
		//sList.addAll(allInts);
		//allInts.clear();
		//allInts=null;
		Collections.sort(sList);
		
		//list items
		//log.debug("Items: "+POut.toString(sList));
		//log.debug("A's list: "+POut.toString(filters.keySet()));
		//log.debug("B's list: "+POut.toString(filters2.keySet()));
		
		HashSet<Key> DAMinusB = new HashSet<Key>(2);
		HashSet<Key> DBMinusA = new HashSet<Key>(2);
		
		int curIdx=-1;
		int idx=sList.get(sList.size()-1);
		
		while(idx>-2){
			log.debug("decode Strata: @"+ idx);
			//no elements
			if(!filters.containsKey(idx)||!filters2.containsKey(idx)){
				//log.debug("does not contain the index of the IBF for: "+idx);
				if(idx<0){
					sList.clear();
					sList=null;
					DAMinusB.clear();
					DBMinusA.clear();
					DAMinusB=null;
					DBMinusA=null;
					
					return (int) (Math.pow(2, idx+1)*count)+1;
				}

				idx--;
				continue;
			}
			
			//below zero
			if(idx<0){
				sList.clear();
				sList=null;
				DAMinusB.clear();
				DBMinusA.clear();
				DAMinusB=null;
				DBMinusA=null;
				
				return (int) (Math.pow(2, idx+1)*count)+1;
			}

			//subtractor
			//
			InvertedBloomFilter subtractIBF = filters.get(idx).subtract(filters2.get(idx));
		
			boolean decodeSub = subtractIBF.decode(DAMinusB, DBMinusA,bw);
			log.debug("complete decode subtractionIBF@Strata");
			
			if(!decodeSub){
				DAMinusB.clear();				
				DBMinusA.clear();
				log.debug("can not decode!");
				sList.clear();
				sList=null;
				DAMinusB.clear();
				DBMinusA.clear();
				DAMinusB=null;
				DBMinusA=null;
				
				return (int) (Math.pow(2, idx+1)*count)+1;
			}else{
				
				count+=DAMinusB.size();
				DAMinusB.clear();				
				DBMinusA.clear();
				

				//decrease
				idx--;	
			}	
		
		}
		
		sList.clear();
		sList=null;
		DAMinusB.clear();
		DBMinusA.clear();
		DAMinusB=null;
		DBMinusA=null;
		return -1;
		
	}
	
	public void clear(){
		Enumeration<InvertedBloomFilter> ier = filters.elements();
		while(ier.hasMoreElements()){
			ier.nextElement().clear();
		}		
		filters.clear();
		this.hashC=null;
	}

	public void reset() {
		// TODO Auto-generated method stub
		/*Enumeration<InvertedBloomFilter> ier = filters.elements();
		while(ier.hasMoreElements()){
			ier.nextElement().clear();
		}*/
		/*Iterator<Entry<Integer, InvertedBloomFilter>> ier = this.filters.entrySet().iterator();
		while(ier.hasNext()){
			ier.remove();
		}*/
		
		this.filters.clear();
	}

	public int getSize() {
		// TODO Auto-generated method stub
		if(filters.isEmpty()){
			return 0;
		}
		int count=0;
		Iterator<Entry<Integer, InvertedBloomFilter>> ier = filters.entrySet().iterator();
		while(ier.hasNext()){
			Entry<Integer, InvertedBloomFilter> t = ier.next();
			count+=4*8+t.getValue().getSize();			
		}
		return count;
	}
}
