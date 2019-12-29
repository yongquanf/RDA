package util.bloom.Apache;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.POut;
import util.async.SetParser;
import util.async.Util;
import util.bloom.Apache.PowerOfTwoBF.OneExpConfig;
import util.bloom.Apache.Hash.Hash;

public class PowerOfTwoBF {

	static Log log = new Log(PowerOfTwoBF.class);
	//# sets of hash functions
	
	public int c=3;
	
	/**
	 * hash function index for each group
	 */
	//public Hashtable<Integer,Set<Integer>> HashIndexForGroups =new 
	//Hashtable<Integer,Set<Integer>>();
	
	/**
	 * # hash functions for each group
	 */
	
	int k=6;
	
	/**
	 * total # hash functions
	 */
	//int H=1000;
	
	/**
	 * size of bf
	 */
	int m=1014;
	
	/**
	 * the cbf
	 */
	CountingBloomFilter CBF;
	
	/**
	 * random
	 */
	Random r=new Random(System.currentTimeMillis());
	
	
	
	public PowerOfTwoBF(){
		
	}
	/**
	 * constructor
	 * @param _c
	 * @param _k
	 * @param _H
	 */
	public PowerOfTwoBF(int _m,int _c, int _k){
		c=_c;
		k=_k;
		
		initBF(_m);
		
		//selectHashFunctions();
	}
	
	/**
	 * init the Bloom filter
	 */
	private void initBF(int _m){
		
		CBF=new CountingBloomFilter(m,k,Hash.MURMUR_HASH);
	}
	
	
/*	*//**
	 * select hash functions
	 *//*
	public void selectHashFunctions(){
		
		for(int i=1;i<=c;i++){
			
			Set<Integer> hashes = new HashSet<Integer>();
			hashes.clear();
			//randomly select k hash functions
			int count=1;
			while(hashes.size()<k){
				hashes.add(r.nextInt(H)+1);
				//hashes.add((i-1)*k+count);
				//count++;
			}
			
		//HashIndexForGroups.put(i, hashes);
			
		}
		
	}*/
	/**
	 * add a key
	 * @param key
	 */
	public void add(Key key) {
		
		/**
		 * select the optimal hash functions that have the minimum number of ones
		 */
		
		int before,after,added;
		int index = -1;
		int minNonZeros=10000000;
		/**
		 * iterate each group
		 */
		for(int iGroup=1; iGroup<=c; iGroup++){
			
			int[] hvalues = PartitionHashingBF.h(key,k,iGroup,m);
			before=CBF.NonZeros();
			CBF.setBits(hvalues);
			after=CBF.NonZeros();
			added=after-before;
			
			CBF.deleteBits(hvalues);
			//better
			if(minNonZeros>added){
				minNonZeros=added;
				index=iGroup;
			}else if(minNonZeros==added){
				if(r.nextBoolean()){
					minNonZeros=added;
					index=iGroup;
				}				
			}			
		}//iterate each group
		if(index>0){
			int[] hvalues = PartitionHashingBF.h(key,k,index,m);
			CBF.setBits(hvalues);
		}else{
			log.error("no hash found");
		}
/*		Iterator<Entry<Integer, Set<Integer>>> ier = HashIndexForGroups.entrySet().iterator();
		while(ier.hasNext()){
			
			Entry<Integer, Set<Integer>> tmp = ier.next();
			
			//index of the set of hash functions
			Integer idx = tmp.getKey();
			//index of the hash functions
			Set<Integer> indexes = tmp.getValue();
			
			//simulate insert
			//ff1=CBF.filledFactor();
			AddedOnes=0;
			//add the bit
			Iterator<Integer> ierHash = indexes.iterator();
			while(ierHash.hasNext()){
				Integer curHashIndex = ierHash.next();
				//compute the hash function
				int indKey=PartitionHashingBF.h(key,curHashIndex,m);
				//set the position
				if(CBF.isZero(indKey)){
					AddedOnes++;
				}
				CBF.setBit(indKey);					
			}
			
			//ff2=CBF.filledFactor();
			//log.info("added ones: "+AddedOnes);
			//record
			if(AddedOnes<minNumOnes){
				//update records
				minNumOnes=AddedOnes;
				//update idx
				bestHashSet=idx;
				bestAddedOnes=minNumOnes;
				
			}else if(AddedOnes==minNumOnes){
				//flip a coin
				if(r.nextBoolean()){
					//update records
					minNumOnes=AddedOnes;
					//update idx
					bestHashSet=idx;
					bestAddedOnes=minNumOnes;
				}
				
			}
			
			//remove the bit
			ierHash = indexes.iterator();
			while(ierHash.hasNext()){
				Integer curHashIndex = ierHash.next();
				//compute the hash function
				int indKey=PartitionHashingBF.h(key,curHashIndex,m);
				//set the position
				CBF.deleteBit(indKey);					
			}
			
			//ff3=CBF.filledFactor();
			//log.info("before: "+ff1+", after: "+ff2+", reset: "+ff3);
		}
		//choose the final
		//int finalHashSet=bestHashSet;
		if(bestHashSet<0){
			log.warn("no hash functions are found");
		}
		
		//set the bits
		
		Set<Integer> indexes = HashIndexForGroups.get(bestHashSet);
		
		Iterator<Integer> ierHash = indexes.iterator();
		while(ierHash.hasNext()){
			Integer curHashIndex = ierHash.next();
			//compute the hash function
			int indKey=PartitionHashingBF.h(key,curHashIndex,m);
			//set the position
			CBF.setBit(indKey);					
		}*/
		
	}
	
	/**
	 * add
	 * @param s
	 */
	public void add(Collection<Key> s){
		Iterator<Key> ier = s.iterator();
		while(ier.hasNext()){
			
			add(ier.next());
			
		}
	}
	
	public double posterirFP(){
		return Math.pow(filledFactor(), k);
	}
	
	public double filledFactor(){
		return CBF.filledFactor();
		
	}
	
	/**
	 * configuration
	 * @author ericfu
	 *
	 */
	class OneExpConfig{
		int maxN;
		double bandwidth;
		
		public OneExpConfig(int _n, double _bw){
			maxN=_n;
			bandwidth = _bw;
		}
	}
	/**
	 * save the parameters
	 */
	List<OneExpConfig> params =new ArrayList<OneExpConfig>(1);
	/**
	 * test the power of two choices using files
	 */
	public void readByInstanceOfBloomTree(String fname){
			
			try {
				
				params.clear();
				
				BufferedReader bf=new BufferedReader(new FileReader(fname));
				
				String thisLine;
				Integer indexOfNodes=0;			
				while ((thisLine = bf.readLine()) != null) { // while loop begins here
				
					log.info(thisLine);
					//parse the users
					String[] strs= SetParser.getKeyParameters(thisLine);
					//failed case
					if(strs==null){
						log.warn("empty parameters");
						continue;
					}
					log.info(POut.toString(strs));
					//number of keys
					int n=Integer.parseInt(strs[0]);
					int d=Integer.parseInt(strs[2]);
					int indexBW=3+d*2+2;
					//due to array starts from 0
					double bw = Double.parseDouble(strs[indexBW-1]);
									
					OneExpConfig s =new OneExpConfig(n,bw);
					
					log.info("Instance: "+s.toString());
					
					params.add(s);					
				}
								
				bf.close();
				
			}catch(Exception e){
				e.printStackTrace();			
			}
	}
	/**
	 * iterate the parameter list	
	 */
	public void testByInstance(BufferedWriter bwriter){
		//repeat rounds
		int repeated=20;
		
		//c
		int[] cs={3,10,100,500};
		
		//double fp=0;
		//iteration
		Iterator<OneExpConfig> ier = params.iterator();
		while(ier.hasNext()){
			//read one parameter
			OneExpConfig one = ier.next();
			
			//keys
			int n=one.maxN;
			//bw
			double bw=one.bandwidth;
			
			//size
			int _m =(int)Math.round(bw);
			//# hash
			int _k = (int)Math.max(1,(Math.log(2)*(_m/(0.0+n))));
			
			//iterate c
			for(int idc=0;idc<cs.length;idc++){
				
			 int _c =cs[idc];
			
			//create keys
			for(int rept=0;rept<repeated;rept++){
				
				Set<Key> keys=new HashSet<Key>(1);
				  Random r =new Random(1);	
				  		  
				 
				  
				  while(keys.size()<n){
					  
					  byte[] key=new byte[4];;
					  	  
					  r.nextBytes( key );
					  
					  Key k=new Key(key);
					  
					  keys.add(k.makeCopy());	
				  }
				  
				  PowerOfTwoBF testpbf =new PowerOfTwoBF(_m,_c,_k);
				  testpbf.add(keys);
				  
				  //false positive
				  double fp=Math.pow(testpbf.filledFactor(),_k);
				  
				  //store
				  try {
					bwriter.append(String.format("%d %.4e  %d %.4e", n,bw,_c, fp));
					bwriter.newLine();
					bwriter.flush();
					  
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				  
				  
				  //release
				  testpbf.CBF.clear();
				  testpbf.CBF=null;
				  testpbf=null;	  
				  //release
				  keys.clear();
				  keys=null;
				  
			}//repeated rounds
			
			}// c
			
		}
		
		
	}
	
//	public void testBloomFilterCompressedBloomFilter(BufferedWriter bwriter){
//		//repeat rounds
//		int repeated=5;
//		
//		int hR=Hash.MURMUR_HASH;	
//		
//		//c
//		//int[] cs={3,10,100,500};
//		
//		//double fp=0;
//		//iteration
//		Iterator<OneExpConfig> ier = params.iterator();
//		while(ier.hasNext()){
//			//read one parameter
//			OneExpConfig one = ier.next();
//			
//			//keys
//			int n=one.maxN;
//			//bw
//			double bw=one.bandwidth;
//			
//			//size
//			int _m =(int)Math.round(bw);
//			//# hash
//			int _k = (int)Math.max(1,(Math.log(2)*(_m/(0.0+n))));
//			
//	
//			
//			//create keys
//			for(int rept=0;rept<repeated;rept++){
//				
//				Set<Key> keys=new HashSet<Key>(1);
//				  Random r =new Random(1);	
//				  		  
//				 
//				  
//				  while(keys.size()<n){
//					  
//					  byte[] key=new byte[4];;
//					  	  
//					  r.nextBytes( key );
//					  
//					  Key k=new Key(key);
//					  
//					  keys.add(k.makeCopy());	
//				  }
//				  
//				  
//				  BloomFilter SBF = new BloomFilter(_m,_k,hR);
//					
//					long T=System.currentTimeMillis();
//
//					SBF.add(keys);
//						
//					double errBFOnce=SBF.getAveragedObservedFalsePositive();										
//					SBF.clear();
//					SBF=null;
//					
//					int compressedK=2;
//					//storage size
//					double BWIBB=_m;
//					
//					int bwBFTree = (int)Math.round(BWIBB);
//					
//					int cbfSize= (int) Math.round(CompressedBF.getIdealBloomFilterSize(bwBFTree,n,compressedK));
//					
//					if(cbfSize<=0){
//						log.warn("Compressed: bw: "+BWIBB +", n: "+n +", k: "+compressedK);
//						cbfSize=(int)Math.round(0.5f+BWIBB*1.98);
//					}
//					
//					BloomFilter SBFA = new BloomFilter(cbfSize,compressedK,hR);
//				
//					SBFA.add(keys);
//					
//					double fpSBFA=SBFA.getAveragedObservedFalsePositive();
//					
//					SBFA.clear();
//					SBFA=null;
//				  
//				  
//				  
//				  //store
//				  try {
//					bwriter.append(String.format("%d %.4e %.4e %d %.4e", n,bw, errBFOnce,cbfSize,fpSBFA));
//					bwriter.newLine();
//					bwriter.flush();
//					  
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//				  
//				  
//				  //release
//				  //release
//				  keys.clear();
//				  keys=null;
//				  
//			}//repeated rounds
//			
//		}
//		
//		
//	}
//	
	
	
	/**
	 * compare to BloomTree with the same bandwidth
	 * @param fname
	 * @param strSaveExp
	 */
	public void Compare2BloomTree(String fname, String strSaveExp){
		
		readByInstanceOfBloomTree(fname);
				
		BufferedWriter bwriter;
				
		try{			
			bwriter = new BufferedWriter(new FileWriter( strSaveExp,true));			
			testByInstance(bwriter);
			
			
			bwriter.flush();
			bwriter.close();
			
		}catch(IOException e){
			e.printStackTrace();
		}
		
	}
	
	/**
	 * test the Bloom filter, compressed Bloom filter
	 * @param fname
	 * @param strSaveExp
	 */
//	public void testBFCompressedBF(String fname, String strSaveExp){
//		
//		readByInstanceOfBloomTree(fname);
//				
//		BufferedWriter bwriter;
//				
//		try{			
//			bwriter = new BufferedWriter(new FileWriter( strSaveExp,true));		
//			
//			testBloomFilterCompressedBloomFilter(bwriter);
//			
//			
//			bwriter.flush();
//			bwriter.close();
//			
//		}catch(IOException e){
//			e.printStackTrace();
//		}
//		
//		
//	}
	
	public static void main2(String[] args){
		
		PowerOfTwoBF test =new PowerOfTwoBF();
		
		test.Compare2BloomTree("SpaceGrowthTest4BloomTreeSynTestParameters2Sample_false",
				"ComparePowerOfTwoChoice_SpaceGrowthTest4BloomTreeSynTestParameters2Sample_false");
	}
	
	
	
	public static void simulatePartitionHashPowerOfTwo(double bw2){
		
		int[] ns = Util.generateSequenceArray(20, 20, 200);
		
		Set<Key> keys=new HashSet<Key>(1);
		
		for(int i=0;i<ns.length;i++){
			int n=ns[i];
			
			keys.clear();
			
			int _m = (int)Math.round(bw2);
			
			int _cA=500;
			
			int _cB=1500;
			
			int _k =  (int)Math.round(Math.log(2) * _m/(200+0.0));
			
			
			  Random r =new Random(1);	
			  		  

			  
			  while(keys.size()<n){
				  
				  byte[] key=new byte[4];;
				  	  
				  r.nextBytes( key );
				  
				  Key k=new Key(key);
				  
				  keys.add(k.makeCopy());	
			  }
			  
			  double fp=0;
			  for(int repted=0;repted<10;repted++){
				//power of two choices
				  PowerOfTwoBF testpbf =new PowerOfTwoBF(_m,_cA,_k);
				  testpbf.add(keys);
				  
				  double fp1=testpbf.filledFactor();
				  fp+=fp1;
				  
				  testpbf.CBF.clear();
				  testpbf.CBF=null;
				  
				  testpbf=null;
			  }
			  fp/=10;
			  
			  double fp2=0;
			  for(int repted=0;repted<10;repted++){
				//power of two choices
				  PowerOfTwoBF testpbf =new PowerOfTwoBF(_m,_cB,_k);
				  testpbf.add(keys);
				  
				  double fp1=testpbf.filledFactor();
				  fp2+=fp1;
				  
				  testpbf.CBF.clear();
				  testpbf.CBF=null;
				  testpbf=null;
				  
			  }
			  fp2/=10;
			  
			  BloomFilter bf= new BloomFilter(_m,_k,Hash.MURMUR_HASH);
			  bf.add(keys);
			  
			 System.out.println(String.format("%d %.4e %.4e %.4e %.4e\n", n,bw2,bf.getAveragedObservedFalsePositive(),
					 Math.pow(fp,_k),Math.pow(fp2,_k)));

			 keys.clear();
			 
		}
		
		keys=null;
		
		
	}
	
	public static void main(String[] args){
		
		double bw2=1.6385e+04;
		simulatePartitionHashPowerOfTwo(bw2);
	}
	
	
	public static void main3(String[] args){
		
		//# keys
		int n=1000;
		 //size
		int _m=8000;
		//set of hash
		
		//c=500. power of two,
		//c= 1000, partitioned hash
		
		int _c=2000;
		//# hash for a bf
		int _k=7;
		//total # hash functions
		int _H=100;
		
		
		
		//create a set of keys
		
		Set<Key> keys=new HashSet<Key>(1);
		  Random r =new Random(1);	
		  		  
		 
		  
		  while(keys.size()<n){
			  
			  byte[] key=new byte[4];;
			  	  
			  r.nextBytes( key );
			  
			  Key k=new Key(key);
			  
			  keys.add(k.makeCopy());	
		  }
		  
		  log.info("#Keys: "+keys.size());
		  
		  //Bloom filter
		  BloomFilter bf= new BloomFilter(_m,_k,Hash.MURMUR_HASH);
		  bf.add(keys);
		  
		  double fp=0;
		  for(int repted=0;repted<10;repted++){
			//power of two choices
			  PowerOfTwoBF testpbf =new PowerOfTwoBF(_m,_c,_k);
			  testpbf.add(keys);
			  
			  double fp1=testpbf.filledFactor();
			  fp+=fp1;
			  double fp2=bf.filledFactor();
		  
		  	System.out.println(String.format("%.4e %.4e\n", Math.pow(fp1,_k),Math.pow(fp2,_k)));
		  }
	}
	
	
}
