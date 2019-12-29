package util.bloom.Apache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import util.async.CompressedBF;
import util.async.MathUtil;
import util.bloom.Apache.Hash.Hash;

//import util.bloom.Apache.Hash.Hash;
import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.POut;

public class IndependentHashBloomFilter extends Filter {
		 
		Log log =new Log(IndependentHashBloomFilter.class);
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 4571691937049250188L;

	private static final byte[] bitvalues = new byte[] {
	    (byte)0x01,
	    (byte)0x02,
	    (byte)0x04,
	    (byte)0x08,
	    (byte)0x10,
	    (byte)0x20,
	    (byte)0x40,
	    (byte)0x80
	  };
	  
	  /** The bit vector. */
	  BitSet bits=null;

	  
//	  /**
//	   * index of the hash function (3d hash function)
//	   * (level,indedx,Indhash)
//	   */
//	  HashFuncSeeds[]HashIndex;
//	  
//	  
//	    class HashFuncSeeds{
//	    	int level;
//	    	int index;
//	    	int IndHash;
//	    	
//	    	HashFuncSeeds(int a_,int b_,int c_){
//	    		 level=a_;
//	    		index=b_;
//	    		IndHash=c_;
//	    	}
//	    }
	  
	  /**
	   * number of keys inserted
	   */
	  public int NumInserted=0;
	  
	  /**
	   * seed for init the hash
	   */
	  int seed=-1;
	  
	  /**
	   * init value 
	   */
	 	static int R1=0x5ed50e23;
	 	static int R2=0x1b75e0d1;
	  
	  public void initSeed(){
		  	      
	      //log.info("Z: "+POut.toString(Z));
	      seed=1;
	      
		  for(int j=0;j<LocationNaming.size();j++){
			  seed=HashFunction.SecondHash.hash(
					  ByteBuffer.allocate(4).putInt(R1+R2*LocationNaming.get(j)).array(),seed);
		  }
	  }
	  
	  /**
	   * count the number of insertions for the bit array
	   */
	  //double[] InsertedCounters;
	  
	  /** Default constructor - use with readFields */
	  public IndependentHashBloomFilter() {
	    super();
	    NumInserted=0;
	    indexHashGroup=-1;
	    
	  }

	  
	  /**
	   * Constructor
	   * @param vectorSize The vector size of <i>this</i> filter.
	   * @param nbHash The number of hash function to consider.
	   * @param hashType type of the hashing function (see
	   * {@link org.apache.hadoop.util.hash.Hash}).
	   */
/*	  public IndependentHashBloomFilter(int vectorSize_, int nbHash_, int hashType_
			  ,int level) {
	    super(vectorSize_, nbHash_, hashType_,level);
		    
		  
		this.bits = new BitSet(this.vectorSize);
		this.bits.clear();
		
		this.NumInserted=0;
		this.indexHashGroup=level;	   
	   }*/

	  public IndependentHashBloomFilter(int vectorSize_, int nbHash_, int hashType_
			  ,List<Integer> PrefixNaming) {
	    super(vectorSize_, nbHash_, hashType_,-1);
		    
		  
		this.bits = new BitSet(this.vectorSize);
		this.bits.clear();
		
		this.NumInserted=0;
		this.LocationNaming= new ArrayList<Integer>(PrefixNaming); 
		
		initSeed();
		
		//record the # insertions
	/*	InsertedCounters=new double[vectorSize];
		for(int i=0;i<vectorSize;i++){
			InsertedCounters[i]=0.0;
		}*/
	   }
	  
	  /**
	   * create a Bloom Filter using a BitSet
	   * @param mm
	   * @return
	   */
	  public IndependentHashBloomFilter createBloomFilter(BitSet mm) {
		// TODO Auto-generated constructor stub
		  IndependentHashBloomFilter f=new IndependentHashBloomFilter(vectorSize,nbHash,hashType
				  ,this.LocationNaming);
		  if(mm!=null){
			  f.NumInserted=-1;
			  f.bits.or(mm);
		  }
		  return f;	  
	}

	@Override
	  public void add(Key key) {
	    if(key == null) {
	      throw new NullPointerException("key cannot be null");
	    }

	    int[] h = hash.hash(key,seed);
	   // int[] h = hash.hash(key);
	    //
	    //log.info("h: for ADD: "+POut.toString(h));
	    
	    //test if we add a new key
/*	  if(true){
	    boolean isAllTrue=true;
	    for(int i = 0; i < nbHash; i++) {  
	    	if(!bits.get(h[i])){
	    		isAllTrue=false;
	    	}
	    }
	    if(!isAllTrue){
	    	NumInserted++;
	    }
	  }*/
	  NumInserted++;
	  //NumInserted++;
	    //set the key
	    hash.clear();
	    //System.out.print("\n################\n");
	    for(int i = 0; i < nbHash; i++) {     
	      bits.set(h[i]);
	    //  System.out.print(bits.get(h[i])+" ");
	      
	      //InsertedCounters[h[i]]++;
	    }
	  //  System.out.print("\n################\n");
	    
	    h=null;
	  }

	MathUtil mth=new MathUtil(100);
	  /**
	   * mean inserted positions
	   * @param key
	   * @return
	   */
	  public double getErrorIndicator(Key key){
		  
		  double count=0.0;
		/*  int[] pos = getHash(key);
		  
		  List aa=new ArrayList();
		  
		  for(int i=0;i<pos.length;i++){
			  aa.add(InsertedCounters[pos[i]]);
			  
		  }
		  //count=count/pos.length;
		  
		  count=mth.mean(aa);
		  
		  aa.clear();
		  pos=null;
		  */
		  return count;
	  }
	
	
	  /**
	   * set the bits and return the indexes of the bits
	   * @param key
	   * @return
	   */
	  public int[] addAndReturnIndex(Key key) {
		    if(key == null) {
		      throw new NullPointerException("key cannot be null");
		    }

		    int[] h = hash.hash(key,seed);
		   // int[] h = hash.hash(key);
		    
/*		    boolean isAllTrue=true;
		    for(int i = 0; i < nbHash; i++) {  
		    	if(!bits.get(h[i])){
		    		isAllTrue=false;
		    	}
		    }
		    if(!isAllTrue){
		    	NumInserted++;
		    }*/
		    NumInserted++;
		    
		    hash.clear();
		    //System.out.print("\n################\n");
		    for(int i = 0; i < nbHash; i++) {     
		      bits.set(h[i]);
		    //  System.out.print(bits.get(h[i])+" ");	
		    }
		    return h;
		  //  System.out.print("\n################\n");
		  }
	  
	  @Override
	  public void and(Filter filter) {
	    if(filter == null
	        || !(filter instanceof IndependentHashBloomFilter)
	        || filter.vectorSize != this.vectorSize
	        || filter.nbHash != this.nbHash) {
	      throw new IllegalArgumentException("filters cannot be and-ed");
	    }

	    this.bits.and(((IndependentHashBloomFilter) filter).bits);
	    
	    
	    //update the error
	    setError(this,(IndependentHashBloomFilter)filter);	    
	  }

	  @Override
	  public boolean membershipTest(Key key) {
	    if(key == null) {
	      throw new NullPointerException("key cannot be null");
	    }

	    int[] h = hash.hash(key,seed);
	    //int[] h = hash.hash(key);
	    
	    hash.clear();
	    for(int i = 0; i < nbHash; i++) {
	    	//System.out.println("$: "+h[i]+", "+bits.get(h[i]));
	      if(!bits.get(h[i])) {
	        return false;
	      }
	    }
	    return true;
	  }

	  /**
	   * get the hashed Key
	   * @param key
	   * @return
	   */
	  public int[] getHash(Key key){
		  
		  return hash.hash(key,seed);
		  //return hash.hash(key);
	  }
	  
	  /**
	   * test membership and return the index
	   * @param key
	   * @param Indexes
	   * @return
	   */
	  public boolean membershipTestAndReturnIndexes(Key key) {
		  
		  	//intialize as null
		  	
		  
		    if(key == null) {
		      throw new NullPointerException("key cannot be null");
		    }

		    int[] h = hash.hash(key,seed);
		    //int[] h = hash.hash(key);
		    
		    hash.clear();
		    for(int i = 0; i < nbHash; i++) {
		    	//System.out.println("$: "+h[i]+", "+bits.get(h[i]));
		      if(!bits.get(h[i])) {
		        return false;
		      }
		    }
		    //System.out.println("$: Yes, found!");
		    //yes
		    
		    return true;
		  }
	  
	  /**
	   * true bits
	   * @return
	   */
	  	public int getNumTrueBits(){
		  int sum=0;
		  //return bits.cardinality();
		  
		  if(bits.cardinality()>0){
			  for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i+1)) {
				  if(i>=0){
					  sum++;
				  }
			  }
			  }
		  return sum;
	  }
	  
	  /**
	   * get all true bits
	   * @return
	   */
	  public List<Integer> getAllTrueBits(){
		  List<Integer> indexes=new ArrayList<Integer>(2);
		  /*for(int i=0;i<vectorSize;i++){
			  if(bits.get(i)){
				  indexes.add(i);
			  }
		  }*/
		  //has true bits
		  if(bits.cardinality()>0){
		  for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i+1)) {
			  if(i>=0){
				  indexes.add(i);
			  }
		  }
		  }
		  return indexes;
	  }
	  
	  
	  /**
	   * simulate the perfect hash
	   * @param nKeys
	   */
	  public void addByPerfectHash(int nKeys){
		  
		  NumInserted=nKeys;
		  int pos=-1;
			  
		  Random seed=new Random(System.currentTimeMillis());		  
			  //k independent hash function
			  for(int j=0;j<nbHash;j++){
				  Random r=new Random(seed.nextInt());
				  
				  for(int i=0;i<nKeys;i++){
					  pos=r.nextInt(vectorSize);
					  setBit(pos);	
				  
				  }
			  }		  
		  }		
	  
	  
	  /**
	   * reset the fit
	   * @param pos
	   */
	  public void flipBit(int pos){
		  bits.flip(pos);
	  }
	  
	  @Override
	  public void not() {
	    bits.flip(0, vectorSize - 1);
	  }

	  @Override
	  public void or(Filter filter) {
	    if(filter == null
	        || !(filter instanceof IndependentHashBloomFilter)
	        || filter.vectorSize != this.vectorSize
	        || filter.nbHash != this.nbHash) {
	      throw new IllegalArgumentException("filters cannot be or-ed");
	    }
	    bits.or(((IndependentHashBloomFilter) filter).bits);
	  }

	  @Override
	  public void xor(Filter filter) {
	    if(filter == null
	        || !(filter instanceof IndependentHashBloomFilter)
	        || filter.vectorSize != this.vectorSize
	        || filter.nbHash != this.nbHash) {
	      throw new IllegalArgumentException("filters cannot be xor-ed");
	    }
	    bits.xor(((IndependentHashBloomFilter) filter).bits);
	  }

	  @Override
	  public String toString() {
	    return bits.toString();
	  }

	  /**
	   * get a copy of the filter
	   * @return
	   */
	  public IndependentHashBloomFilter getCopy(){
		  IndependentHashBloomFilter f=new IndependentHashBloomFilter(
				  vectorSize,nbHash,hashType,this.LocationNaming);
		  f.or(this);
		  
		  f.NumInserted=this.NumInserted;
		  f.indexHashGroup=this.indexHashGroup;
		  
		  //error
		 // System.arraycopy(this.InsertedCounters, 0, f.InsertedCounters, 0, this.InsertedCounters.length);
		  
		  
		  return f;
	  }
	  
	  public BitSet getBitSet(){
		  return bits;
		  
	  }
	  
	  /**
	   * @return size of the the VariableHashBloomFilter
	   */
	  public int getVectorSize() {
	    return vectorSize;
	  }

	  // Writable

	  @Override
	  public void write(DataOutput out) throws IOException {
	    super.write(out);
	    byte[] bytes = new byte[getNBytes()];
	    for(int i = 0, byteIndex = 0, bitIndex = 0; i < getVectorSize(); i++, bitIndex++) {
	      if (bitIndex == 8) {
	        bitIndex = 0;
	        byteIndex++;
	      }
	      if (bitIndex == 0) {
	        bytes[byteIndex] = 0;
	      }
	      if (bits.get(i)) {
	        bytes[byteIndex] |= bitvalues[bitIndex];
	      }
	    }
	    out.write(bytes);
	  }

	  @Override
	  public void readFields(DataInput in) throws IOException {
	    super.readFields(in);
	    bits = new BitSet(this.getVectorSize());
	    byte[] bytes = new byte[getNBytes()];
	    in.readFully(bytes);
	    for(int i = 0, byteIndex = 0, bitIndex = 0; i < getVectorSize(); i++, bitIndex++) {
	      if (bitIndex == 8) {
	        bitIndex = 0;
	        byteIndex++;
	      }
	      if ((bytes[byteIndex] & bitvalues[bitIndex]) != 0) {
	        bits.set(i);
	      }
	    }
	  }
	  
	  /* @return number of bytes needed to hold bit vector */
	  private int getNBytes() {
	    return (getVectorSize() + 7) / 8;
	  }
	  
	  /**
	   * reset to all false bits
	   */
	  public void clear(){
		 if(this.bits!=null){
			 this.bits.clear();
		 }
		 this.bits=null;
		 this.LocationNaming.clear();
		 this.LocationNaming=null;
		 //this.InsertedCounters=null;
	  }
	  /**
	   * get the size of the bits
	   * @return
	   */
	  public int getSize(){
		  return getVectorSize();
	  }
	  
	  public static void main(String[] args){
		 
		  
		  
		int[] s={2,1};
		List ls=new ArrayList();
		for(int i=0;i<s.length;i++){
			ls.add(s[i]);
		}
		  
		  IndependentHashBloomFilter test=new IndependentHashBloomFilter(
				  
				  1000,8,Hash.MURMUR_HASH,
				 ls);
		  
		  Vector<Key> vec=new Vector<Key>(1);
		  
		  Vector<Key> Reals=new Vector<Key>(1);
		  
		  Random r =new Random(1);	
		  
		  
		  int size=0;
		  
		  
		  for(int j=0;j<500;j++){
			  
			  byte[] key=new byte[4];
			  	  
			  r.nextBytes( key );
			  
			  Key k=new Key(key);
			  
			  vec.add(k.makeCopy());		  
			  if(j%2==0){
				  test.add(k.makeCopy());
				  Reals.add(k.makeCopy());				  
				  size++;			 
			  }
			  
			 
		  }
		  boolean EE=false;
		  int count=0;
		  for(int j=0;j<vec.size();j++){
			  Key k=vec.get(j);
			  //System.out.println("$: "+POut.toString(k.getBytes()));
			  
			  EE=test.membershipTest(k);
			  if(EE){
				  count++;
				//  System.out.println("======================");
				  if(!(Reals.contains(k))){
					  System.err.println("1: false positive"); 
				  }
			  }
			  if((!EE)&&(Reals.contains(k))){
				  System.err.println("2: false negative");
			  }
		  }
		  
		  System.out.println("$: size: "+vec.size());
		  
		  System.out.println("$: Total: "+size);
		  
		  System.out.println("$: real: "+count);
		  
		  System.out.println(test.getAveragedPriorFP()+", "+test.getAveragedObservedFalsePositive());

	  //====================
		  
		  BloomFilter test2=new BloomFilter(
				  
				  1000,8,Hash.MURMUR_HASH);
		  
		  vec=new Vector<Key>(1);
		  
		  Reals=new Vector<Key>(1);
		  
		  r =new Random(1);	
		  
		  
		  size=0;
		  
		  
		  for(int j=0;j<500;j++){
			  
			  byte[] key=new byte[4];
			  	  
			  r.nextBytes( key );
			  
			  Key k=new Key(key);
			  
			  vec.add(k.makeCopy());		  
			  if(j%2==0){
				  test2.add(k.makeCopy());
				  Reals.add(k.makeCopy());				  
				  size++;			 
			  }
			  
			 
		  }
		  EE=false;
		  count=0;
		  for(int j=0;j<vec.size();j++){
			  Key k=vec.get(j);
			  //System.out.println("$: "+POut.toString(k.getBytes()));
			  
			  EE=test2.membershipTest(k);
			  if(EE){
				  count++;
				//  System.out.println("======================");
				  if(!(Reals.contains(k))){
					  System.err.println("1: false positive"); 
				  }
			  }
			  if((!EE)&&(Reals.contains(k))){
				  System.err.println("2: false negative");
			  }
		  }
		  
		  System.out.println("BF$: size: "+vec.size());
		  
		  System.out.println("BF$: Total: "+size);
		  
		  System.out.println("BF$: real: "+count);
		  
		  System.out.println("BF: "+test2.getAveragedPriorFP()+", "+test2.getAveragedObservedFalsePositive());

	  
	  }
	  
	  /**
	   * set a bit
	   * @param indKey
	   */
	  public void setBit(int indKey) {
		// TODO Auto-generated method stub
		  bits.set(indKey);
		}
	  /**
	   * query the bit
	   * @param indKey
	   * @return
	   */
	  public boolean queryBit(int indKey){
		  if(bits.get(indKey)){
			  return true;
		  }else{
			  return false;
		  }
	  }
	  
	  /**
	   * filled factor
	   * @return
	   */
	  public double filledFactor(){
		  
		 //int ff=getAllTrueBits().size();
		 int  ff=bits.cardinality();
		  
		 return (ff+0.0)/vectorSize;
		 
	  }

	  /**
	   * prior fp
	   * @param n
	   * @return
	   */
	  public double getAveragedPriorFP(int n) {
		// TODO Auto-generated method stub
		  
					//		nbHashLev_2_Bloom);
		  
		return Math.pow((1-Math.exp(-(n*nbHash+0.0)/vectorSize)),nbHash);
	  }
	  
	  	/**
	  	 * prior fp
	  	 * @return
	  	 */
	  	public double getAveragedPriorFP() {
			// TODO Auto-generated method stub
			  
						//		nbHashLev_2_Bloom);
	  		int n=NumInserted;
	  		
			return Math.pow((1-Math.exp(-(n*nbHash+0.0)/vectorSize)),nbHash);
		  }
	  
	  
	  	/**
	  	 * posterior 
	  	 * @return
	  	 */
		public double getAveragedObservedFalsePositive() {
		// TODO Auto-generated method stub
			
			return Math.pow(filledFactor(), nbHash);
		}
		/**
		 * different bits
		 * @param b
		 * @return
		 */
		public int diff(IndependentHashBloomFilter b){
			int s=0;
			for(int i=0;i<vectorSize;i++){
				if(queryBit(i)!=b.queryBit(i)){
					s++;
				}			
			}
			return s;
		}


		/**
		 * test the same bits
		 * @return
		 */
		public boolean isAll1() {
			// TODO Auto-generated method stub
			if(this.bits.cardinality()==vectorSize){
				return true;
			}else{
				return false;
			}
			
			/*
			 * ||
					this.bits.cardinality()==0
			 * */
		}

		/**
		 * set the error indicator
		 * @param bf
		 * @param bf2
		 */
		public void setError(IndependentHashBloomFilter bf,
				IndependentHashBloomFilter bf2) {
			// TODO Auto-generated method stub
			/*for(int i=0;i<vectorSize;i++){
				InsertedCounters[i]=Math.min(bf.InsertedCounters[i], bf2.InsertedCounters[i]);
			
				//InsertedCounters[i]=(bf.InsertedCounters[i]+bf2.InsertedCounters[i])/2;

			}*/
		}

		/**
		 * compressed size
		 * @return
		 */
		public double getCompressedSize() {
			// TODO Auto-generated method stub
			double p1=filledFactor();
			
			return CompressedBF.getIdealCompressedBloomFilterSize2(vectorSize, (1-p1));
			
			
		}

		public boolean isAll0() {
			// TODO Auto-generated method stub
			if(this.bits.cardinality()==0){
				return true;
			}else{
				return false;
			}
		}

		/**
		 * hash and query time
		 * @param key
		 * @return
		 */
		public double[] getQueryTime(Key key) {
			// TODO Auto-generated method stub
			long T = System.currentTimeMillis();
			
		    int[] h = hash.hash(key);
		    hash.clear();
		    
		    double THash = System.currentTimeMillis()-T;
		    //log.info(String.format("%d ", System.currentTimeMillis()-T));
		    
		    T=System.currentTimeMillis();
		    
		    for(int j = 0; j < nbHash; j++) {
		    	//System.out.println("$: "+h[i]+", "+bits.get(h[i]));
		      if(!bits.get(h[j])) {			  
		      }
		    }
		    double TAddress = System.currentTimeMillis()-T;
		    
		    double[] time1={THash,TAddress};
		    
		    return time1;
		}
	  
		
		
		
		
		
	}//end class
