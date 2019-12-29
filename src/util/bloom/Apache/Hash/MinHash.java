package  util.bloom.Apache.Hash;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Dictionary;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.collections.CollectionUtils;

import edu.harvard.syrah.prp.Log;

import util.async.Util;

public class MinHash
{
	
	static Log log =new Log(MinHash.class);
	
    private int m_numHashFunctions = 100; //Modify this parameter
    //private int Hash(int index);
    private HashFuncSeeds[] m_hashFunctions;
    
    /**
     * universe
     */
    static int universeSize=Integer.MAX_VALUE;
    
    /**
     * store the seeds of the hash function
     * @author Administrator
     *
     */
    class HashFuncSeeds{
    	int a;
    	int b;
    	int c;
    	
    	HashFuncSeeds(int a_,int b_,int c_){
    		a=a_;
    		b=b_;
    		c=c_;
    	}
    }

    /**
     * constructor
     * @param universeSize
     */
    public MinHash(int k)
    {
    	
    	m_numHashFunctions=k;
      
        m_hashFunctions = new HashFuncSeeds[m_numHashFunctions];

        Random r = new Random(11);
        for (int i = 0; i < m_numHashFunctions; i++)
        {
            int a = (int)r.nextInt(universeSize);
            int b = (int)r.nextInt(universeSize);
            int c = (int)r.nextInt(universeSize);
            //m_hashFunctions[i] = x => QHash((int)x, a, b, c, (int)universeSize);
            m_hashFunctions[i]=new HashFuncSeeds(a,b,c);
        } 
    }

    /**
     * get the hash value
     * @param x
     * @param a
     * @param b
     * @param c
     * @param universeSize
     * @return
     */
    public int getX(int x,int a,int b,int c,int universeSize){
    	return  QHash((int)x, a, b, c, (int)universeSize);
    }
    
    /**
     * similarity
     * @param set1
     * @param set2
     * @return
     */
    public double Similarity(HashSet<Integer> set1, HashSet<Integer> set2)
    {
    
        int numSets = 2;
        Hashtable<Integer,Boolean[]> bitMap = BuildBitMap(set1, set2);
        
        int[][] minHashValues = GetMinHashSlots(numSets, m_numHashFunctions);

        ComputeMinHashForSet(set1, 0, minHashValues, bitMap);
        ComputeMinHashForSet(set2, 1, minHashValues, bitMap);

        double dd=ComputeSimilarityFromSignatures(minHashValues, m_numHashFunctions);
        
        //clear
        bitMap.clear();
        bitMap=null;
        //array clear
        for(int i=0;i<minHashValues.length;i++){
        	minHashValues[i]=null;
        }
        minHashValues=null;
        
        return dd;
    }

    
    /**
     * min-hash
     * @param set
     * @param setIndex
     * @param minHashValues
     * @param bitArray
     */
    private void ComputeMinHashForSet(HashSet<Integer> set, int setIndex, 
    		int[][] minHashValues, Hashtable<Integer,Boolean[]> bitArray)
    {
        int index = 0;
        
        Iterator<Entry<Integer, Boolean[]>> ier = bitArray.entrySet().iterator();
        while(ier.hasNext()){
        	Entry<Integer, Boolean[]> tmp = ier.next();
        	
        	int id=tmp.getKey();
        	Boolean[] tester = tmp.getValue();
        	
        	for (int i = 0; i < m_numHashFunctions; i++){
        		if(set.contains(id)){
        			 int hindex = getX(id, m_hashFunctions[i].a, 
        					 m_hashFunctions[i].b, m_hashFunctions[i].c, universeSize);

                     if (hindex < minHashValues[setIndex][ i])
                     {
                         minHashValues[setIndex][ i] = hindex;
                     }
        			
        			
        		}
        		
        		
        	}
        	
        	index++;
        	
        }
    }

    /**
     * hash slots
     * @param numSets
     * @param numHashFunctions
     * @return
     */
    private static int[][] GetMinHashSlots(int numSets, int numHashFunctions)
    {
        int[][] minHashValues = new int[numSets][numHashFunctions];

        for (int i = 0; i < numSets; i++)
        {
            for (int j = 0; j < numHashFunctions; j++)
            {
                minHashValues[i] [j] = Integer.MAX_VALUE;
            }
        }
        return minHashValues;
    }

    private static int QHash(int x, int a, int b, int c, int bound)
    {
        //Modify the hash family as per the size of possible elements in a Set
        int hashValue = (int)((a * (x >> 4) + b * x + c) & 131071);
        return Math.abs(hashValue);
    }

    private static Hashtable<Integer,Boolean[]> BuildBitMap(HashSet<Integer> set1, HashSet<Integer> set2)
    {
       
        
        Boolean[] bool={true,false};
        Hashtable<Integer,Boolean[]> bitArray = new Hashtable<Integer,Boolean[]>();
        
        Iterator<Integer> ier = set1.iterator();
        while(ier.hasNext()){
        	bitArray.put(ier.next(), bool );
        }
        
        ier=set2.iterator();
        while(ier.hasNext()){
        	Integer tmp = ier.next();
        	
        	if(bitArray.containsKey(tmp)){
        		Boolean[] bb={true,true};
        		bitArray.put(tmp,bb );
        	}else{
        		Boolean[] bb={false,true};
        		bitArray.put(tmp,bb);
        	}
        }    
        return bitArray;
    }

    
    /**
     * similarity
     * @param minHashValues
     * @param numHashFunctions
     * @return
     */
    private static double ComputeSimilarityFromSignatures(int[][] minHashValues, 
    		int numHashFunctions)
    {
        int identicalMinHashes = 0;
        for (int i = 0; i < numHashFunctions; i++)
        {
            if (minHashValues[0][i] == minHashValues[1][i])
            {
                identicalMinHashes++;
            }
        }
        return (1.0 * identicalMinHashes) / numHashFunctions;
    }

    
    public void clear(){
    	
    	for(int i=0;i<m_hashFunctions.length;i++){
    		m_hashFunctions[i]=null;
    	}
    	m_hashFunctions=null;
    	
    }
    
    public static void testSimilarity(){
    	
    	
    	int n=10000;
    	
    	int[] curIntersect=Util.generateSequenceArray(2000, 2000, n);
    	
    	
    	int[]NHash={10,50,100,500,1000};
    	
    	for(int j=0;j<NHash.length;j++){   		
    		int kk=NHash[j];
    		log.info("\n\n"+"#: "+kk+"\n\n");
    	for(int i=0;i<curIntersect.length;i++){
    		testSimilarity("TestMinWise",n,curIntersect[i],kk);
    	}
       }
    }
    
    
    
    /**
     * test the accuracy
     * @param n
     * @param curIntersect
     * @param k
     */
    public static void testSimilarity(String str,int n,int curIntersect,int k){
    	
    	//int n=1000;
    	
    	//int curIntersect=50;
    	
    	//int k=10000;
    	
    	//log.info(String.format("%d %d %d", n,curIntersect,k));
    	
    	//log.info("real: "+curIntersect);

    	BufferedWriter bufferedWriter = null;
		try{
		
			bufferedWriter = new BufferedWriter(new FileWriter(str,true));

    	
    	
    	
    	int repts=1;
    	int aa=0;
    	
    	for(int i=0;i<repts;i++){
    		//log.info("$: "+i);
    	//==========================
    	List<Integer> totalIDs = Util.generateRandomIntegers(n);
		
    	List<Integer> totalIDs2 = Util.generateRandomIntegers(curIntersect,totalIDs,n);
		//==========================
    	HashSet<Integer> set1=new HashSet<Integer>(totalIDs);
    	HashSet<Integer> set2=new HashSet<Integer>(totalIDs2);
    	//===========================
    	totalIDs.clear();
    	totalIDs=null;
    	totalIDs2.clear();
    	totalIDs2=null;
    	
    	//testor
    	MinHash test1=new MinHash(k);
    	//MinHash test2=new MinHash();
    	
    	double similar=test1.Similarity(set1, set2);
    	
    	/**
    	 * similarity
    	 */
    	int f1=set1.size();
    	int f2=set2.size();
    	
    	int estimatedIntersect=(int)Math.round(similar*k);
    	

    	log.info("similarity: "+similar+ ", JaccardSimilarity: "+JaccardSimilarity(set1, set2));
    	
    	
    	test1.clear();
    	test1=null;
    	
    	set1.clear();
    	set2.clear();
    	set1=null;
    	set2=null;
    	
    	aa+=getEstimatedIntersection(f1, f2, similar);
    	
    	}
    	//log.info("Real: "+curIntersect);
    	
    	//log.info("estimatedV1: "+estimatedIntersect);
    	
    	//log.info("estimatedV2: "+getEstimatedIntersection(f1, f2, similar));
    	
    	bufferedWriter.append(String.format("%d %d %d %d", n,curIntersect,k,
    			(int)Math.round(aa/(repts+0.0))));
    	bufferedWriter.newLine();
    	bufferedWriter.flush();
    	
    	bufferedWriter.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		
		
    	

    }
    
    /**
     * estimate size using min-wise hash
     * @param f1
     * @param f2
     * @param R
     * @return
     */
    public static int getEstimatedIntersection(int f1,int f2,double R){
    	
    	return (int)Math.round(R*(f1+f2)/(1+R));
    }
    
    
    /**
     * JaccardSimilarity 
     * @param set1
     * @param set2
     * @return
     */
    public static double JaccardSimilarity (HashSet<Integer> set1, HashSet<Integer> set2) 
    { 
    
    	
        int intersectionCount =  CollectionUtils.intersection(set1, set2).size();
        int unionCount = CollectionUtils.union(set1, set2).size(); 

        return (1.0 * intersectionCount) / unionCount; 
    } 
    
    /**
     * test
     * @param args
     */
    public static void main(String[] args){
    	
    	testSimilarity();
    	
    	
    }
}
