package util.async;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import edu.harvard.syrah.prp.Log;

import util.bloom.Apache.BloomFilter;
import util.bloom.Apache.Key;
import util.bloom.Apache.Hash.Hash;

public class CompressedBF {
	static Log log = new Log(CompressedBF.class);
	
	/**
	 * we know the compressed size, decompress to obtain the real size
	 * @param z
	 * @param n
	 * @param k
	 * @return
	 */
	public static double getDecompressedBFSize(int z,int n,int k){
		
		
		
		return -1;
		
	}
	
	public static int getDecompressBFByConstruction(int optM,int n, int k){
		
		int hR=Hash.MURMUR_HASH;
		
		Random r= new Random(System.currentTimeMillis());
		
		Set<Key> keyList=new HashSet<Key>(n);
		while(true){
			
			//break until enough keys
			if(keyList.size()==n){
				break;
			}
			
			byte[] key=new byte[4];
  	  
			r.nextBytes( key );
  
			//key=ByteBuffer.allocate(4).putInt(100).array();
			
			Key keyOne=new Key(key);
			//same key, continue;
			if(keyList.contains(keyOne)){
				
				log.debug("repeated key!!");
				continue;
			}else{
				keyList.add(keyOne.makeCopy());
			}		  
	}
		
		
		int decompressedM=optM;
		int step = (int)Math.round(decompressedM/(2.0)); 
		double p=-1;
		double compressedSize;
		
		int round = 0;
		while(true){
		
		BloomFilter SBF = new BloomFilter(decompressedM,k,hR);
		SBF.add(keyList);
		
		//a bit is zero
		p = 1- SBF.filledFactor();
		
		//decompressed size
		compressedSize=decompressedM*getEntropy(p);
		
		log.debug("round: "+round+", decompressedM: "+decompressedM+", compressed: "+compressedSize+", OPT: "+optM);	
		
		//reach the optimal size
		if(compressedSize>=optM){
			break;
		}
		
		//increase the step
		decompressedM+=step;
		
		round++;
		//sbf clear
		SBF.clear();
		SBF=null;
		}

		//clear the keys
		keyList.clear();
		keyList=null;
		
		return decompressedM;
	}
	
	
	/**
	 * the compressed size
	 * @param m
	 * @param n
	 * @param k
	 * @return
	 */
	public static double getIdealBloomFilterSize0(int m,int n,int k){
		
		double p;
		
		double d2;
	
		//return d2*m;
		
		double m0=m;
		double increment=m;
		
		double compressedSize=-1;
		
		double diff=0;
		
		double oldDiff=10000000;
		//increase the size
		while(true){
			
			if(Math.abs(oldDiff-diff)<5){
				break;
			}
			/*if(getIdealCompressedBF((int)Math.round(m0),n,k)>=m){
				break;
			}*/
						
			oldDiff=diff;
			
			p=Math.pow((1-1.0/m0),k*n);
			
			d2=getEntropy(p);
			
			compressedSize=m0*d2;
			
			log.debug(m0+", "+compressedSize);
			
			diff=Math.abs(compressedSize-m);
			
			m0=m0+increment;
		}
		
		return m0;
	}
	
	
	public static double getIdealBloomFilterSize(int m,int n,int k){
		
		int storage=m;
		int step=(int)Math.round(Math.max(10, storage/100.0));
		double realCompressedSize=0;
		
		//int maximalStep=(storage/step)*1000;
		//int maximalStep=(100000/step);
		double upperBound=Math.pow(10, 6);
		
		while(true){
			
			realCompressedSize=CompressedBF.getIdealCompressedBF(storage,n,k);
			
			log.debug(storage+", "+realCompressedSize);
			if(realCompressedSize>m||realCompressedSize>=upperBound){
				break;
			}
			storage+=step;
			//maximalStep--;
		}
		
		return storage;
	}
	
	/**
	 * compressed size
	 * @param storage
	 * @param n
	 * @param k
	 * @return
	 */
	public static double getIdealCompressedBF(int storage,int n,int k){
		
		double p=Math.pow((1-1.0/storage),k*n);
		
		double d2=getEntropy(p);
		
		double compressedSize=storage*d2;
		
		return compressedSize;
		
	}
	
	
	/**
	 * compute use the posterior false positive probability
	 * @param m
	 * @param p
	 * @return
	 */
	public static double getIdealCompressedBloomFilterSize2(int m,double p){
		
		double d2=getEntropy(p);
		
		//return m/d2;
		
		return d2*m;
	}
	
	
	/**
	 * entropy of the bloom filter
	 * @param p
	 * @return
	 */
	public static double getEntropy(double p){
		
		double d1=Math.log(p)/Math.log(2);
		
		double d2=Math.log(1-p)/Math.log(2);
		
		return -p*d1-(1-p)*d2;
		
		
	}
	
	/**
	 * main entry
	 * @param args
	 */
	public static void main(String[] args){
		
		
		int k = 2;
		int n=100;
		int optM=8193;
		
		//int newSize=getDecompressBFByConstruction(optM,n,k);
		//double newSize=getIdealBloomFilterSize(optM,n,k);
		double newSize;
		
		//newSize= getIdealBloomFilterSize(optM,n,k);
		
		newSize=1000000;
		
		
		double compressedSize=getIdealCompressedBF((int)Math.round(newSize),n,k);
		
		
		log.info(String.format("%d %.4e %.4e\n", optM,newSize, compressedSize));
	}
}
