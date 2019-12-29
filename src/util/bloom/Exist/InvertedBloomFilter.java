package util.bloom.Exist;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.POut;

import util.async.Util;
import util.bloom.Apache.Filter;
import util.bloom.Apache.HashFunction;
import util.bloom.Apache.Key;

public class InvertedBloomFilter extends Filter{

	Log log=new Log(InvertedBloomFilter.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 6031758968525628235L;
	
	
	/**
	 * instance
	 */
	public CellEntry[] instance;
	
	/**
	 * for oneHash
	 */
	public HashFunction hashC;

	public InvertedBloomFilter(int vectorSize, int nbHash, int hashType){
		super(vectorSize,nbHash,hashType);
		instance = new CellEntry[vectorSize];
		for(int i=0;i<vectorSize;i++){
			instance[i] = new CellEntry();
		}
		hashC=new HashFunction(vectorSize, 1, hashType);
	}
	
	/**
	 * copy an IBF
	 * @param t
	 * @param vectorSize
	 * @param nbHash
	 * @param hashType
	 */
	public InvertedBloomFilter(InvertedBloomFilter t,int vectorSize, int nbHash, int hashType){
		super(vectorSize,nbHash,hashType);
		instance = new CellEntry[vectorSize];
		for(int i=0;i<vectorSize;i++){
			instance[i] = t.instance[i].getCopy();
		}
		hashC=new HashFunction(vectorSize, 1,hashType);
	}
	
	
	
	
	public InvertedBloomFilter(InvertedBloomFilter ibf) {
		// TODO Auto-generated constructor stub
		super(ibf.vectorSize,ibf.nbHash,ibf.hashType);
		instance = new CellEntry[ibf.vectorSize];
		for(int i=0;i<ibf.vectorSize;i++){
			instance[i] = ibf.instance[i].getCopy();
		}
		hashC=new HashFunction(ibf.vectorSize, 1, ibf.hashType);
	}

	/**
	 * subtract the key
	 * @param y
	 */
	public InvertedBloomFilter subtract(InvertedBloomFilter B2){
		if(B2==null||B2.vectorSize!=this.vectorSize){
			log.warn("subtract failed");
			return null;
		}
		
		InvertedBloomFilter B3=new InvertedBloomFilter(this.vectorSize,
				this.nbHash,this.hashType);
		for(int i=0;i<this.vectorSize;i++){
			B3.instance[i].set(this.instance[i],B2.instance[i]);
		}
		
		return B3;
	}
	
	
	@Override
	public void add(Key key) {
		// TODO Auto-generated method stub
	
		if(key == null) {
		      throw new NullPointerException("key cannot be null");
		    }

		    /**
		     * h
		     */
		    int[] h = hash.hash(key);
		    
		    /**
		     * h_c
		     */
		    int[] h2 = hashC.hash(key);
		    
		    /**
		     * s
		     */
		    byte[]ss=key.getBytes();
		    
		    //BigInteger keyVal = new BigInteger(ss);
		    
		    /**
		     * h_c(s)
		     */
		    //log.debug("real c: "+h2[0]);
		    //BigInteger c = new BigInteger(ByteBuffer.allocate(4).putInt(h2[0]).array());
		    //BigInteger c = new BigInteger(h2[0]+"");

		    
		    
		    hash.clear();
		    log.debug("\n################\n");
		    for(int i = 0; i < nbHash; i++) {     
		      instance[h[i]].add(ss,ByteBuffer.allocate(4).putInt(h2[0]).array());		    	
		    }
		    
		    h=null;
		    ss=null;
		    h2=null;
		    //c=null;
		    //keyVal=null;
	}

	/**
	 * 
	 * @param idx
	 * @return
	 */	
	public boolean isPure(int idx){
		//boolean isPured=false;
		CellEntry tmp = instance[idx];
		if(tmp==null){
			log.debug("the IBF cell is null");
			return false;
		}
		//log.debug("tmp.hashSum: "+tmp.hashSum.toString());
		
		if(tmp.count==0){
			log.debug("cell at "+idx+" is zeros");
			return false;
		}
		
		
		if(tmp.count==1||
				tmp.count==-1){		
						
			
			//int[] hashedVal=hashC.hash(new Key(ByteBuffer.allocate(4).putInt(tmp.idSum.intValue()).array()));			
			int[] hashedVal=hashC.hash(new Key(tmp.idSum));
			
			if(Util.equalsByte(tmp.hashSum,ByteBuffer.allocate(4).putInt(hashedVal[0]).array())){
				log.debug("cell at "+idx+" passed!");
				
				hashedVal=null;
				
				return true;
			}else{
				log.debug("cell at "+idx+" NOT passed!");
				hashedVal=null;
				
				return false;
			}
		}else{
			log.debug("cell at "+idx+" unpured: "+tmp.count);
			return false;
		}
		
	}
	
	/**
	 * decode
	 * @throws IOException 
	 */
	public boolean decode(HashSet<Key> DAMinusB,HashSet<Key> DBMinusA){
		boolean t=false;
		try {
			t = decode(DAMinusB,DBMinusA,null);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return t;
	}
	
	/**
	 * decode
	 * @throws IOException 
	 */
	public boolean decode(HashSet<Key> DAMinusB,HashSet<Key> DBMinusA,BufferedWriter bw) throws IOException{
		
		DAMinusB.clear();
		DBMinusA.clear();
		
		Set<Integer> pureList=new HashSet<Integer>();
		for(int i=0;i<this.vectorSize;i++){
			if(isPure(i)){
				pureList.add(i);
			}
		}
		if(pureList.isEmpty()){
			pureList.clear();
			pureList=null;
			
			if(!testAllZeros()){
			//bw.append("no pured item, failed");
			//bw.newLine();
			//bw.flush();
			return false;
			}else{
				return true;
			}
		}
		
		//HashSet<Key> DAMinusB = new HashSet<Key>(2);
		//HashSet<Key> DBMinusA = new HashSet<Key>(2);
		
		//Iterator<Integer> ier=null;
		while(!pureList.isEmpty()){
			//remove the item
			Integer i = pureList.iterator().next();
			pureList.remove(i);
			
			//log.debug("i: "+i);
			//empty
			if(i==null){
				break;
			}
			if(!isPure(i)){				
				continue;
			}
			byte[] s = this.instance[i].idSum;
			int c = this.instance[i].count;
			
			//log.debug("s: "+POut.toString(s)+", c: "+c);
			//byte[] byteS = s.toByteArray();
			
			if(DAMinusB.contains(new Key(s))||
					DBMinusA.contains(new Key(s))	
			){
				break;
			}
			
			
			if(c>0){
				DAMinusB.add(new Key(s));
			}else{
				DBMinusA.add(new Key(s));
			}
			
			
			
			int[] hc2 = hashC.hash(new Key(s));
			
			//update
			int[] h = hash.hash(new Key(s));
			
			
			//todo: h does not include the position of i
			if(!Util.containValue(h, i)){
				break;
				//log.warn("can not decode");
				
			}
			
			
			for(int idx2=0;idx2<h.length;idx2++){
				
				log.debug("idx: "+h[idx2]);
				log.debug("before revision\n"+
						POut.toString(instance[h[idx2]].idSum)+
			", hashSum: "+POut.toString(instance[h[idx2]].hashSum)+
			", count: "+ POut.toString(instance[h[idx2]].count)	
				);
				
				instance[h[idx2]].idSum=Util.xor(instance[h[idx2]].idSum,
						s);

				instance[h[idx2]].hashSum=Util.xor(instance[h[idx2]].hashSum,
						ByteBuffer.allocate(4).putInt(hc2[0]).array());
						//new BigInteger(ByteBuffer.allocate(4).putInt(hc2[0]).array()));
				
				instance[h[idx2]].count=instance[h[idx2]].count-c;	
				
				log.debug("after revision\n"+
						POut.toString(instance[h[idx2]].idSum)+
			", hashSum: "+POut.toString(instance[h[idx2]].hashSum)+
			", count: "+ POut.toString(instance[h[idx2]].count)	
				);
				//test pure
				if(isPure(h[idx2])){
					pureList.add(h[idx2]);
				}
				
			}
			
			//update
			/*pureList.clear();
			for(int jj=0;jj<this.vectorSize;jj++){
				if(isPure(jj)){
					pureList.add(jj);
				}
			}*/
			
			
			hc2=null;
			h=null;
			
		}
		
//		DAMinusB.clear();
//		DAMinusB=null;
//		DBMinusA.clear();
//		DBMinusA=null;
		
		pureList.clear();
		pureList=null;
		
		byte[]zeros=new byte[4];
		for(int i=0;i<4;i++){
			zeros[i]=0;
		}
		
		for(int i=0;i<this.vectorSize;i++){
			if(!Util.equalsByte(this.instance[i].idSum,zeros)||
			!Util.equalsByte(this.instance[i].hashSum,zeros)||
			this.instance[i].count!=0){
				//bw.append("failed to decode IBF");
				//bw.newLine();
				//bw.flush();
				//bw.append("$Failed, DECODE: size: A-B: "+DAMinusB.size()+", B-A: "+DBMinusA.size());
				//bw.newLine();
				//bw.flush();
				
				return false;
			}
		}
		
		//bw.append("$Success, DECODE: size: A-B: "+DAMinusB.size()+", B-A: "+DBMinusA.size());
		//bw.newLine();
		//bw.flush();
		return true;
	}
	
	/**
	 * all zeros
	 * @return
	 */
	private boolean testAllZeros() {
		// TODO Auto-generated method stub
		for(int i=0;i<vectorSize;i++){
			log.debug("@ "+i+"= "+instance[i].count);
			if(instance[i].count!=0){
				return false;
			}
		}
		return true;
		
	}



	/**
	 * test the membership
	 */
	public boolean membershipTest(Key key) {
		// TODO Auto-generated method stub
		int[] h = hash.hash(key);
	    hash.clear();
	    for(int i = 0; i < nbHash; i++) {
	    	//System.out.println("$: "+h[i]+", "+bits.get(h[i]));
	      if(instance[i].count<=0) {
	        return false;
	      }
	    }
	    return true;
	}

	@Override
	public void and(Filter filter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void or(Filter filter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void xor(Filter filter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void not() {
		// TODO Auto-generated method stub
		
	}

	public int getSize(){
		int count=0;
		for(int i=0;i<this.instance.length;i++){
			count+=this.instance[i].Size();
			
		}
		return count;
		
	}

	public void clear() {
		// TODO Auto-generated method stub
		for(int i=0;i<this.instance.length;i++){
			this.instance[i]=null;
		}
		this.instance=null;
		this.hash=null;
		this.hashC=null;
	}


	/**
	 * copy the IBF
	 * @return
	 */
	public InvertedBloomFilter getCopy() {
		// TODO Auto-generated method stub
		InvertedBloomFilter ibf=new InvertedBloomFilter(this,
		this.vectorSize,this.nbHash,this.hashType);
		
		return ibf;
	}
}
