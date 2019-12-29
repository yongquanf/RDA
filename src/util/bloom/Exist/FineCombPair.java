package util.bloom.Exist;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Hashtable;

import util.bloom.Apache.Key;

public class FineCombPair {

	//measured in millisecond;System.currentTimeMillis()
	long Counter=0;
	double TS=0;
	//int keyCheck=0;
	long keySum=0;

	Hashtable<Long,Double> keyValues;
	//byte[] valueSum=null;
	
	//public static int N_HASHCHECK = 11;
	//static int  maxHashValue = Integer.MAX_VALUE;
	//public static HashFunction hashC=new HashFunction(maxHashValue, 1, Hash.MURMUR_HASH);
	
	//public static int valueArrayLen=8;
	
	public Hashtable<Long,Double> getFromTables(){
		return keyValues;
	}
	/**
	 * initialize
	 */
	public FineCombPair(){
		Counter=0;
		TS = 0;	
		keySum=0;
		
		 keyValues = new Hashtable<Long,Double>();
	}
	
	public FineCombPair clone(){
		FineCombPair a =new FineCombPair();
		a.Counter=this.Counter;
		a.TS=this.TS;
		a.keySum=this.keySum;
		a.keyValues.putAll(this.keyValues);
		return a;
	}
	
	/**
	 * transform a long to a byte array
	 * @param number
	 * @return
	 */
	public static byte[] ToByteArray(long number){
		byte[] v = new byte[8];
		for(int i=0;i<v.length;i++){
			v[i]=(byte) ((number >>i*8) & 0xff);			
		}
		return v;
	}
	
	/**
	 * average TS
	 * @return
	 */
	public double getAvgTS(){
		long Num=getNumItems();
		if(Num>0){
		return TS/(0.0+Num);
		}else{
			return -1;
		}
	}
	
	public void insert(long id,double ts){
		this.keySum ^=FineComb.sHash64.hash(ByteBuffer.allocate(8).putLong(id).array());
		this.Counter++;
		this.TS+=ts;
		
		this.keyValues.put(id, ts);
	}
	
	
	public void remove(long id,double ts){
		this.keySum ^=FineComb.sHash64.hash(ByteBuffer.allocate(8).putLong(id).array());;
		this.Counter--;
		this.TS-=ts;
		
		this.keyValues.remove(id);
	}
	
	/**
	 * get the number of items
	 * @return
	 */
	public long getNumItems(){
		
		//long tick = Math.round(this.TS/(System.currentTimeMillis()+0.0));
		return Math.abs(Counter);
	}
	
	public boolean empty(){
		long count = getNumItems();
		return count ==0 && keySum==0;
	}
	
	public String toString(){
		return "Counter: "+Counter+",TS: "+TS+",keySum: "+keySum;
	}
}
