package util.bloom.RDA;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

import edu.harvard.syrah.prp.Log;
import util.async.Writable;
import util.bloom.Apache.Key;
import util.bloom.Exist.FineComb;

public class SimpleRDATableEntry  implements Writable ,Serializable{

	static Log log  = new Log(SimpleRDATableEntry.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 4729804739772463166L;
	//measured in millisecond;System.currentTimeMillis()
	public int Counter=0;
	public float TS=0;
	//long keyCheck = 0;
	public long keySum = 0;
	//byte[] valueSum=null;
	/**
	 * items
	 */
	//Hashtable<Long,Double> items=new Hashtable<Long,Double>();
	//signed ts
	//public double STS=0;
	
	//public static int N_HASHCHECK = 11;
	//static int  maxHashValue = Integer.MAX_VALUE;
	//public static HashFunction hashC=new HashFunction(maxHashValue, 1, Hash.MURMUR_HASH);
	
	//public static int valueArrayLen=8;
	/**
	 * initialize
	 */
	public SimpleRDATableEntry(){
		Counter=0;
		TS = 0;
		//keyCheck =0;
		keySum=0;
		//valueSum = RDATableEntry.ToByteArray(0);
		//STS=0;
		//items.clear();
	}
	
	public SimpleRDATableEntry clone(){
		SimpleRDATableEntry a =new SimpleRDATableEntry();
		a.Counter=this.Counter;
		a.TS=this.TS;
		//a.STS=this.STS;
		a.keySum=this.keySum;
		//a.keyCheck=this.keyCheck;
		//a.valueSum=Arrays.copyOf(valueSum, valueSum.length);
		//a.items.putAll(this.items);
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
	
	/**
	 * get the number of items
	 * @return
	 */
	public long getNumItems(){
		
		//long tick = Math.round(this.TS/(System.currentTimeMillis()+0.0));
		return Math.abs(Counter);
	}
	

	
	public SimpleRDATableEntry XORHashTable(SimpleRDATableEntry you){
		
		SimpleRDATableEntry me=this.clone();
		
		//log.main(you.TS+", "+me.TS);
		me.TS-=you.TS;
		me.Counter-=you.Counter;
		//me.STS-=you.STS;
		//me.keyCheck^=you.keyCheck;
		me.keySum^=you.keySum;
	//	me.addValue(you.valueSum);
		/*Iterator<Entry<Long, Double>> ier = me.items.entrySet().iterator();
		while(ier.hasNext()){
			Entry<Long, Double> tmp = ier.next();
			if(you.items.containsKey(tmp.getKey())){
				ier.remove();
			}
		}*/
		return me;
	}
	
	
	
	
	public boolean isPureOld(long keyCheck){
		
		long tick = getNumItems();
		
		if((tick==1 || tick==-1)){
			
		//int[] h = HashTableEntry.hashC.hash(new Key(HashTableEntry.ToByteArray(keySum)),HashTableEntry.N_HASHCHECK);
		
		return RDATable.LongHashFunction4PKeyHash.hashLong(keySum) == keyCheck;	
		//return FineComb.sHash64.hash(ByteBuffer.allocate(8).putLong(keySum).array())==keyCheck;
		}
		return false;
		
	}
	
	public boolean empty(){
		long count = getNumItems();
		return count ==0 && keySum==0;// && keyCheck == 0;
	}
	
	/**
	 * add value
	 * @param v
	 */
	public void addValue(byte[] v){
		if(v==null){
			//System.err.println("value is null");
			return;
		}
//		if(this.valueSum==null){
//			this.valueSum = ByteBuffer.allocate(v.length).putLong(0).array();
//		}
//		if(this.valueSum.length<v.length){
//		byte[] v1=new byte[v.length];
//		for(int i=0;i<this.valueSum.length;i++){
//			v1[i]=this.valueSum[i];
//		}
//		this.valueSum = v1;
//		}		
	//	for(int i=0;i<this.valueSum.length;i++){
			//System.out.print("Old: "+this.valueSum[i]);
	//		valueSum[i] ^=v[i];
			//System.out.print(", new: "+this.valueSum[i]);
	//	}
		//System.out.print("\n");
	}
	
	public String toString(){
		return "Counter: "+Counter+",TS: "+TS+",keySum: "+keySum;//+",keyCheck: "+keyCheck;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}

	/**
	 * reset
	 */
	public void reset() {
		// TODO Auto-generated method stub
		this.Counter=0;
		this.TS=0;
		this.keySum=0;
	}
}
