package util.bloom.Exist;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import util.async.Writable;
import util.bloom.Apache.HashFunction;
import util.bloom.Apache.Key;
import util.bloom.Apache.Hash.Hash;
import util.bloom.Apache.Hash.hashing.LongHashFunction;

public class IBLTEntry  implements Writable ,Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 9152232023861924227L;
	int count;
	long keyCheck;
	long keySum;
	byte[] valueSum;
	
	public static int N_HASHCHECK = 11111;
	static int  maxHashValue = Integer.MAX_VALUE;
	public static int seed0 = 765432;
	//public static HashFunction hashC=new HashFunction(maxHashValue, 1, Hash.MURMUR_HASH);
	//public static LongHashFunction hashC=LongHashFunction.xx(seed0);
	
	public static int seed1 = 45678;
	public static int seed2 = 54321;
	//hashPos
	//public static LongHashFunction LongHashFunction4PosHash=LongHashFunction.xx(seed1);
	public static LongHashFunction LongHashFunction4PKeyHash=LongHashFunction.xx(seed2);
	
	public static int valueArrayLen=8;
	/**
	 * initialize
	 */
	public IBLTEntry(){
		count = 0;
		keyCheck = 0;
		keySum=0;
		valueSum = IBLTEntry.ToByteArray(0);

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
	 * pure entry
	 * @return
	 */
	public boolean isPure(){
		if((this.count==1 || this.count==-1)){
			
			long h = LongHashFunction4PKeyHash.hashLong(keySum);//,N_HASHCHECK);
			return h==keyCheck;
		}
		return false;
	}
	public boolean empty(){
		return count ==0 && keySum==0 && keyCheck == 0;
	}
	
	/**
	 * add value
	 * @param v
	 */
	public void addValue(byte[] v){
		if(v==null){
			System.err.println("value is null");
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
		for(int i=0;i<this.valueSum.length;i++){
			//System.out.print("Old: "+this.valueSum[i]);
			valueSum[i] ^=v[i];
			//System.out.print(", new: "+this.valueSum[i]);
		}
		//System.out.print("\n");
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
