package util.bloom.Exist;

import java.nio.ByteBuffer;

public class LDAPair {

	long counter;
	double timestampSum;
	long keySum=0;
	
	
	public LDAPair(){
		this.counter=0;
		this.timestampSum=0;
	}

	public void insert(long id,double ts){
		this.counter++;
		this.timestampSum+=ts;
		this.keySum ^=FineComb.sHash64.hash(ByteBuffer.allocate(8).putLong(id).array());
	}
	public void remove(long id,double ts){
		this.counter--;
		this.timestampSum-=ts;
		this.keySum ^=FineComb.sHash64.hash(ByteBuffer.allocate(8).putLong(id).array());
	}
}
