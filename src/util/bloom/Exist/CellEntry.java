package util.bloom.Exist;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import util.async.Util;
import util.async.Writable;
import edu.harvard.syrah.prp.Log;

public class CellEntry  implements Writable ,Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -3293661928992299135L;

	static Log log =new Log(CellEntry.class);
	
	int count=0;
	byte[] idSum=null;
	byte[] hashSum=null;
	int len=4;
	
	public CellEntry(){
		this.count=0;
		this.idSum=new byte[4];
		this.hashSum=new byte[4];
		for(int i=0;i<len;i++){
			this.idSum[i]=0;
			this.hashSum[i]=0;
		}
	}
	

	/**
	 * constructor
	 * @param cellIBF
	 */
	public CellEntry(CellEntry cellIBF) {
		this.count=cellIBF.count;
		this.idSum=new byte[4];
		this.hashSum=new byte[4];
		for(int i=0;i<len;i++){
			this.idSum[i]=cellIBF.idSum[i];
			this.hashSum[i]=cellIBF.hashSum[i];
		}
	}



	public void add(byte[] keyVal, byte[] c) {
		// TODO Auto-generated method stub
		/*log.debug("before Added\ncount: "+this.count.toString()+", idSum: "+this.idSum.toString()
				+", hashSum: "+this.hashSum.toString());
		log.debug("s: "+keyVal+", c: "+c);*/
		idSum=Util.xor(idSum, keyVal);
		hashSum=Util.xor(hashSum,c);
		count=count+1;
		/*log.debug("after Added\ncount: "+this.count.toString()+", idSum: "+this.idSum.toString()
				+", hashSum: "+this.hashSum.toString());*/
	}

	/**
	 * set the cell
	 * @param counter_
	 * @param keyVal
	 * @param c
	 */
	public void set(int counter_, byte[] keyVal, byte[] c) {
		// TODO Auto-generated method stub
		/*log.debug("before Added\ncount: "+this.count.toString()+", idSum: "+this.idSum.toString()
				+", hashSum: "+this.hashSum.toString());
		log.debug("s: "+keyVal+", c: "+c);*/
		for(int i=0;i<len;i++){
			idSum[i]=keyVal[i];
			hashSum[i]=c[i];	
		}
		
		count=counter_;
		/*log.debug("after Added\ncount: "+this.count.toString()+", idSum: "+this.idSum.toString()
				+", hashSum: "+this.hashSum.toString());*/
	}
	
	
	/**
	 * set the cell
	 * @param cellIBF
	 * @param cellIBF2
	 */
	public void set(CellEntry cellIBF, CellEntry cellIBF2) {
		// TODO Auto-generated method stub
		/*log.debug("before subtraction\ncount: "+cellIBF.count.toString()+", idSum: "+cellIBF.idSum.toString()
				+", hashSum: "+cellIBF.hashSum.toString());
		log.debug("\ncount: "+cellIBF2.count.toString()+", idSum: "+cellIBF2.idSum.toString()
				+", hashSum: "+cellIBF2.hashSum.toString());*/
		
		count=cellIBF.count-cellIBF2.count;
		idSum=Util.xor(cellIBF.idSum, cellIBF2.idSum);
		hashSum=Util.xor(cellIBF.hashSum,cellIBF2.hashSum);
		/*log.debug("after subtraction\ncount: "+this.count.toString()+", idSum: "+this.idSum.toString()
				+", hashSum: "+this.hashSum.toString());*/
	}
	

	
	/**
	 * reset the cell
	 */
	public void reset(){
		this.idSum=new byte[4];
		this.hashSum=new byte[4];
		for(int i=0;i<len;i++){
			this.idSum[i]=0;
			this.hashSum[i]=0;
		}
		this.count=0;
	}

	
	public CellEntry getCopy(){
		
		CellEntry t=new CellEntry(this);
		return t;
	}

	public int Size() {
		// TODO Auto-generated method stub
		return (4+8)*8;
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
