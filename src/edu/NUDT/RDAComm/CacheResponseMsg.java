package edu.NUDT.RDAComm;

import java.util.Hashtable;

import edu.harvard.syrah.sbon.async.comm.obj.ObjMessage;

public class CacheResponseMsg extends ObjMessage {
	static final long serialVersionUID = 220L;

	public Hashtable<Long,Double> _hashTable;
	
	public CacheResponseMsg(Hashtable<Long,Double> hashTable) {
				
		_hashTable= hashTable;//new Hashtable<Long,Double>(hashTable);	
	}
	
	public CacheResponseMsg(){
		
	}
}