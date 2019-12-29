package edu.NUDT.RDAComm;

import java.util.Hashtable;

import edu.harvard.syrah.sbon.async.comm.obj.ObjMessage;

public class MissingResponseMsg extends ObjMessage {
	static final long serialVersionUID = 20L;

	public Hashtable<Long,Double> _hashTable;
	
	public MissingResponseMsg(Hashtable<Long,Double> hashTable) {
				
		_hashTable= hashTable;//new Hashtable<Long,Double>(hashTable);	
	}
	
	public MissingResponseMsg(){
		
	}
}