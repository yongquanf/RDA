package edu.NUDT.RDAComm;

import edu.harvard.syrah.sbon.async.comm.obj.ObjMessage;
import util.bloom.RDA.RDATableEntry;
import util.bloom.RDA.SimpleRDATableEntry;

public class SRDAResponseMsg extends ObjMessage {
	static final long serialVersionUID = 119L;

	public SimpleRDATableEntry[] _hashTable;
	
	public SRDAResponseMsg(SimpleRDATableEntry[] hashTable) {
				
		_hashTable=hashTable;	
	}
	
	public SRDAResponseMsg(){
		
	}
}