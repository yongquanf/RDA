package edu.NUDT.RDAComm;

import edu.harvard.syrah.sbon.async.comm.obj.ObjMessage;
import util.bloom.RDA.RDATableEntry;

public class RDAFourResponseMsg extends ObjMessage {
	static final long serialVersionUID = 19L;

	public RDATableEntry[] _hashTable;
	
	public RDAFourResponseMsg(RDATableEntry[] hashTable) {
				
		_hashTable=hashTable;	
	}
	
	public RDAFourResponseMsg(){
		
	}
}