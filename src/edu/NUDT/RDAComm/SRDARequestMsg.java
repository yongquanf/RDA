package edu.NUDT.RDAComm;

import edu.harvard.syrah.sbon.async.comm.AddressIF;
import edu.harvard.syrah.sbon.async.comm.obj.ObjMessage;

public class SRDARequestMsg extends ObjMessage {

	static final long serialVersionUID = 119L;
	public boolean isA=false;
	public AddressIF from;

	public SRDARequestMsg(AddressIF _from,boolean _isA) {

		from = _from;
		isA = _isA;
	}
	
	public SRDARequestMsg(AddressIF _from) {

		from = _from;
		isA = false;
	};
}
