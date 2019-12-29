package edu.NUDT.RDAComm;

import edu.harvard.syrah.sbon.async.comm.AddressIF;
import edu.harvard.syrah.sbon.async.comm.obj.ObjMessage;

public class RDARequestMsg extends ObjMessage {

	static final long serialVersionUID = 19L;
	public boolean isA=false;
	public AddressIF from;

	public RDARequestMsg(AddressIF _from,boolean _isA) {

		from = _from;
		isA = _isA;
	}
	
	public RDARequestMsg(AddressIF _from) {

		from = _from;
		isA = false;
	};
}
