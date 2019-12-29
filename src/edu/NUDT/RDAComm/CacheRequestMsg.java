package edu.NUDT.RDAComm;

import java.util.HashSet;
import java.util.Set;

import edu.harvard.syrah.sbon.async.comm.AddressIF;
import edu.harvard.syrah.sbon.async.comm.obj.ObjMessage;

public class CacheRequestMsg extends ObjMessage {

	static final long serialVersionUID = 220L;
	public AddressIF from;
	public boolean isA=false;
	public Set<Long> ids;

	public CacheRequestMsg(AddressIF _from, Set<Long> _ids) {

		from = _from;
		ids = new HashSet<Long>(_ids);
	};
	
	public CacheRequestMsg(AddressIF _from, Set<Long> _ids,boolean _isA) {

		from = _from;
		ids = new HashSet<Long>(_ids);
		isA = _isA;
	};
}
