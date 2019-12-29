package edu.NUDT.RDAComm;

import java.util.HashSet;
import java.util.Set;

import edu.harvard.syrah.sbon.async.comm.AddressIF;
import edu.harvard.syrah.sbon.async.comm.obj.ObjMessage;
import util.bloom.Apache.BloomFilter;
import util.bloom.Apache.CountingBloomFilter;
import util.bloom.Apache.Filter;

public class MissingNodeTSRequestMsg2 extends ObjMessage {

	static final long serialVersionUID = 20L;
	public AddressIF from;
	public boolean isA=false;
	public Set<Long> ids;
	//public CountingBloomFilter bf;
	
	public MissingNodeTSRequestMsg2(AddressIF _from, Set<Long> _ids) {

		from = _from;
		ids = _ids;
	};
	
	public MissingNodeTSRequestMsg2(AddressIF _from, Set<Long> _ids,boolean _isA) {

		from = _from;
		ids = _ids;
		isA = _isA;
	};
}
