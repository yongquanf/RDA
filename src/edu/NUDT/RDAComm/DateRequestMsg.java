package edu.NUDT.RDAComm;


import java.time.ZonedDateTime;

import edu.harvard.syrah.sbon.async.comm.obj.ObjMessage;

public class DateRequestMsg extends ObjMessage {

	static final long serialVersionUID = 299L;
	public long date;
	public long myNearestStartDate;
	public DateRequestMsg(long _myNearestStartDate, long me) {
		date = me;
		myNearestStartDate = _myNearestStartDate;
	}
}
