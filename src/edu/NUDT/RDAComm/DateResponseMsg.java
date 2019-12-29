package edu.NUDT.RDAComm;


import java.time.ZonedDateTime;

import edu.harvard.syrah.sbon.async.comm.obj.ObjMessage;

public class DateResponseMsg extends ObjMessage {

	static final long serialVersionUID = 299L;
	public long  commonDate;

	public DateResponseMsg(long me) {
		commonDate = me;
	}
}
