package edu.NUDT.control;

import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.EL;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.EL.Priority;

public class EndMeasureTask implements Task{
		
	@Override
	public void registerCB(long clock,PassiveCollector pc) {
		// TODO Auto-generated method stub
		EL.get().registerClockTimerCB(clock, new CB0() {
			@Override
			protected void cb(CBResult result) {
				// TODO Auto-generated method stub
				//request to remote nodes
				pc.terminateCollect();
		};
	},Priority.HIGH);
	}
	
}
