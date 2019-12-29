package edu.NUDT.control;

import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.EL.Priority;
import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.EL;

public class StartMeasureTask  implements Task{
		
	@Override
	public void registerCB(long clock,PassiveCollector pc) {
		// TODO Auto-generated method stub
		EL.get().registerClockTimerCB(clock, 
				new CB0() {
			@Override
			protected void cb(CBResult result) {
				// TODO Auto-generated method stub
				//request to remote nodes
				pc.restartCollect();
				pc.startCollect();
		}}, Priority.HIGH);
	}

}
