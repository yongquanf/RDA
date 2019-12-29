package edu.NUDT.control;

import java.time.ZonedDateTime;

import org.pcap4j.packet.factory.PacketFactories;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.Config;
import edu.harvard.syrah.sbon.async.EL;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;
import edu.harvard.syrah.sbon.async.EL.Priority;
import edu.harvard.syrah.sbon.async.comm.AddressFactory;
import util.bloom.RDA.PassiveRDAHost;

/**
 * control the start and end of the collect
 * @author quanyongf
 *
 */
public class EndPointControllerPeriod {

	static Log log = new Log(EndPointControllerPeriod.class);
	
	final static Logger logger
    = LoggerFactory.getLogger(EndPointControllerPeriod.class);

	/**
	 * private collector
	 */
	public static PassiveCollectorPeriod pc;
	/**
	 * set trigger
	 */
	//public void setTrigger(){
	//	if(pc!=null&&pc.RDAHost!=null){
	//		pc.RDAHost.setTrigger(pc);
	//	}
	//}
	
	public EndPointControllerPeriod(CB0 cbDone){
		/*
		 * Create the event loop
		 */
		EL.set(new EL(Long.valueOf(Config.getConfigProps().getProperty(
				"sbon.eventloop.statedump", "600000")), Boolean.valueOf(Config
				.getConfigProps().getProperty("sbon.eventloop.showidle",
						"false"))));
		
		pc = new PassiveCollectorPeriod(new CB0(){
			@Override
			protected void cb(CBResult result) {
				// TODO Auto-generated method stub
				
				switch (result.state) {
				case OK: {
					log.main("measure sync");
					cbDone.call(result);
					break;
				}
				case TIMEOUT:
				case ERROR: {
					log.error("Could not resolve  address: " + result.what);
					
					cbDone.call(result);
					break;
				}
				}

				}
			
		});		
					
	}
	

//	
//	/**
//	 * start: ts
//	 * end: ts, or reach the maximum count
//	 * @param startTS
//	 * @param endTS
//	 */
//	public void setupCollection(long startTS, long endTS,CB0 cbDone){
//		scheduleStart(startTS);
//		
//		scheduleEnd(endTS,cbDone);
//	}
//	
//	/**
//	 * restart
//	 */
//	public void restartCollection(CB0 cbDone){
//			
//		if(PassiveRDAHost.isActive){
//			EL.get().registerTimerCB(restartMeasureInterval, new CB0(){
//				protected void cb(CBResult result) {
//				// TODO Auto-generated method stub						
//				//passive
//				setupMeasureSync(new CB0(){
//					@Override
//					protected void cb(CBResult result) {
//						// TODO Auto-generated method stub
//						cbDone.call(result);
//					}										
//				});	
//				
//			}
//			
//		});
//	}
//	}
//	
//	/**
//	 * long now = System.currentTimeMillis();
//				long delay = 1000;
//				//last time
//				long period = measurementPeriod;				
//				setupCollection(now+delay, now+delay+period);
//	 */
//	
//	/**
//	 * begin,
//	 * @param clock
//	 */
//	private void scheduleStart(long clock){
//		StartMeasureTask sm = new StartMeasureTask();
//		sm.registerCB(clock,pc);
//	}
//	
//	/**
//	 * end
//	 * @param clock
//	 */
//	private void scheduleEnd(long clock, CB0 cbDone){
//		EL.get().registerClockTimerCB(clock, new CB0() {
//			@Override
//			protected void cb(CBResult result) {
//				// TODO Auto-generated method stub
//				//request to remote nodes
//				pc.terminateCollect(new CB0(){
//					@Override
//					protected void cb(CBResult result) {
//						// TODO Auto-generated method stub
//						restartCollection(new CB0(){
//							@Override
//							protected void cb(CBResult result) {
//								// TODO Auto-generated method stub
//								cbDone.call(result);
//							}
//							
//						});
//					}
//				});
//				
//		};
//	},Priority.HIGH);
//	}
//	
//	public void terminate(){
//		pc.close();
//	}
//	
	public static void main(String[] args){
		
			
		
	EndPointControllerPeriod test = new EndPointControllerPeriod(new CB0(){
			@Override
			protected void cb(CBResult result) {
				// TODO Auto-generated method stub				
				log.main(result.toString());
				switch (result.state) {
				case OK: {
					log.main("measure sync");
					//not closed
					break;
				}
				case TIMEOUT:
				case ERROR: {
					log.error("Could not resolve  address: " + result.what);										
					break;
				}
				}
				
				
			}						
		});
		
		
		//long now = System.currentTimeMillis();
		//long delay = 1000;
		//last time
		//int period = 10000;
		
		//test.setupCollection(now+delay, now+delay+measurementPeriod);
		
		
		try {
			EL.get().main();
		} catch (OutOfMemoryError e) {
			EL.get().dumpState(true);
			e.printStackTrace();
			log.error("Error: Out of memory: " + e);
		}

		System.out.println("Shutdown");
		System.exit(0);
	}


//	/**
//	 * set up between two users
//	 */
//	private void setupMeasureSync(CB0 cbDone) {
//		// TODO Auto-generated method stub
//		//last timer
//		log.main("set up sync");		
//		pc.RDAHost.syncDate(PassiveRDAHost.target[0], new CB1<Long>(){
//			@Override
//			protected void cb(CBResult result, Long commonDate0) {
//				// TODO Auto-generated method stub	
//				log.main("sync acked");
//				//monotone, timestamp
//				long commonDate = Math.max(pc.RDAHost.lastpassiveCommonStartDate,commonDate0);
//							
//				log.main("my timer: "+pc.RDAHost.myNearestStartDate+", commonDate: "+commonDate);
//				pc.RDAHost.myNearestStartDate=commonDate;
//				setupCollection(commonDate,commonDate+measurementPeriod,cbDone);			
//
//				/*
//				if(pc.RDAHost.myNearestStartDate!=commonDate){
//					//update
//					pc.RDAHost.myNearestStartDate=commonDate;					
//					//set up collection
//									}else{
//					log.main("init postponed");
//					cbDone.call(result);
//				}*/
//				
//			}
//			
//		});
//	}
}
