package edu.NUDT.control;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.sbon.async.Config;
import edu.harvard.syrah.sbon.async.EL;

/**
 * control the start and end of the collect
 * @author quanyongf
 *
 */
public class EndPointController {

	static Log log = new Log(EndPointController.class);
	
	static {

		// All config properties in the file must start with 'HSHer.'

		Config.read("RDA", System
				.getProperty("RDA.config", "config/RDA.cfg"));
	}
	
	/**
	 * private collector
	 */
	PassiveCollector pc;
	
	public EndPointController(){
		/*
		 * Create the event loop
		 */
		EL.set(new EL(Long.valueOf(Config.getConfigProps().getProperty(
				"sbon.eventloop.statedump", "600000")), Boolean.valueOf(Config
				.getConfigProps().getProperty("sbon.eventloop.showidle",
						"false"))));
		
		
		pc = new PassiveCollector();
	}
	

	
	/**
	 * start: ts
	 * end: ts, or reach the maximum count
	 * @param startTS
	 * @param endTS
	 */
	public void setupCollection(long startTS, long endTS){
		scheduleStart(startTS);
		scheduleEnd(endTS);
	}
	
	/**
	 * begin,
	 * @param clock
	 */
	private void scheduleStart(long clock){
		StartMeasureTask sm = new StartMeasureTask();
		sm.registerCB(clock,pc);
	}
	
	/**
	 * end
	 * @param clock
	 */
	private void scheduleEnd(long clock){
		EndMeasureTask sm = new EndMeasureTask();
		sm.registerCB(clock,pc);
	}
	
	public void terminate(){
		pc.close();
	}
	
	public static void main(String[] args){
		
		EndPointController test = new EndPointController();
		
		long now = System.currentTimeMillis();
		long delay = 100;
		//last time
		int period = 10000000;
		
		test.setupCollection(now+delay, now+delay+period);
		
		
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
}
