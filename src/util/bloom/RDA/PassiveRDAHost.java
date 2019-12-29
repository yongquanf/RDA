package util.bloom.RDA;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import edu.NUDT.RDAComm.CacheRequestMsg;
import edu.NUDT.RDAComm.CacheResponseMsg;
import edu.NUDT.RDAComm.DateRequestMsg;
import edu.NUDT.RDAComm.DateResponseMsg;
import edu.NUDT.RDAComm.MissingNodeTSRequestMsg2;
import edu.NUDT.RDAComm.MissingResponseMsg;
import edu.NUDT.RDAComm.RDARequestMsg;
import edu.NUDT.RDAComm.RDAResponseMsg;
import edu.NUDT.RDAComm.SRDARequestMsg;
import edu.NUDT.RDAComm.SRDAResponseMsg;
import edu.NUDT.control.EndPointControllerPeriod;
import edu.NUDT.control.PassiveCollectorPeriod;
import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.POut;
import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.Config;
import edu.harvard.syrah.sbon.async.EL;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;
import edu.harvard.syrah.sbon.async.comm.AddressIF;
import edu.harvard.syrah.sbon.async.comm.NetAddress;
import edu.harvard.syrah.sbon.async.comm.obj.ObjComm;
import edu.harvard.syrah.sbon.async.comm.obj.ObjCommCB;
import edu.harvard.syrah.sbon.async.comm.obj.ObjCommIF;
import edu.harvard.syrah.sbon.async.comm.obj.ObjCommRRCB;
import edu.harvard.syrah.sbon.async.comm.obj.ObjMessage;
import edu.harvard.syrah.sbon.async.comm.obj.ObjMessageIF;
import edu.harvard.syrah.sbon.async.comm.obj.ObjUDPComm;
import util.async.HashMapCache;
import util.bloom.Apache.GeneralHashFunction;
import util.bloom.Apache.Hash.Hash;
import util.bloom.Apache.Hash.MurmurHashV2;
import util.bloom.Exist.FineComb;


public class PassiveRDAHost {

	static Log log=new Log(PassiveRDAHost.class);


  	public static int seed=1256422325;
  	
  	public static Random rd;
  	
  	//pseudo header
  	public static long increaser = -1;
  	
	// RDA measurement host
	public SimpleRDAMeasurementPoint _hostRDA=null;

	public SimpleRDAMeasurementPointSRDA _hostSRDA=null;

	
	public static int Repeat_DELAY = 1000;

	// tick control
	final CB0 tickControlRDA;	

	
	public ObjCommIF comm = null;
	public static	AddressIF me;
	/**
	 * passive collector
	 */
	//public PassiveCollectorPeriod pc =null;
	/**
	 * whether to be passive only
	 */
	//public volatile boolean closedMyDateSync = false;
	//public EndPointControllerPeriod trigger = null;
	
	public void setTrigger(EndPointControllerPeriod _trigger){
		EndPointControllerPeriod trigger = _trigger;
	}
	
	//constructor
	public PassiveRDAHost(ObjCommIF _comm){
		rd = new Random(System.currentTimeMillis());
		increaser = 1;
		//comm = _comm;	
		_hostRDA = new SimpleRDAMeasurementPoint(PassiveCollectorPeriod.RDAEntries,PassiveCollectorPeriod.TotalPackets,PassiveCollectorPeriod.hashFuncNum);	
		
		_hostSRDA = new SimpleRDAMeasurementPointSRDA(PassiveCollectorPeriod.RDAEntries,PassiveCollectorPeriod.TotalPackets,PassiveCollectorPeriod.hashFuncNum);	
		
		//repeat the request
		tickControlRDA = new CB0() {
			@Override
			protected void cb(CBResult result) {
				// TODO Auto-generated method stub
				//request to remote nodes
				RemoteRDAComputeTest(new  CB0(){
					@Override
					protected void cb(CBResult result) {
						// TODO Auto-generated method stub					
						log.main(result.toString());
					}						
				});}};
			
	}
	
	//constructor
	public PassiveRDAHost() {
		
		// TODO Auto-generated constructor stub
	rd = new Random(System.currentTimeMillis());	
	increaser = 1;
	//RDA initialize	
	_hostRDA = new SimpleRDAMeasurementPoint(PassiveCollectorPeriod.RDAEntries,PassiveCollectorPeriod.TotalPackets,PassiveCollectorPeriod.hashFuncNum);	
	
	_hostSRDA = new SimpleRDAMeasurementPointSRDA(PassiveCollectorPeriod.RDAEntries,PassiveCollectorPeriod.TotalPackets,PassiveCollectorPeriod.hashFuncNum);	
	
	//repeat the request
	//repeat the request
	tickControlRDA = new CB0() {
		@Override
		protected void cb(CBResult result) {
			// TODO Auto-generated method stub
			//request to remote nodes
			RemoteRDAComputeTest(new  CB0(){
				@Override
				protected void cb(CBResult result) {
					// TODO Auto-generated method stub					
					log.main(result.toString());
				}						
			});}};
		
	
	}

	
	/**
	 * read the trace
	 * return the target
	 */
	public void parseRecordsCacheRDA(AbstractMap<Long,Long> SenderTSTable){		
		 _hostRDA.packetIncoming(SenderTSTable);    
		 _hostRDA.isParsed=true;
	}
	
	public void parseRecordsSRDACacheRDA(AbstractMap<Long,Long> SenderTSTable){		
		 _hostSRDA.packetIncoming(SenderTSTable);    
		 _hostSRDA.isParsed=true;
	}

	/**
	 * parse a packet record
	 * @param id
	 * @param ts
	 */
	public void parseRecord(Long id,long ts){
		_hostRDA.packetIncoming(id, ts);
		_hostSRDA.packetIncoming(id, ts);
	}
	
	/**
	 * parse the ts  
	 * @param str
	 * @param id
	 * @param TS
	 */
	public static void  parseTimeStamp(String str,long[]id,double[]TS){
			StringTokenizer st=new StringTokenizer(str);
			
			int count=0;
	       // BigDecimal ts= BigDecimal.valueOf(0);
			double ts=0;
            long seq=0,ack=0,check=0,checksum=0,packetLen;
	        String client="";
	        String server="";
	        String direction="";
			StringBuffer sb = new StringBuffer();
			
	        while (st.hasMoreElements() ){
	      	  String rec=st.nextToken();
			if(count==0){
	    		//obtain timestamp, ts
	    		  //substring
	    		  //ts=new BigDecimal(rec.substring(8));
				//ts=new BigDecimal(rec);
				ts=Double.parseDouble(rec);

	    	  }else if(count==1){
	    		  seq=Long.parseLong(rec);
	    		  sb.append(seq);
	    	  }else if(count==2){
	    		  ack=Long.parseLong(rec);
	    		  sb.append(ack);
	    	  }else if(count==3){
	    		  check=Long.parseLong(rec);
	    		  sb.append(check);
	    	  }else if(count==4){
	    		  client=(rec);
	    		  sb.append(client);
	    	  }else if(count==5){
	    		continue;  
	    	  }else if(count==6){
	    		  server=(rec);
	    		  sb.append(server);
	    	  }else if(count ==7){
	    		  direction = rec;
	    		  sb.append(direction);
	    	  }        	  
	    	  count ++;
    	  }
	        //no header
	       if(count ==1){
	    	   log.main("no header info, we use pseudo seq instead");
	    	   sb.append(increaser);
	    	   increaser++;
	       }
	        
	          try {
	        	  //string
	        	str = sb.toString();
				id[0] = MapFromTCP2HashID(str);
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	          TS[0] = ts;
				}

	 /** Hashing algorithm to use. */
	 // private static Hash hashFunction=Hash.getInstance(Hash.MURMUR_HASH);
	  
		/**
		 * TCP: SRCip:port,DSTip:port,seqNumber
		 * @param TCPHeader
		 * @return
		 * @throws UnsupportedEncodingException 
		 */
		public static long MapFromTCP2HashID(String Header) throws UnsupportedEncodingException{
			
			//return MurmurHashV2.hash64(Header);
			//hybrid hash function, xor
			//return (MurmurHashV2.hash64(Header)&0xffffffff)^(GeneralHashFunction.DJBHash(Header));
			return FineComb.sHash64.hash(Header);
			
			//
			//return  (FineComb.sHash64.hash(Header));
			//ByteBuffer sendBuffer=ByteBuffer.wrap((TCPHeader+sequenceNumber).getBytes("UTF-16"));			
			//return new Long(sendBuffer.array());
			
		}
	
		
		public static long MapFromTCP2HashID(byte[] Header) throws UnsupportedEncodingException{
			//if(false){
				//return MurmurHashV2.hash64(Header,Header.length);
				
			//}
			return FineComb.sHash64.hash(Header);
			//
			//return  (FineComb.sHash64.hash(Header));
			//ByteBuffer sendBuffer=ByteBuffer.wrap((TCPHeader+sequenceNumber).getBytes("UTF-16"));			
			//return new Long(sendBuffer.array());
			
		}
	
	
	public void registerRepeatRDATestTimer(){
		long delay = Repeat_DELAY;

		// log.debug("setting timer to " + delay);
		EL.get().registerTimerCB(delay, tickControlRDA);
	}

	

	/**
	 * remote call
	 * @param cbDone
	 */
	public void RemoteRDAComputeTest(CB0 cbDone){
	
		doRDARequestRDA(PassiveCollectorPeriod.target[0],  new  CB0(){
		@Override
		protected void cb(CBResult result1) {
			// TODO Auto-generated method stub
			//registerRepeatRDATestTimer();
			//cbDone.call(result);
			switch (result1.state) {
			case OK: {
				doRDARequestSRDA(PassiveCollectorPeriod.target[0],  new  CB0(){
					@Override
					protected void cb(CBResult result2) {
						// TODO Auto-generated method stub
						switch (result2.state) {
						case OK: {
							doDirectCompute(NetAddress.createUnresolved(PassiveCollectorPeriod.target[0], PassiveCollectorPeriod.COMM_PORT), cbDone);
							break;
							}
						case ERROR:
						case TIMEOUT:{
							cbDone.call(result2);
							break;
							}
					}
					}}
				);
			break;
			}
			case ERROR:
			case TIMEOUT:{
				cbDone.call(result1);
				break;
				}
			}
		}						
	}) ;
	}
	
	/*
	 * request the cache, compute directly
	 */
	public void doDirectCompute(AddressIF addr,final CB0 cbDone){
		
		/**
		 * my local cache
		 */
		Set<Long> ReceiverItems = _hostRDA.SenderTSTable.keySet();
		CacheRequestMsg msg2 = new CacheRequestMsg(
				me,ReceiverItems);
		
		long CacheMsgTS = System.nanoTime();
		comm.sendRequestMessage(msg2, addr,
				new ObjCommRRCB<CacheResponseMsg>() {

					@Override
					protected void cb(CBResult result, CacheResponseMsg argMiss,
							AddressIF arg2, Long arg3) {
						// TODO Auto-generated method stub
						switch (result.state) {
						case OK: {
							long CacheResponseTS=System.nanoTime();
							double delayCache=(CacheResponseTS-CacheMsgTS)/1000000.0;
							//return packets
							Hashtable<Long, Double> ReceiverTSTable=argMiss._hashTable;
							//repair
							long repairT1=System.nanoTime();
							double[] sta=_hostRDA.computeDirect(ReceiverTSTable);
							long repairT2=System.nanoTime();
						
							double eraseDelay = (repairT2 - repairT1)/1000000.0;
							
							//sendRDA,sendMiss
							log.main("direct: sendDelay: "+delayCache+", compute: "+eraseDelay+",  sta: "+POut.toString(sta));
							cbDone.call(result);
							break;
						}
						case TIMEOUT:
						case ERROR: {
							//failure
							cbDone.call(result);
							break;
						}
					}
					}
		}
			
		);
	}
	
	public void doRDARequestRDA(String target, final CB0 cbDone) {

		//registerRepeatRDATestTimer();
		
		final long time1 = System.currentTimeMillis();

		AddressIF addr=NetAddress.createUnresolved(target, PassiveCollectorPeriod.COMM_PORT);//, new CB1<AddressIF>() {
		
		
		
			//@Override
			//protected void cb(CBResult result, AddressIF addr) {
				// TODO Auto-generated method stub
			//	switch (result.state) {
			//	case OK: {
					log.main("doRDARequest: "+target);
					
					//AddressIF  neighbor = NetIPAddress.createUnresolved(target, COMM_PORT);					
					long time2 = System.currentTimeMillis();
					 double resolveAddrDelay = (time2-time1)/1000.00;
				if(true){
						
						//request RDA
						RDARequestMsg msg = new RDARequestMsg(me);
						final long sendStamp = System.nanoTime();

						//log.info("Sending gossip request to "+ neighbor);

						comm.sendRequestMessage(msg, addr,
								new ObjCommRRCB<RDAResponseMsg>() {
	

									@Override
									protected void cb(CBResult result, RDAResponseMsg arg1, AddressIF arg2, Long arg3) {
										// TODO Auto-generated method stub
										long receiveTS = System.nanoTime();
										switch (result.state) {
										case OK: {											
											
											//log.main("received RDA response: "+arg1._hashTable.length);
											//milli
											double delay =  (receiveTS - sendStamp)/1000000.0;
											
											RDATableEntry[] you = arg1._hashTable;
											//call compute
											RDATable receiver = RDATable.getRDATable(you);
											//ids
											HashSet<Long> SenderItems = new HashSet<Long>();
											HashSet<Long>  ReceiverItems= new HashSet<Long>();
											//decode
											long decodeTS = System.nanoTime();
											boolean decoded=_hostRDA.decodeSet(receiver, SenderItems, ReceiverItems);//.decodeSetDiffWithTime(receiver);
											long d2=System.nanoTime();
											
											log.main("Table: "+arg1._hashTable.length+", decoded: "+decoded+", "+SenderItems.size()+", "+ReceiverItems.size());
											
											double decodeDelay = (d2 - decodeTS)/1000000.0;
											//no need to repair
											if(SenderItems.isEmpty()&& ReceiverItems.isEmpty()){
												double[] sta = computeStatisticsRDA(receiver);												
												log.main("sendDelay: "+delay+", decodeDelay: "+decodeDelay+", sta: "+POut.toString(sta));
												cbDone.call(result);
												break;
											}else{
											//request the other's packets
											if(!ReceiverItems.isEmpty()){
												//request 
												MissingNodeTSRequestMsg2 msg2 = new MissingNodeTSRequestMsg2(
														me,ReceiverItems);
												long missMsgTS = System.nanoTime();
												comm.sendRequestMessage(msg2, addr,
														new ObjCommRRCB<MissingResponseMsg>() {

															@Override
															protected void cb(CBResult result, MissingResponseMsg argMiss,
																	AddressIF arg2, Long arg3) {
																// TODO Auto-generated method stub
																switch (result.state) {
																case OK: {
																	long missResponseTS=System.nanoTime();
																	double delayMiss=(missResponseTS-missMsgTS)/1000000.0;
																	//return packets
																	Hashtable<Long, Double> ReceiverTSTable=argMiss._hashTable;
																	//repair
																	long repairT1=System.nanoTime();
																	_hostRDA.repairDirect(SenderItems,receiver,ReceiverItems, ReceiverTSTable);
																	long repairT2=System.nanoTime();
																	double[] sta = computeStatisticsRDA(receiver);
																	long repairT3=System.nanoTime();
																	double eraseDelay = (repairT2 - repairT1)/1000000.0;
																	double computeDelay = (repairT3 - repairT2)/1000000.0;
																	//sendRDA,sendMiss
																	log.main("sendDelay: "+delay+", decodeDelay: "+
																	decodeDelay+", missRequestDelay: "+delayMiss+", eraseDelay: "+eraseDelay+", computeDelay: "+computeDelay+",  sta: "+POut.toString(sta));
																	cbDone.call(result);
																	break;
																}
																case TIMEOUT:
																case ERROR: {
																	//failure
																	cbDone.call(result);
																	break;
																}
															}
															}
												}
													
												);
											}else{
												//me repair only, no send
												if(!SenderItems.isEmpty()){
													//_host.repair(receiver,null);
													_hostRDA.repairDirect(SenderItems,receiver,ReceiverItems, null);
												}
												double[] sta = computeStatisticsRDA(receiver);
												log.main("sendDelay: "+delay+", decodeDelay: "+decodeDelay+", sta: "+POut.toString(sta));
								
												cbDone.call(result);
											}
											}
											//log.main("finished!");
											
											break;
										}
										case TIMEOUT:
										case ERROR: {
											
											//registerRepeatRDATestTimer();
											cbDone.call(result);
											break;
										}
										default:
											break;
										}
									}
									
								});
			
						
						

					}
		
	}
	
	/**
	 * SRDA request
	 * @param target
	 * @param cbDone
	 */
	public void doRDARequestSRDA(String target, final CB0 cbDone) {

		//registerRepeatRDATestTimer();
		
		final long time1 = System.currentTimeMillis();

		AddressIF addr=NetAddress.createUnresolved(target, PassiveCollectorPeriod.COMM_PORT);//, new CB1<AddressIF>() {
		
			
			//@Override
			//protected void cb(CBResult result, AddressIF addr) {
				// TODO Auto-generated method stub
			//	switch (result.state) {
			//	case OK: {
					log.main("doRDARequest: "+target);
					
					//AddressIF  neighbor = NetIPAddress.createUnresolved(target, COMM_PORT);					
					long time2 = System.currentTimeMillis();
					 double resolveAddrDelay = (time2-time1)/1000.00;
				if(true){
						
						//request RDA
						SRDARequestMsg msg = new SRDARequestMsg(me);
						final long sendStamp = System.nanoTime();

						//log.info("Sending gossip request to "+ neighbor);

						comm.sendRequestMessage(msg, addr,
								new ObjCommRRCB<SRDAResponseMsg>() {

									@Override
									protected void cb(CBResult result, SRDAResponseMsg arg1, AddressIF arg2, Long arg3) {
										// TODO Auto-generated method stub
										long receiveTS = System.nanoTime();
										switch (result.state) {
										case OK: {											
											
											//log.main("received RDA response: "+arg1._hashTable.length);
											//milli
											double delay =  (receiveTS - sendStamp)/1000000.0;
											
											SimpleRDATableEntry[] you = arg1._hashTable;
											//call compute
											SimpleRDATable receiver = SimpleRDATable.getRDATable(you);
											//ids
											HashSet<Long> SenderItems = new HashSet<Long>();
											HashSet<Long>  ReceiverItems= new HashSet<Long>();
											//decode
											log.main("SRDA decode:");
											long decodeTS = System.nanoTime();
											boolean decoded=_hostSRDA.decodeSet(receiver, SenderItems, ReceiverItems);//.decodeSetDiffWithTime(receiver);
											long d2=System.nanoTime();
											
											log.main("SRDA Table: "+arg1._hashTable.length+", decoded: "+decoded+", "+SenderItems.size()+", "+ReceiverItems.size());
											
											double decodeDelay = (d2 - decodeTS)/1000000.0;
											//no need to repair
											if(SenderItems.isEmpty()&& ReceiverItems.isEmpty()){
												double[] sta = computeStatisticsSRDA(receiver);												
												log.main("SRDA: sendDelay: "+delay+", decodeDelay: "+decodeDelay+", sta: "+POut.toString(sta));
												cbDone.call(result);
												break;
											}else{
											//request the other's packets
											if(!ReceiverItems.isEmpty()){
												//request 
												MissingNodeTSRequestMsg2 msg2 = new MissingNodeTSRequestMsg2(
														me,ReceiverItems);
												long missMsgTS = System.nanoTime();
												comm.sendRequestMessage(msg2, addr,
														new ObjCommRRCB<MissingResponseMsg>() {

															@Override
															protected void cb(CBResult result, MissingResponseMsg argMiss,
																	AddressIF arg2, Long arg3) {
																// TODO Auto-generated method stub
																switch (result.state) {
																case OK: {
																	long missResponseTS=System.nanoTime();
																	double delayMiss=(missResponseTS-missMsgTS)/1000000.0;
																	//return packets
																	Hashtable<Long, Double> ReceiverTSTable=argMiss._hashTable;
																	//repair
																	long repairT1=System.nanoTime();
																	_hostSRDA.repairDirect(SenderItems,receiver,ReceiverItems, ReceiverTSTable);
																	long repairT2=System.nanoTime();
																	double[] sta = computeStatisticsSRDA(receiver);
																	long repairT3=System.nanoTime();
																	double eraseDelay = (repairT2 - repairT1)/1000000.0;
																	double computeDelay = (repairT3 - repairT2)/1000000.0;
																	//sendRDA,sendMiss
																	log.main("SRDA: sendDelay: "+delay+", decodeDelay: "+
																	decodeDelay+", missRequestDelay: "+delayMiss+", eraseDelay: "+eraseDelay+", computeDelay: "+computeDelay+",  sta: "+POut.toString(sta));
																	cbDone.call(result);
																	break;
																}
																case TIMEOUT:
																case ERROR: {
																	//failure
																	cbDone.call(result);
																	break;
																}
															}
															}
												}
													
												);
											}else{
												//me repair only, no send
												if(!SenderItems.isEmpty()){
													//_host.repair(receiver,null);
													_hostSRDA.repairDirect(SenderItems,receiver,ReceiverItems, null);
												}
												double[] sta = computeStatisticsSRDA(receiver);
												log.main("SRDA: sendDelay: "+delay+", decodeDelay: "+decodeDelay+", sta: "+POut.toString(sta));
								
												cbDone.call(result);
											}
											}
											//log.main("finished!");
											
											break;
										}
										case TIMEOUT:
										case ERROR: {
											
											RemoteRDAComputeTest(new  CB0(){
												@Override
												protected void cb(CBResult result0) {
													// TODO Auto-generated method stub					
													cbDone.call(result);
												}						
											});
											
											//registerRepeatRDATestTimer();										
											break;
										}
										}
									}
									
								});
			
						
						

					}
		
	}
	/**
	 * directly compute statistics
	 * @param receiver
	 */
	public double[] computeStatisticsRDA(RDATable  receiver){
		long TS1 = System.nanoTime();
		double avg = _hostRDA.getAverage(receiver);
		long TS2=System.nanoTime();
		double std = _hostRDA.getStandardDeviation(avg, receiver);
		long TS3=System.nanoTime();
		log.main("avgDelay: "+(TS2-TS1)/1000000.0+", stdDelay: "+(TS3-TS2)/1000000.0);
		double[] recs={avg,std};
		return recs;
	}
	/**
	 * simple RDA
	 * @param receiver
	 * @return
	 */
	public double[] computeStatisticsSRDA(SimpleRDATable  receiver){
		long TS1 = System.nanoTime();
		double avg = _hostSRDA.getAverage(receiver);
		long TS2=System.nanoTime();
		double std = _hostSRDA.getStandardDeviation(avg, receiver);
		long TS3=System.nanoTime();
		log.main("avgDelay: "+(TS2-TS1)/1000000.0+", stdDelay: "+(TS3-TS2)/1000000.0);
		double[] recs={avg,std};
		return recs;
	}
	
	
	public static void main(String[] args){
		
		
		/*
		 * Create the event loop
		 */
		EL.set(new EL(Long.valueOf(Config.getConfigProps().getProperty(
				"sbon.eventloop.statedump", "600000")), Boolean.valueOf(Config
				.getConfigProps().getProperty("sbon.eventloop.showidle",
						"false"))));
		
		
		PassiveRDAHost test=new PassiveRDAHost();

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

	public void reset() {
		// TODO Auto-generated method stub
		this._hostRDA.clear();
		this._hostSRDA.clear();
	}

	/**
	 * parse the cache
	 * @param senderTSTable0
	 */
	public void parseRecordsCache(AbstractMap<Long,Long>  senderTSTable0) {
		// TODO Auto-generated method stub
		parseRecordsCacheRDA(senderTSTable0);
		parseRecordsSRDACacheRDA(senderTSTable0);
	}
	
	public boolean containsKey(Long key) {
		// TODO Auto-generated method stub
		if(getTimeStampForGivenID(key)>=0){
			return true;
		}else{
			return false;
		}
	}
	
	//public static Hashtable<Long,Double> IDs = new Hashtable<Long,Double>();
	/**
	 * get all ids
	 */
	/*public void readCachedIDs() {
		// TODO Auto-generated method stub
		IDs.clear();
		if(!_hostSRDA.SenderTSTable.isEmpty()){
			IDs.putAll(_hostSRDA.SenderTSTable);
		}
		if(!_hostRDA.SenderTSTable.isEmpty()){
			IDs.putAll(_hostRDA.SenderTSTable);
		}
	}*/
	/**
	 * get timestamp
	 * @param id
	 * @return
	 */
	public double getTimeStampForGivenID(long id){
		
		if(!_hostSRDA.SenderTSTable.isEmpty()&&_hostSRDA.SenderTSTable.containsKey(id)){
			return _hostSRDA.SenderTSTable.get(id);
		}
		
		if(!_hostRDA.SenderTSTable.isEmpty()&&_hostRDA.SenderTSTable.containsKey(id)){
			return _hostRDA.SenderTSTable.get(id);
		}
		
		return -1;
			
		
	}

	//symbol
	public void setParsed() {
		// TODO Auto-generated method stub
		_hostRDA.isParsed=true;
		_hostSRDA.isParsed=true;
	}

	public void setUnParsed() {
		// TODO Auto-generated method stub
		_hostRDA.isParsed=false;
		_hostSRDA.isParsed=false;
	}
	
	public boolean isParsed(){
		return _hostRDA.isParsed;
	}
}
