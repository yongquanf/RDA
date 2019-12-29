package util.bloom.RDA;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
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

import edu.NUDT.RDAComm.MissingNodeTSRequestMsg2;
import edu.NUDT.RDAComm.MissingResponseMsg;
import edu.NUDT.RDAComm.RDARequestMsg;
import edu.NUDT.RDAComm.RDAResponseMsg;
import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.NetUtil;
import edu.harvard.syrah.prp.SortedList;
import edu.harvard.syrah.sbon.async.Barrier;
import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.Config;
import edu.harvard.syrah.sbon.async.EL;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB2;
import edu.harvard.syrah.sbon.async.comm.AddressFactory;
import edu.harvard.syrah.sbon.async.comm.AddressIF;
import edu.harvard.syrah.sbon.async.comm.NetAddress;
import edu.harvard.syrah.sbon.async.comm.obj.ObjComm;
import edu.harvard.syrah.sbon.async.comm.obj.ObjCommCB;
import edu.harvard.syrah.sbon.async.comm.obj.ObjCommIF;
import edu.harvard.syrah.sbon.async.comm.obj.ObjCommRRCB;
import edu.harvard.syrah.sbon.async.comm.obj.ObjMessage;
import edu.harvard.syrah.sbon.async.comm.obj.ObjMessageIF;
import edu.harvard.syrah.sbon.async.comm.obj.ObjUDPComm;
import util.bloom.Apache.Key;
import util.bloom.Exist.FineComb;


public class RDAHost {

	static Log log=new Log(RDAHost.class);

	static {

		// All config properties in the file must start with 'HSHer.'

		Config.read("RDA", System
				.getProperty("RDA.config", "config/RDA.cfg"));
	}
	
	// Imperial blocks ports outside of 55000-56999
	public static final int COMM_PORT = Integer.parseInt(Config
			.getConfigProps().getProperty("port", "55504"));
	//my Ip address
	public static final String[] myRegion = Config.getConfigProps()
			.getProperty("myDNSAddress", "").split("[\\s]");
	//target, to the other node
	public static final String[] target = Config.getConfigProps()
			.getProperty("targetDNSAddress", "").split("[\\s]");
	//session
	public static final String SessionFileName=Config
			.getConfigProps().getProperty("SessionFileName", "oU20");
	//total packets
	public static final long TotalPackets=Long.parseLong(Config
			.getConfigProps().getProperty("TotalPackets", "500000"));
	//scale the trace
	public static final double scaleTS=Double.parseDouble(Config
			.getConfigProps().getProperty("scaleTS", "1000"));;//Math.pow(10, 1);//Math.pow(10, -11);

	public static final int RDAEntries = Integer.parseInt(Config
			.getConfigProps().getProperty("RDAEntries", "1000"));		
	public static final int hashFuncNum = Integer.parseInt(Config
			.getConfigProps().getProperty("hashFuncNum", "2"));	
	
	//my address
	public static	AddressIF me;
  	
  	public static int seed=1256422325;
  	
  	public static Random rd;
  	
  	//pseudo header
  	public static long increaser = -1;
  	
	// RDA measurement host
	private RDAMeasurementPoint _hostFourFields;
	private SimpleRDAMeasurementPoint _host;
	
	//comm end point
	private ObjCommIF comm;
	
	public static int Repeat_DELAY = 1000;
	// tick control
	final CB0 tickControl;	
	
	//constructor
	public RDAHost(ObjCommIF _comm){
		rd = new Random(System.currentTimeMillis());
		increaser = 1;
		comm = _comm;
		_hostFourFields = new RDAMeasurementPoint(RDAEntries,TotalPackets,hashFuncNum);	
		_host = new SimpleRDAMeasurementPoint(RDAEntries,TotalPackets,hashFuncNum);	
		//parse files, construct RDA					
		parseFiles();
		
		//repeat the request
		tickControl = new CB0() {
			@Override
			protected void cb(CBResult result) {
				// TODO Auto-generated method stub
				//request to remote nodes
				doRDARequest(target[0],  new  CB0(){
					@Override
					protected void cb(CBResult result) {
						// TODO Auto-generated method stub					
					
					}						
				}) ;
			}
		};
	}
	
	//constructor
	public RDAHost() {
		// TODO Auto-generated constructor stub
	rd = new Random(System.currentTimeMillis());	
	increaser = 1;
	//RDA initialize	
	_hostFourFields = new RDAMeasurementPoint(RDAEntries,TotalPackets,hashFuncNum);
	_host = new SimpleRDAMeasurementPoint(RDAEntries,TotalPackets,hashFuncNum);	
	
	//parse files					
	parseFiles();
	
	//repeat the request
	tickControl = new CB0() {
		@Override
		protected void cb(CBResult result) {
			// TODO Auto-generated method stub
			//request to remote nodes
			doRDARequest(target[0],  new  CB0(){
				@Override
				protected void cb(CBResult result) {
					// TODO Auto-generated method stub										
				}						
			}) ;
		}
	};
	
	}

	/**
	 * set the session name
	 */
	public void setSessionFile(){
		
	}
	/**
	 * entry
	 */
	public void  begin(){
		
		
		try {					 
			AddressFactory.createResolved(Arrays.asList(myRegion),
	   			COMM_PORT, new CB1<Map<String, AddressIF>>() {
	   					
	   					protected void cb(CBResult result,
	   							Map<String, AddressIF> addrMap) {
	   						switch (result.state) {
	   						case OK: {
	   
	   							for (String node : addrMap.keySet()) {
	   
	   								AddressIF remoteAddr = addrMap.get(node);
	   								me = AddressFactory.create(remoteAddr);
	   								log.main("resolved'" + me + "'");
	   								
	   								//start a new session
	   								 startSession(new CB0() {
	   									
	   									protected void cb(CBResult result1) {
	   										switch (result1.state) {
	   										case OK: {
	   											System.out.println("node initialised successfully");
	   											registerRepeatRDATestTimer();
	   											break;
	   										}
	   										case TIMEOUT:
	   										case ERROR: {
	   											log.error("Could not initialise NiNAer node: "
	   															+ result1.toString());
	   											System.exit(0);
	   											break;
	   										}
											default:
												System.out.println("unknown conditions");
												break;
	   										}
	   									}
	   								});	
	   							}
	   							break;
	   						}
	   						case TIMEOUT:
	   						case ERROR: {
	   							log.error("Could not resolve bootstrap list: "
	   									+ result.what);
	   							break;
	   						}
							default:
								log.main("pass!");
								break;
	   						}
	   					}
	   
	   				});	
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	/**
	 * session
	 * @param cbDone
	 */
	public void startSession(CB0 cbDone){
		
		
		
		//comm = new ObjUDPComm(); 		
		comm = new ObjComm();
		
		final AddressIF objCommAddr =AddressFactory.create(me,COMM_PORT);
		
		comm.initServer(objCommAddr, new CB0() {
			
			protected void cb(CBResult result) {
				switch (result.state) {
				case OK: {
					
					//RDA request, packet ts request
					comm.registerMessageCB(RDARequestMsg.class, new QueryRDAReqHandler());
					comm.registerMessageCB(MissingNodeTSRequestMsg2.class, new MissingNodeReqHandler());
					
					
					
					//request to remote nodes
					doRDARequest(target[0],  new  CB0(){
						@Override
						protected void cb(CBResult result) {
							// TODO Auto-generated method stub
							//registerRepeatRDATestTimer();
							cbDone.call(result);
						
						}						
					}) ;

					break;
				}
				default: {
					cbDone.call(result);
					log.warn("error to init the server!");
					System.exit(0);
					break;
				}
				}
			}
		});
	}
	

	/**
	 * read the trace
	 * return the target
	 */
	public void parseFiles(){
		
		Scanner sc;
		try {
			sc = new Scanner(new File(SessionFileName));
			long lines=0;
			
		      while (sc.hasNext()&&lines<=TotalPackets) {
		    	  
		          String str=sc.nextLine();	            
		            long[]id={1};
		            double[]TS={1.0};
		        	 parseTimeStamp(str,id,TS);
		        
		          //System.out.println("line: "+lines + ", "+timestamp);
		         // double timestamp = TS[0];
		         if(Double.isInfinite(TS[0])||Double.isNaN(TS[0])){
						System.err.println("infinite or nan");
						continue;
					}
		         //record the packets
		         _host.packetIncoming(id[0],TS[0]);
		      }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	      
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

		/**
		 * TCP: SRCip:port,DSTip:port,seqNumber
		 * @param TCPHeader
		 * @return
		 * @throws UnsupportedEncodingException 
		 */
		public static long MapFromTCP2HashID(String Header) throws UnsupportedEncodingException{
			
			return  (FineComb.sHash64.hash(Header));
			//ByteBuffer sendBuffer=ByteBuffer.wrap((TCPHeader+sequenceNumber).getBytes("UTF-16"));			
			//return new Key(sendBuffer.array());
			
		}
	

	
	
	public void registerRepeatRDATestTimer(){
		long delay = Repeat_DELAY;

		// log.debug("setting timer to " + delay);
		EL.get().registerTimerCB(delay, tickControl);
	}

	//--------------  	
	public abstract class ResponseObjCommCB<T extends ObjMessageIF> extends ObjCommCB<T> {

		protected void sendResponseMessage(final String handler, final AddressIF remoteAddr, final ObjMessage response,
				final long requestMsgId, final String errorMessage, final CB1<Boolean> cbHandled) {

			if (errorMessage != null) {
				log.warn(handler + " :" + errorMessage);
			}
			/*
			 * if(response.hasRepeated>hasResendCouter){ log.warn(
			 * "reponse failed"); return; }
			 */
			comm.sendResponseMessage(response, remoteAddr, requestMsgId, new CB0() {
				protected void cb(CBResult sendResult) {
					switch (sendResult.state) {
					case TIMEOUT:
					case ERROR: {
						log.warn(handler + ": " + sendResult.what);
						/*
						 * response.hasRepeated++; sendResponseMessage(handler,
						 * remoteAddr, response, requestMsgId, errorMessage,
						 * cbHandled);
						 */
						return;
					}
					}
				}
			});
			cbHandled.call(CBResult.OK(), true);
		}
	}



	public class QueryRDAReqHandler extends ResponseObjCommCB<RDARequestMsg> {

		@Override
		protected void cb(CBResult result, RDARequestMsg arg1, AddressIF arg2, Long arg3, CB1<Boolean> cbHandled) {
			// TODO Auto-generated method stub
			final AddressIF fromNode = arg1.from;
			//store my hashtable
			RDAResponseMsg msg = new RDAResponseMsg(_host.sender.hashTable);
			sendResponseMessage("RDARequest", fromNode, msg, arg1.getMsgId(), null, cbHandled);
		}

	}

	public class MissingNodeReqHandler extends ResponseObjCommCB<MissingNodeTSRequestMsg2> {

		@Override
		protected void cb(CBResult result, MissingNodeTSRequestMsg2 arg1, AddressIF arg2, Long arg3, CB1<Boolean> arg4) {
			// TODO Auto-generated method stub
			final AddressIF fromNode = arg1.from;
			//store nodes
			Set<Long> nodes = arg1.ids;
			Hashtable<Long,Double> hashTable = new Hashtable<Long,Double>();
			Iterator<Long> ier = nodes.iterator();
			while(ier.hasNext()){
				Long key = ier.next();				
				if(_host.SenderTSTable.containsKey(key)){
					hashTable.put(key, _host.SenderTSTable.get(key));
				}
			}
						
			MissingResponseMsg msg = new MissingResponseMsg(hashTable);
			sendResponseMessage("MissingRequest", fromNode, msg, arg1.getMsgId(), null, arg4);

		}

	}

	
	public void doRDARequest(String target, final CB0 cbDone) {

		registerRepeatRDATestTimer();
		
		final long time1 = System.currentTimeMillis();

		AddressFactory.createResolved(target, COMM_PORT, new CB1<AddressIF>() {
			@Override
			protected void cb(CBResult result, AddressIF addr) {
				// TODO Auto-generated method stub
				switch (result.state) {
				case OK: {
					log.main("doRDARequest: "+target);
					AddressIF  neighbor = addr;					
					long time2 = System.currentTimeMillis();
					 double resolveAddrDelay = (time2-time1)/1000.00;
				if(true){
						
						//request RDA
						RDARequestMsg msg = new RDARequestMsg(me);
						final long sendStamp = System.nanoTime();

						//log.info("Sending gossip request to "+ neighbor);

						comm.sendRequestMessage(msg, neighbor,
								new ObjCommRRCB<RDAResponseMsg>() {
	

									@Override
									protected void cb(CBResult result, RDAResponseMsg arg1, AddressIF arg2, Long arg3) {
										// TODO Auto-generated method stub
										long receiveTS = System.nanoTime();
										switch (result.state) {
										case OK: {											
											
											log.main("received RDA response: "+arg1.getMsgId());
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
											boolean decoded=_host.decodeSet(receiver, SenderItems, ReceiverItems);//.decodeSetDiffWithTime(receiver);
											long d2=System.nanoTime();
											
											log.main("decoded: "+SenderItems.size()+", "+ReceiverItems.size());
											
											double decodeDelay = (d2 - decodeTS)/1000000.0;
											//no need to repair
											if(SenderItems.isEmpty()&& ReceiverItems.isEmpty()){
												computeStatistics(receiver);												
												System.out.println("resolveAddrDelay: "+resolveAddrDelay+"sendDelay: "+delay+", decodeDelay: "+decodeDelay);
												break;
											}else{
											//request the other's packets
											if(!ReceiverItems.isEmpty()){
												//request 
												MissingNodeTSRequestMsg2 msg2 = new MissingNodeTSRequestMsg2(
														me,ReceiverItems);
												long missMsgTS = System.nanoTime();
												comm.sendRequestMessage(msg2, neighbor,
														new ObjCommRRCB<MissingResponseMsg>() {

															@Override
															protected void cb(CBResult result, MissingResponseMsg argMiss,
																	AddressIF arg2, Long arg3) {
																// TODO Auto-generated method stub
																switch (result.state) {
																case OK: {
																	long missResponseTS=System.nanoTime();
																	double delayMiss=(missResponseTS-missMsgTS)/1000.0;
																	//return packets
																	Hashtable<Long, Double> ReceiverTSTable=argMiss._hashTable;
																	//repair
																	long repairT1=System.nanoTime();
																	_host.repairDirect(SenderItems,receiver,ReceiverItems, ReceiverTSTable);
																	long repairT2=System.nanoTime();
																	computeStatistics(receiver);
																	long repairT3=System.nanoTime();
																	double eraseDelay = (repairT2 - repairT1)/1000000.0;
																	double computeDelay = (repairT3 - repairT2)/1000000.0;
																	//sendRDA,sendMiss
																	System.out.println("resolveAddrDelay: "+resolveAddrDelay+"sendDelay: "+delay+", decodeDelay: "+
																	decodeDelay+", missRequestDelay: "+delayMiss+", eraseDelay: "+eraseDelay+", computeDelay: "+computeDelay);
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
													_host.repairDirect(SenderItems,receiver,ReceiverItems, null);
													computeStatistics(receiver);
												}
											}
											}
											log.main("finished!");
											break;
										}
										case TIMEOUT:
										case ERROR: {
											cbDone
													.call(result);
											break;
										}
										default:
											break;
										}
									}
									
								});
			
						
						

					}
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
	
	/**
	 * directly compute statistics
	 * @param receiver
	 */
	public double[] computeStatistics(RDATable  receiver){
		double avg = _host.getAverage(receiver);
		double std = _host.getStandardDeviation(avg, receiver);
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
		
		
		RDAHost test=new RDAHost();
		test.begin();

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
