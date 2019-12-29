package edu.NUDT.control;


import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.collections.FastHashMap;
import org.pcap4j.core.BpfProgram;
import org.pcap4j.core.NotOpenException;
import org.pcap4j.core.PacketListener;
import org.pcap4j.core.PcapAddress;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.core.PcapNetworkInterface;
import org.pcap4j.core.Pcaps;
import org.pcap4j.core.BpfProgram.BpfCompileMode;
import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.IpPacket;
import org.pcap4j.packet.IpPacket.IpHeader;
import org.pcap4j.packet.IpV4Packet;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.TcpPacket;
import org.pcap4j.packet.Packet.Header;
import org.pcap4j.packet.TcpPacket.TcpHeader;

import com.google.common.collect.Sets;

import org.pcap4j.core.PcapNetworkInterface.PromiscuousMode;

import edu.NUDT.RDA.parallel.Pair;
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
import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.NetUtil;
import edu.harvard.syrah.prp.POut;
import edu.harvard.syrah.sbon.async.CBResult;
import edu.harvard.syrah.sbon.async.Config;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB0;
import edu.harvard.syrah.sbon.async.CallbacksIF.CB1;
import edu.harvard.syrah.sbon.async.EL.Priority;
import edu.harvard.syrah.sbon.async.comm.AddressIF;
import edu.harvard.syrah.sbon.async.comm.NetAddress;
import edu.harvard.syrah.sbon.async.comm.obj.ObjComm;
import edu.harvard.syrah.sbon.async.comm.obj.ObjCommCB;
import edu.harvard.syrah.sbon.async.comm.obj.ObjCommIF;
import edu.harvard.syrah.sbon.async.comm.obj.ObjCommRRCB;
import edu.harvard.syrah.sbon.async.comm.obj.ObjMessage;
import edu.harvard.syrah.sbon.async.comm.obj.ObjMessageIF;
import edu.harvard.syrah.sbon.async.EL;
import util.async.HashMapCache;
import util.async.UniformHashFunc;
import util.async.java.util.concurrent.ConcurrentHashMap;
import util.bloom.Apache.BloomFilter;
import util.bloom.Apache.BloomFilterFactory;
import util.bloom.Apache.CountingBloomFilter;
import util.bloom.Apache.Filter;
import util.bloom.Apache.Key;
import util.bloom.Apache.Hash.Hash;
import util.bloom.Apache.Hash.MurmurHash3;
import util.bloom.Apache.Hash.MurmurHash3.LongPair;
import util.bloom.Exist.FineComb;
import util.bloom.RDA.PassiveRDAHost;

/**
 * control the passive collector
 */
public class PassiveCollectorPeriod{

	public final static Log log = new Log(PassiveCollectorPeriod.class);
	
	static {

		// All config properties in the file must start with 'HSHer.'

		Config.read("RDA", System
				.getProperty("RDA.config", "config/RDA.cfg"));
	}
	/**
	 * threads to collect packets
	 */
	public static ExecutorService execRDA = Executors.newFixedThreadPool(15);
	
	public static final boolean isActive = Boolean.parseBoolean(Config
			.getConfigProps().getProperty("active", "false"));
	// Imperial blocks ports outside of 55000-56999
	public static final int COMM_PORT = Integer.parseInt(Config
			.getConfigProps().getProperty("port", "55504"));
	/**
	 * shift the measure
	 */
	public static final long TSConstant = Long.parseLong(Config
			.getConfigProps().getProperty("TSConstant", "1482065348060"));
	/**
	 * receiver delay the measure at a time
	 */
	public static final long lateBindMeasure4Receiver = Long.parseLong(Config
			.getConfigProps().getProperty("lateBindMeasure4Receiver", "200"));
	
	public static final int UDPPort= Integer.parseInt(Config
			.getConfigProps().getProperty("UDPPort", "55548"));
	
	//my Ip address
	public static final String[] myRegion = Config.getConfigProps()
			.getProperty("myDNSAddress", "").split("[\\s]");
	//target, to the other node
	public static final String[] target = Config.getConfigProps()
			.getProperty("targetDNSAddress", "").split("[\\s]");
	public static final String netcard=Config.getConfigProps()
			.getProperty("netcard", "localhost");
	
	//total packets
	public static final long TotalPackets=Long.parseLong(Config
			.getConfigProps().getProperty("TotalPackets", "500000"));
	//scale the trace
	public static final double scaleTS=Double.parseDouble(Config
			.getConfigProps().getProperty("scaleTS", "0.0000000000001"));;//Math.pow(10, 1);//Math.pow(10, -11);

	public static final double falsePositiveProbability=Double.parseDouble(Config
			.getConfigProps().getProperty("falsePositiveProbability", "0.0000001"));
			
	public static final int RDAEntries = Integer.parseInt(Config
			.getConfigProps().getProperty("RDAEntries", "1000"));		
	public static final int hashFuncNum = Integer.parseInt(Config
			.getConfigProps().getProperty("hashFuncNum", "2"));	

	public static final long offsetDate = Integer.parseInt(Config
			.getConfigProps().getProperty("offsetDate", "5000"));	
	/**
	 * how long, how often
	 */
	public static final long measurementPeriod=Long.parseLong(Config.getConfigProps()
			.getProperty("measurementPeriod", "10000"));
	public static final long restartMeasureInterval =Long.parseLong(Config.getConfigProps()
			.getProperty("restartMeasureInterval", "30000"));

	public final static String filterProg=Config.getConfigProps()
			.getProperty("filterProg", "ip host 10.107.20.2 and 10.107.20.3");
	
	BpfProgram prog = null;
	
	/**
	 * test existence
	 */
	BloomFilter packetBF;
	 /**
	  * cache the latest date
	  */
	 public volatile long myNearestStartDate = -1;
	 /**
	  * common value
	  */
	 public volatile long lastpassiveCommonStartDate = -1;
	//my address
	public static	AddressIF me;
	
//comm end point
public ObjCommIF comm = null;

public ObjComm commSync= null;


	
	/**
	 * collector
	 */

	/**
	 * handler to pcap
	 */
    PcapHandle dumpHandler;
	
	/**
	 * maximum cache
	 */
    //ConcurrentHashMap<Long,Long> SenderTable; 
    
    //ConcurrentHashMap<Key,Pair<Long,Long>> ID2Index; 
    /**
     * duplicate packet
     */
    //ConcurrentHashMap<Long,Long> DuplicatedSenderTSTable;
	/**
	 * host
	 */
	public PassiveRDAHost RDAHost = null; 
	
	private PacketListener  listener;
	/**
	 * maximum packets
	 */
	private long  MaximumPackets= -1;
	//address
	byte[] nodeA;
	byte[] nodeB;
	private AddressIF synTargetAddress;


	
	public PassiveCollectorPeriod(CB0 cbDone){
		
		MaximumPackets = PassiveCollectorPeriod.TotalPackets;
		
		long vectorSize = Math.round((Math.log(falsePositiveProbability)/Math.log(0.6185))*(int)MaximumPackets);
		
		int nbHash= (int)Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2)));
				//(Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))) / Math.log(2);
		
		packetBF = (BloomFilter) BloomFilterFactory.createBloomFilter((int)vectorSize, nbHash,Hash.MURMUR_HASH);
		
		RDAHost=new PassiveRDAHost();	
		
		//SenderTable = new ConcurrentHashMap<Long,Long>((int)MaximumPackets);
		//ID2Index = new ConcurrentHashMap<Key,Pair<Long,Long>>((int)MaximumPackets);
		
		
		


		
		
		//DuplicatedSenderTSTable= new ConcurrentHashMap<Long,Long>((int)MaximumPackets);
		nodeA =NetUtil.stringIPToByteIP(PassiveCollectorPeriod.myRegion[0]);
		nodeB = NetUtil.stringIPToByteIP(PassiveCollectorPeriod.target[0]);

		synTargetAddress=NetAddress.createUnresolved(PassiveCollectorPeriod.target[0], PassiveCollectorPeriod.UDPPort);
		
		/**
		 * init the pcap
		 */
		initPcap();
		
		startSingleTonSession(new CB0(){
			@Override
			protected void cb(CBResult result) {
				// TODO Auto-generated method stub
				switch (result.state) {
				case OK: {		
					log.main("RDA host initialized");
					//setTrigger();
					if(PassiveCollectorPeriod.isActive){
					setupMeasureSync(new CB0(){
						@Override
						protected void cb(CBResult result) {
							log.main("result: "+result.toString());
							}
					});}
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
	
	public boolean openPcap(){
		   /***************************************************************************
	     * Fouth we create a packet handler which receives packets and tells the 
	     * dumper to write those packets to its output file
	     **************************************************************************/
	   
		boolean result = false;
		
		int snaplen = 64 * 1024;           // Capture all packets, no trucation
	    int timeout = 10 * 1000;           // 10 seconds in millis

	    PcapNetworkInterface card=null;
	    
	    		List<PcapNetworkInterface> tmp;
				try {
					tmp = Pcaps.findAllDevs();
					POut.toString(tmp);
		    		if(!tmp.isEmpty()){
		    			Iterator<PcapNetworkInterface> ier = tmp.iterator();
		    			while(ier.hasNext()){
		    				PcapNetworkInterface nxt = ier.next();
		    				log.main(nxt.getName());
		    				
		    				Iterator<PcapAddress> ierPcap = nxt.getAddresses().iterator();
		    				while(ierPcap.hasNext()){
		    					byte[] addr = ierPcap.next().getAddress().getAddress();
		    					log.main(NetUtil.byteIPAddrToString(addr));
		    					
		    					if(testEqual(addr,nodeA)){
		    						try {
		    							card = nxt;
		    							
		    							
		    							
										dumpHandler =nxt.openLive(snaplen, PromiscuousMode.PROMISCUOUS,  timeout);
										
										prog = dumpHandler.compileFilter(
		    								       filterProg, BpfCompileMode.OPTIMIZE, PcapHandle.PCAP_NETMASK_UNKNOWN
		    								      );
		    							dumpHandler.setFilter(prog);
		    							
										//prog = dumpHandler.compileFilter(
										//        "tcp", BpfCompileMode.OPTIMIZE, PcapHandle.PCAP_NETMASK_UNKNOWN
										//	      );
										
										//dumpHandler.setFilter(prog);;
										
										result = dumpHandler.isOpen();
										break;
									} catch (Exception e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
		    					}
		    				}
		    				if(false){
		    				//not local
		    				if(nxt.getName().equalsIgnoreCase(PassiveCollectorPeriod.netcard)){
		    					try {
									dumpHandler =nxt.openLive(snaplen, PromiscuousMode.PROMISCUOUS,  timeout);
									result = dumpHandler.isOpen();
									break;
								} catch (PcapNativeException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
		    				}
		    				}
		    			}
		    		}
				} catch (PcapNativeException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		if(card!=null){		
			log.main("selected card: "+card.getName());
		}
	    return result;
	}
	/**
	 * init the data structure
	 * @param maxPacket
	 */
	public void initPcap(){
		
		//log.main("init pcap");
		//open
 		openPcap();
 		
 		//listener
	    listener = new PacketListener(){
			@Override
			public void gotPacket(Packet packet) {
				// TODO Auto-generated method stub
				//log.main(packet.toString());
				//&&packet.contains(TcpPacket.class)&&packet.contains(TcpPacket.class)
				
				//scale
				if(packet.contains(IpV4Packet.class)){	
					long ts = dumpHandler.getTimestamp().getTime()-PassiveCollectorPeriod.TSConstant;
					
					IpV4Packet ipPkt = packet.get(IpV4Packet.class);
					IpHeader ipHeader = ipPkt.getHeader();
					 byte[] src = ipHeader.getSrcAddr().getAddress();
					 byte[] dst = ipHeader.getDstAddr().getAddress();
					 
					 //TcpPacket tcppkt = ipPkt.getPayload().get(TcpPacket.class);
					 
					 try {
						 
						 //test the existence of the packet, if it is recorded before, we remove it
						 
						 //get the key of tcp packet
						// byte[] header = ipPkt.getRawData();
						 
						 if((PassiveCollectorPeriod.isActive&&testEqual(src,nodeA)&&testEqual(dst,nodeB))||
								 (!PassiveCollectorPeriod.isActive&&testEqual(dst,nodeA)&&testEqual(src,nodeB))){
							 Key byteRaw = new Key(ipPkt.getRawData());
							 
							 //store key,value
							   if(!packetBF.membershipTest(byteRaw)){
								   //get the hash code
								  packetBF.add(byteRaw);
								  addNewKey(byteRaw,ts);
						 	}
							 
						 }
						
						
							 // hashCode=PassiveRDAHost.MapFromTCP2HashID(tcpheader);
							   
							   //key,hashCode							   
							   //store &&testEqual(src,nodeA)&&testEqual(dst,nodeB)//&&testEqual(dst,nodeA)&&testEqual(src,nodeB)
							   /*if(PassiveCollectorPeriod.isActive){
									 //64 byte is not enough		 
									 //long hashCode = PassiveRDAHost.MapFromTCP2HashID();
										//log.main("ts: "+ts+", hash: "+hashCode);
									 //avoid repeat
								   Key byteRaw = new Key(ipPkt.getRawData());
									 
									 //store key,value
									   if(!packetBF.membershipTest(byteRaw)){
										   //get the hash code
										  packetBF.add(byteRaw);
										  addNewKey(byteRaw,ts);
									   }
									 
								 }else if(!PassiveCollectorPeriod.isActive){
									 //long ts = dumpHandler.getTimestamp().getTime();
									 //long hashCode = PassiveRDAHost.MapFromTCP2HashID(packet.getRawData());						
										//log.main("ts: "+ts+", hash: "+hashCode);
									 
									 
								 }*/
						   
						   
					 
					 } catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					/* 
					long hashCode;
					if((testEqual(src,nodeA)&&testEqual(dst,nodeB))||(testEqual(src,nodeA)&&testEqual(dst,nodeB))){
						
						long ts = dumpHandler.getTimestamp().getTime();
						try {
							TcpPacket tcpPkt = packet.get(TcpPacket.class);
							//TcpHeader header = tcpPkt.getHeader();
							hashCode = PassiveCollectorPeriod.MapFromTCP2HashID(tcpPkt.getRawData());
							//log.main("ts: "+ts+", hash: "+hashCode);
							SenderTSTable.put(hashCode,Long.valueOf(ts));
						} catch (UnsupportedEncodingException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}*/
					if(false){
						src = ipHeader.getSrcAddr().getAddress();
						dst = ipHeader.getDstAddr().getAddress();
						byte[] a =NetUtil.stringIPToByteIP(PassiveCollectorPeriod.myRegion[0]);
						byte[] b = NetUtil.stringIPToByteIP(PassiveCollectorPeriod.target[0]);
						//filter, only record the output to the destination values
						if((testEqual(src,a)&&testEqual(dst,b))||(testEqual(src,b)&&testEqual(dst,a))){
							//int ts = dumpHandler.getTimestamp().getNanos();	
							TcpPacket tcpPkt = packet.get(TcpPacket.class);
							TcpHeader header = tcpPkt.getHeader();
							//long t1 = header.getSequenceNumberAsLong()^header.getAcknowledgmentNumberAsLong();
							//long t2 = Long.valueOf(header.getWindowAsInt())^Long.valueOf(header.getSrcPort().valueAsInt())^Long.valueOf(header.getDstPort().valueAsInt());
							//long hashCode =  UniformHashFunc.os_dietz8to3(t1, t2);
							try {
								//long hashCode = PassiveRDAHost.MapFromTCP2HashID(header.toHexString());
								//log.main("ts: "+ts+", hash: "+hashCode);
								//SenderTSTable.put(hashCode,Long.valueOf(ts));
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}		
					}
					
					
			
				}	   			
	}
	    };
	    
	}
	    
	/**
	 * xor of f
	 * @param ids
	 * @return
	 */
	protected long computeXor(int[] ids) {
		// TODO Auto-generated method stub
		Long f=Long.valueOf(ids[0]);
		for(int i=1;i<ids.length;i++){
			f^=ids[i];
		}
		return f;
	}

	/**
	 * add to the hash map, increase the idIndex
	 * @param hashCode
	 * @param ts
	 */
	protected void addNewKey(Key key, long ts) {
		// TODO Auto-generated method stub
		//murmum3
		int offset=0;
		byte[] bytes = key.getBytes();
		int seed=0x5bd1e995;
		int m2 = 0x9747b28c;
		LongPair out = new LongPair();
		MurmurHash3.murmurhash3_x64_128(bytes, offset, bytes.length, seed, out);
		long hashCode = out.val1+m2*out.val1;
		
		//store to RDA directly
		RDAHost.parseRecord(hashCode, ts);
		
		//SenderTable.put(hashCode, ts);
		//store the key
		//ID2Index.put(key, new Pair<Long,Long>(hashCode,ts));
	}


		/**
		 * two address point to the same IP
		 */
		public static boolean testEqual(byte[] byteA,byte[] byteB){
			if(byteA==null||byteB==null||byteA.length!=byteB.length){
				return false;
			}
			for(int i = 0; i<byteA.length;i++){				
				byte tmp =(byte) (byteA[i] ^ byteB[i]);
				if(tmp != (byte)(0x00)){
					return false;
				}
			}
			return true;
		}
		
	
	/**
	 * byteBuffer
	 * @param ofile
	 * @param count
	 */
	public void pcapStore2Hash(int count){
   	    		 	
	    /***************************************************************************
	     * Fifth we enter the loop and tell it to capture  packets.
	     **************************************************************************/
		
		
	    try {
	    	long t1=System.currentTimeMillis();
	    	log.main("begin: "+t1+", count: "+count);
	    	/**
	    	 * ready threads
	    	 */
	    	//ExecutorService execRDA = Executors.newCachedThreadPool();
			dumpHandler.loop(count, listener,execRDA);
			//execRDA.shutdown();
			log.main("end: "+(System.currentTimeMillis()-t1)/1000);
			log.main("BF recorded: "+packetBF.NumInserted+", fp: "+packetBF.getAveragedPriorFP());
			terminateCollect(new CB0(){
				@Override
				protected void cb(CBResult result) {
					// TODO Auto-generated method stub
					restartCollection(new CB0(){
						@Override
						protected void cb(CBResult result) {
							// TODO Auto-generated method stub
							log.main("end terminate collect");
						}
						
					});
				}
			});
			
			
		} catch (PcapNativeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotOpenException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	private boolean restartPcapInit() {
		// TODO Auto-generated method stub	
		boolean result = false;
		int snaplen = 64 * 1024;           // Capture all packets, no trucation
	    int timeout = 10 * 1000;           // 10 seconds in millis

	    		List<PcapNetworkInterface> tmp;
				try {
					tmp = Pcaps.findAllDevs();
					POut.toString(tmp);
		    		if(!tmp.isEmpty()){
		    			Iterator<PcapNetworkInterface> ier = tmp.iterator();
		    			while(ier.hasNext()){
		    				PcapNetworkInterface nxt = ier.next();
		    				log.main(nxt.getName());
		    				
		    				if(false){
		    					if(nxt.getName().equalsIgnoreCase(PassiveCollectorPeriod.netcard)){
		    					try {
									dumpHandler =nxt.openLive(snaplen, PromiscuousMode.PROMISCUOUS,  timeout);
									result = dumpHandler.isOpen();
									break;
								} catch (PcapNativeException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
		    				}
		    				}
		    				Iterator<PcapAddress> ierPcap = nxt.getAddresses().iterator();
		    				while(ierPcap.hasNext()){
		    					byte[] addr = ierPcap.next().getAddress().getAddress();
		    					if(testEqual(addr,nodeA)){
		    						try {
										dumpHandler =nxt.openLive(snaplen, PromiscuousMode.PROMISCUOUS,  timeout);
									
										prog = dumpHandler.compileFilter(
		    								       filterProg, BpfCompileMode.NONOPTIMIZE, PcapHandle.PCAP_NETMASK_UNKNOWN
		    								      );
		    							dumpHandler.setFilter(prog);
										
										//dumpHandler.setFilter(prog);
										
										result = dumpHandler.isOpen();
										break;
									} catch (Exception e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
		    					}
		    				}
		    			}
		    		}
				} catch (PcapNativeException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				return result;
	}

	
	/**
	 * stop
	 * call the end point, stop the measurement
	 */
	public void terminateCollect(CB0 cbDone){
			try {
				//dumpHandler.breakLoop();				
				//dumpHandler.close();
				log.main("terminate!");
				//flush the record
				RDAHost.setParsed();				
				startRDAProcess(new CB0(){
					@Override
					protected void cb(CBResult result) {
						// TODO Auto-generated method stub
						cbDone.call(result);
					}					
				});
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
			

	}
	
	
	/**
	 * RDA process
	 */
	private void startRDAProcess(CB0 cbDone) {
		// TODO Auto-generated method stub
		
		//clear duplicated
		/*Iterator<Long> ier = DuplicatedSenderTSTable.keySet().iterator();
		while(ier.hasNext()){
			SenderTSTable.remove(ier.next());
		}*/
		//log.main("id stored: "+ID2Index.size());
		//RDAHost.parseRecordsCache(SenderTable);
		
		//DuplicatedSenderTSTable.clear();
		
		
		//active node only
		if(PassiveCollectorPeriod.isActive){		
		RDAHost.RemoteRDAComputeTest(new CB0(){
			@Override
			protected void cb(CBResult result) {
				// TODO Auto-generated method stub
				//log.main("remote RDA compute finish");
				cbDone.call(result);
			}
			
		});
		}
	}



	/**
	 * start the measure:
	 * call the end point, to begin the collection at time	
	 */
	public void startCollect(){		
		//SenderTable.clear();
		//ID2Index.clear();		
		RDAHost.setUnParsed();
		pcapStore2Hash(Long.valueOf(MaximumPackets).intValue());
	}
	
	/*
	 * restart the collection
	 */
	public boolean restartCollect(){
		//reset
		RDAHost.reset();
		packetBF.clear();	
		
		//DuplicatedSenderTSTable.clear();
		if(!dumpHandler.isOpen()){
			boolean result1 = openPcap();
			return result1;
		}
		
		/*if(dumpHandler==null){
			restartPcapInit();
		}*/
		
		return false;
	}



	public class MissingNodeReqHandler extends ResponseObjCommCB<MissingNodeTSRequestMsg2> {

		@Override
		protected void cb(CBResult result, MissingNodeTSRequestMsg2 arg1, AddressIF arg2, Long arg3, CB1<Boolean> arg4) {
			// TODO Auto-generated method stub
			final AddressIF fromNode = arg1.from;
			//store nodes
			
			Iterator<Long> ier = arg1.ids.iterator();
			
			Hashtable<Long,Double> hashTable = new Hashtable<Long,Double>();
			
			double ts;
			while(ier.hasNext()){
			    Long key = ier.next();
			    ts = RDAHost.getTimeStampForGivenID(key);
				if(ts>=0){
					hashTable.put(key, ts);
				}
			}
						
			MissingResponseMsg msg = new MissingResponseMsg(hashTable);
			sendResponseMessage("MissingRequest", fromNode, msg, arg1.getMsgId(), null, arg4);

		}

	}
	
	public class CacheReqHandler extends ResponseObjCommCB<CacheRequestMsg> {

		@Override
		protected void cb(CBResult result, CacheRequestMsg arg1, AddressIF arg2, Long arg3, CB1<Boolean> arg4) {
			// TODO Auto-generated method stub
			final AddressIF fromNode = arg1.from;
			//store nodes
			Set<Long> nodes = arg1.ids;
			Set<Long> setMe = RDAHost._hostRDA.SenderTSTable.keySet();
			Set<Long> common=Sets.intersection(nodes, setMe);
			log.main("LateBind: "+lateBindMeasure4Receiver+"set dist: "+nodes.size()+", "+setMe.size()+", same: "+common.size()+
					", diff: "+(nodes.size()+setMe.size()-2*common.size()));
			
			Hashtable<Long,Double> hashTable = new Hashtable<Long,Double>();
			//get common		
			double ts;
			if(!common.isEmpty()){
			Iterator<Long> ier = common.iterator();
			while(ier.hasNext()){
				Long key = ier.next();	
				ts = RDAHost.getTimeStampForGivenID(key);
				if(ts>=0){
					hashTable.put(key, ts);
				}
			}
			}
						
			CacheResponseMsg msg = new CacheResponseMsg(hashTable);
			sendResponseMessage("CacheRequest", fromNode, msg, arg1.getMsgId(), null, arg4);

		}

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
			//check if the parse is ready, if not ready, wait for the signal
			long millis = 100;
			int delta = 100;
			while(!RDAHost.isParsed()&&(packetBF.NumInserted+delta<MaximumPackets)){
				log.warn("not yet ready!");
				try {
					Thread.sleep(millis);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				}
				//if(isParsed){
			/*if(!RDAHost._hostRDA.isParsed){
				RDAHost.parseRecordsCacheRDA(SenderTable);
			}*/
				//store my hashtable
			 
				RDAResponseMsg msg = new RDAResponseMsg(RDAHost._hostRDA.sender.copyTable().hashTable);
				sendResponseMessage("RDARequest", fromNode, msg, arg1.getMsgId(), null, cbHandled);
				//}
		}

	}

	public class QuerySRDAReqHandler extends ResponseObjCommCB<SRDARequestMsg> {

		@Override
		protected void cb(CBResult result, SRDARequestMsg arg1, AddressIF arg2, Long arg3, CB1<Boolean> cbHandled) {
			// TODO Auto-generated method stub
			final AddressIF fromNode = arg1.from;
			//check if the parse is ready, if not ready, wait for the signal
			/*long millis = 100;
			while(!isParsed){
				log.warn("not yet ready!");
				try {
					Thread.sleep(millis);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				}*/
				//if(isParsed){
			/*if(!RDAHost._hostSRDA.isParsed){
				RDAHost.parseRecordsSRDACacheRDA(SenderTable);
			}*/
				//store my hashtable
				SRDAResponseMsg msg = new SRDAResponseMsg(RDAHost._hostSRDA.sender.copyTable().hashTable);
				log.main("send response 4 SRDA: "+msg._hashTable.length);
				sendResponseMessage("SRDARequest", fromNode, msg, arg1.getMsgId(), null, cbHandled);
				//}
		}

	}
	
	/**
	 * @param myNearestStartDate
	 * @param destAddr
	 * @param cb1
	 */
	public void doDateSync(AddressIF destAddr,CB1<Long> cb1){
		
		long date = System.currentTimeMillis();
		log.main("$: "+date);
		DateRequestMsg msg = new DateRequestMsg(myNearestStartDate,date);
		commSync.sendRequestMessage(msg, destAddr, new ObjCommRRCB<DateResponseMsg>(){
			@Override
			protected void cb(CBResult result, DateResponseMsg arg1, AddressIF arg2, Long arg3) {
				// TODO Auto-generated method stub
				//update the common date
				switch(result.state){
				case OK:{
					//callback
					log.main("common date: "+arg1.commonDate);
					cb1.call(result, arg1.commonDate);
					break;
				}
				case TIMEOUT:
				case ERROR: {
					log.warn("Could not resolve  address: " + result.what);					
					registerReSync(destAddr,cb1);
					//cb1.call(result,-1L);
					break;
				}
				}//end switch
				
				
			}			
		});
	}
	
	
	protected void registerReSync(AddressIF destAddr,CB1<Long> cb1) {
		// TODO Auto-generated method stub
		EL.get().registerTimerCB(restartMeasureInterval, new CB0(){
			protected void cb(CBResult result) {
				// TODO Auto-generated method stub
				doDateSync(destAddr,cb1);
			}
			
		});
	}

	/**
	 * synchronize the date
	 * @param myNearestStartDate 
	 * @param target
	 * @param cbDone
	 */
	public void syncDate(String target,CB1<Long> cbDone){
		
		AddressIF addr=NetAddress.createUnresolved(target, PassiveCollectorPeriod.UDPPort);//, new CB1<AddressIF>() {
		//	@Override
		//	protected void cb(CBResult result, AddressIF addr) {
				// TODO Auto-generated method stub
			//	switch (result.state) {
			//	case OK: {
					doDateSync(addr,new CB1<Long>(){
						@Override
						protected void cb(CBResult result, Long arg1) {
							// TODO Auto-generated method stub
							switch(result.state){
							case OK:{
								cbDone.call(result, arg1);
								break;
							}
							case ERROR:
							case TIMEOUT:{
								cbDone.call(result, -1L);
								break;
							}
							}
							
						}			
						});
				/*	break;
				}
				case TIMEOUT:
				case ERROR: {
					log.error("Could not resolve  address: " + result.what);
					
					cbDone.call(result,-1L);
					break;
				}
		
				}
			}
		});*/
		

	}
	
	public void syncDate(AddressIF addr,CB1<Long> cbDone){
		
		//AddressIF addr=NetAddress.createUnresolved(target, PassiveCollectorPeriod.UDPPort);//, new CB1<AddressIF>() {
		//	@Override
		//	protected void cb(CBResult result, AddressIF addr) {
				// TODO Auto-generated method stub
			//	switch (result.state) {
			//	case OK: {
					doDateSync(addr,new CB1<Long>(){
						@Override
						protected void cb(CBResult result, Long arg1) {
							// TODO Auto-generated method stub
							switch(result.state){
							case OK:{
								cbDone.call(result, arg1);
								break;
							}
							case ERROR:
							case TIMEOUT:{
								cbDone.call(result, -1L);
								break;
							}
							}
							
						}			
						});
				/*	break;
				}
				case TIMEOUT:
				case ERROR: {
					log.error("Could not resolve  address: " + result.what);
					
					cbDone.call(result,-1L);
					break;
				}
		
				}
			}
		});*/
		

	}
	
	/**
	 * common date
	 * current t;
	 * your t;
	 * if both are -1, not communicated, exchange the date;
	 * if both are the same, and are ahead of now, set this date
	 * otherwise, and are earlier than now, update the date
	 * @param you
	 * @param me
	 * @return
	 */
	public long commonScheduleDate(long yourNearestStart,long you,long me){
		log.main("$: timer: "+yourNearestStart+", you: "+you+", me: "+me+", offsetDate: "+offsetDate);
		if(myNearestStartDate==-1||yourNearestStart==-1){	
			return Math.max(you,me)+offsetDate;
		}else{
			long future =  Math.max(you,me);			
			if(myNearestStartDate!=yourNearestStart){
				return future+offsetDate;				
			}else{				
				if(myNearestStartDate<future){
					return future+offsetDate;
				}else{
					return myNearestStartDate;
				}
			}
		}
	}
	/**
	 * //update my timer
			if(future>myNearestStartDate){
				myNearestStartDate = future;
			}
			log.main();
	 */
	/**
	 * request date
	 * @author quanyongf
	 *
	 */
	public class DateRequestMsgHandler extends ResponseSyncObjCommCB<DateRequestMsg> {
		@Override
		protected void cb(CBResult result, DateRequestMsg arg1, AddressIF fromNode, Long arg3, CB1<Boolean> cbHandled) {
			// TODO Auto-generated method stub
			long future = commonScheduleDate(arg1.myNearestStartDate,arg1.date,System.currentTimeMillis());
		
			if(future > myNearestStartDate){
				myNearestStartDate = future;
				lastpassiveCommonStartDate = future;
			}
			log.main("future: "+future+"Myfuture: "+myNearestStartDate+", lastCommonStartDate: "+lastpassiveCommonStartDate);
			DateResponseMsg msg = new DateResponseMsg(future);
			
			if(!PassiveCollectorPeriod.isActive){				
				//if(myNearestStartDate!=future){
					//update
				myNearestStartDate=future;
				//delay measurement at the receiver
				setupCollection(future+lateBindMeasure4Receiver, future+measurementPeriod+lateBindMeasure4Receiver,
						new CB0(){
							@Override
							protected void cb(CBResult result) {
								// TODO Auto-generated method stub
								log.main("passive adjust the timer: "+future);
							}
					
				});	
				//}
			}
			//store my hashtable
			sendResponseMessage("DateRequest", fromNode, msg, arg1.getMsgId(), null, cbHandled);
		}
	}
	
	
	public abstract class ResponseSyncObjCommCB<T extends ObjMessageIF> extends ObjCommCB<T> {

		protected void sendResponseMessage(final String handler, final AddressIF remoteAddr, final ObjMessage response,
				final long requestMsgId, final String errorMessage, final CB1<Boolean> cbHandled) {

			if (errorMessage != null) {
				log.warn(handler + " :" + errorMessage);
			}
			/*
			 * if(response.hasRepeated>hasResendCouter){ log.warn(
			 * "reponse failed"); return; }
			 */
			commSync.sendResponseMessage(response, remoteAddr, requestMsgId, new CB0() {
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

	
	/**
	 * entry
	 */
	public void  begin(){
						
		try {
			
			me  = NetAddress.createUnresolved(PassiveCollectorPeriod.myRegion[0],PassiveCollectorPeriod.COMM_PORT);
				//log.main("resolved'" + me + "'");
				
				//start a new session
				 startSession(new CB0() {
					
					protected void cb(CBResult result1) {
						switch (result1.state) {
						case OK: {
							System.out.println("node initialised successfully");
							//registerRepeatRDATestTimer();
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
				 
				 
			/*
			NetIPAddress.createUnresolved(Arrays.asList(myRegion),
	   			COMM_PORT, new CB1<Map<String, AddressIF>>() {
	   					
	   					protected void cb(CBResult result,
	   							Map<String, AddressIF> addrMap) {
	   						switch (result.state) {
	   						case OK: {	   
	   							for (String node : addrMap.keySet()) {
	   
	   								//AddressIF remoteAddr = addrMap.get(node);
	   								//me = NetIPAddress.create(remoteAddr);
	   							
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
	   
	   				});	*/
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	/**
	 * singleton session, called only once
	 * 						//request to remote nodes
	 * @param cbDone
	 */
	public void startSingleTonSession(CB0 cbDone){
		try {
			
			NetAddress.createResolved(PassiveCollectorPeriod.myRegion[0],PassiveCollectorPeriod.COMM_PORT, new CB1<AddressIF>() {
				
				@Override
				protected void cb(CBResult result, AddressIF addr) {
					// TODO Auto-generated method stub
					switch (result.state) {
					case OK: {			
						me = addr;
						RDAHost.me=me;
						log.main("resolved'" + me + "'");
						
						//comm = new ObjUDPComm(); 		
						comm = new ObjComm();
						RDAHost.comm=comm;																	
						comm.initServer(me, new CB0() {							
							protected void cb(CBResult result0) {
								switch (result0.state) {
								case OK: {
									log.main("register message");
									//RDA request, packet ts request
									//simple RDA
									comm.registerMessageCB(SRDARequestMsg.class, new QuerySRDAReqHandler());
									//RDA
									comm.registerMessageCB(RDARequestMsg.class, new QueryRDAReqHandler());
									comm.registerMessageCB(MissingNodeTSRequestMsg2.class, new MissingNodeReqHandler());
									comm.registerMessageCB(CacheRequestMsg.class, new CacheReqHandler());
									
									
									final AddressIF objSyncCommAddr =NetAddress.createUnresolved(PassiveCollectorPeriod.myRegion[0],PassiveCollectorPeriod.UDPPort);//NetIPAddress.create(me,UDPPort);
									commSync = new ObjComm();
									commSync.initServer(objSyncCommAddr, new CB0(){

										@Override
										protected void cb(CBResult result) {
											// TODO Auto-generated method stub
											switch (result.state) {
											case OK: {
												
												commSync.registerMessageCB(DateRequestMsg.class, new DateRequestMsgHandler());										
												log.main("init comm completed");
												cbDone.call(result);
												break;
											}
											default: {
												cbDone.call(result);
												log.warn("error to init the server!");
												//System.exit(0);
												break;
											}
											}
										}
									});
									//cbDone.call(result);
									break;
								}
								case ERROR:
								case TIMEOUT: {
									log.warn("error to init the server!");
									cbDone.call(result0);							
									//System.exit(0);
									break;
								}
								}
							}
						});	
						
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
		
		final AddressIF objCommAddr =NetAddress.createUnresolved(PassiveCollectorPeriod.myRegion[0],PassiveCollectorPeriod.COMM_PORT);
		
		comm.initServer(objCommAddr, new CB0() {
			
			protected void cb(CBResult result) {
				switch (result.state) {
				case OK: {
					
					//RDA request, packet ts request
					comm.registerMessageCB(RDARequestMsg.class, new QueryRDAReqHandler());
					comm.registerMessageCB(MissingNodeTSRequestMsg2.class, new MissingNodeReqHandler());

					//request to remote nodes
					RDAHost.RemoteRDAComputeTest( new  CB0(){
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
	 * start: ts
	 * end: ts, or reach the maximum count
	 * @param startTS
	 * @param endTS
	 */
	public void setupCollection(long startTS, long endTS,CB0 cbDone){
		scheduleStart(startTS);
		
		//scheduleEnd(endTS,cbDone);
	}
	
	/**
	 * restart
	 */
	public void restartCollection(CB0 cbDone){
			
		if(PassiveCollectorPeriod.isActive){
			EL.get().registerTimerCB(restartMeasureInterval, new CB0(){
				protected void cb(CBResult result) {
				// TODO Auto-generated method stub						
				//passive
				setupMeasureSync(new CB0(){
					@Override
					protected void cb(CBResult result) {
						// TODO Auto-generated method stub
						cbDone.call(result);
					}										
				});	
				
			}
			
		});
	}
	}
	
	/**
	 * long now = System.currentTimeMillis();
				long delay = 1000;
				//last time
				long period = measurementPeriod;				
				setupCollection(now+delay, now+delay+period);
	 */
	
	/**
	 * begin,
	 * @param clock
	 */
	private void scheduleStart(long clock){
		
		//we prepare for the future
		restartCollect();
		
		//begin next round,
		EL.get().registerClockTimerCB(clock, 
				new CB0() {
			@Override
			protected void cb(CBResult result) {
				// TODO Auto-generated method stub
				//request to remote nodes				
				startCollect();
		}}, Priority.HIGH);		
	}
	
	/**
	 * end
	 * @param clock
	 */
	private void scheduleEnd(long clock, CB0 cbDone){
		
		/*
		EL.get().registerClockTimerCB(clock, new CB0() {
			@Override
			protected void cb(CBResult result) {
				// TODO Auto-generated method stub
				//request to remote nodes
				terminateCollect(new CB0(){
					@Override
					protected void cb(CBResult result) {
						// TODO Auto-generated method stub
						restartCollection(new CB0(){
							@Override
							protected void cb(CBResult result) {
								// TODO Auto-generated method stub
								cbDone.call(result);
							}
							
						});
					}
				});
				
		};
	},Priority.HIGH);*/
	}
	
	
	public static void main(String[] args){
		
	EndPointControllerPeriod test = new EndPointControllerPeriod(new CB0(){
			@Override
			protected void cb(CBResult result) {
				// TODO Auto-generated method stub				
				log.main(result.toString());
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


	/**
	 * set up between two users
	 */
	void setupMeasureSync(CB0 cbDone) {
		// TODO Auto-generated method stub
		//last timer
		log.main("set up sync");		
		syncDate(synTargetAddress, new CB1<Long>(){
			@Override
			protected void cb(CBResult result, Long commonDate0) {
				switch(result.state){
				case OK:{
					// TODO Auto-generated method stub	
					log.main("sync acked");
					//monotone, timestamp
					long commonDate = Math.max(lastpassiveCommonStartDate,commonDate0);
								
					log.main("my timer: "+myNearestStartDate+", commonDate: "+commonDate);
					myNearestStartDate=commonDate;
					setupCollection(commonDate,commonDate+measurementPeriod,cbDone);			
					break;
				}
				case ERROR:
				case TIMEOUT:{
					//registerReSync(NetAddress.createUnresolved(PassiveCollectorPeriod.target[0], PassiveCollectorPeriod.UDPPort),cbDone);
					cbDone.call(result);
					break;
				}
				
				
				}
				
				
				/*
				if(RDAHost.myNearestStartDate!=commonDate){
					//update
					RDAHost.myNearestStartDate=commonDate;					
					//set up collection
									}else{
					log.main("init postponed");
					cbDone.call(result);
				}*/
				
			}
			
		});
	}
	
}
