package edu.NUDT.control;


import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.List;

import org.pcap4j.core.NotOpenException;
import org.pcap4j.core.PacketListener;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.core.PcapNetworkInterface;
import org.pcap4j.core.Pcaps;
import org.pcap4j.packet.EthernetPacket;
import org.pcap4j.packet.IpPacket;
import org.pcap4j.packet.IpPacket.IpHeader;
import org.pcap4j.packet.Packet;
import org.pcap4j.packet.TcpPacket;
import org.pcap4j.packet.Packet.Header;
import org.pcap4j.packet.TcpPacket.TcpHeader;
import org.pcap4j.core.PcapNetworkInterface.PromiscuousMode;

import edu.harvard.syrah.prp.Log;
import edu.harvard.syrah.prp.NetUtil;
import edu.harvard.syrah.prp.POut;
import util.async.HashMapCache;
import util.async.UniformHashFunc;
import util.bloom.RDA.PassiveRDAHost;

/**
 * control the passive collector
 */
public class PassiveCollector {

	Log log = new Log( PassiveCollector.class);
	
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
	HashMapCache<Long,Long> SenderTSTable; 
	/**
	 * host
	 */
	private PassiveRDAHost RDAHost; 
	
	private PacketListener  listener;
	/**
	 * maximum packets
	 */
	private long  MaximumPackets= -1;
	
	
	public PassiveCollector(){
		
		RDAHost=new PassiveRDAHost();		
		MaximumPackets = PassiveCollectorPeriod.TotalPackets;
		SenderTSTable = new HashMapCache<Long,Long>((int)MaximumPackets);
		/**
		 * init the pcap
		 */
		initPcap();
	}
	
	/**
	 * init the data structure
	 * @param maxPacket
	 */
	public void initPcap(){
		
		//log.main("init pcap");
		 /***************************************************************************
	     * Second we open up the selected device
	     **************************************************************************/
	 
	    /***************************************************************************
	     * Fouth we create a packet handler which receives packets and tells the 
	     * dumper to write those packets to its output file
	     **************************************************************************/
	   
		
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
		    				//not local
		    				if(nxt.getName().equalsIgnoreCase(PassiveCollectorPeriod.netcard)){
		    					try {
									dumpHandler =nxt.openLive(snaplen, PromiscuousMode.PROMISCUOUS,  timeout);
									break;
								} catch (PcapNativeException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
		    				}
		    			}
		    		}
				} catch (PcapNativeException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

	    
	    listener = new PacketListener(){
			@Override
			public void gotPacket(Packet packet) {
				// TODO Auto-generated method stub
				//log.main(packet.toString());			
				if(packet.contains(IpPacket.class)&&packet.contains(TcpPacket.class)){
					IpPacket ipPkt = packet.get(IpPacket.class);
					IpHeader ipHeader = ipPkt.getHeader();
					 byte[] src = ipHeader.getSrcAddr().getAddress();
					 byte[] dst = ipHeader.getDstAddr().getAddress();
					byte[] a =NetUtil.stringIPToByteIP(PassiveCollectorPeriod.myRegion[0]);
					byte[] b = NetUtil.stringIPToByteIP(PassiveCollectorPeriod.target[0]);
					//filter, only record the output to the destination values
					if((testEqual(src,a)&&testEqual(dst,b))||(testEqual(src,b)&&testEqual(dst,a))){
						int ts = dumpHandler.getTimestamp().getNanos();	
						TcpPacket tcpPkt = packet.get(TcpPacket.class);
						TcpHeader header = tcpPkt.getHeader();
						//long t1 = header.getSequenceNumberAsLong()^header.getAcknowledgmentNumberAsLong();
						//long t2 = Long.valueOf(header.getWindowAsInt())^Long.valueOf(header.getSrcPort().valueAsInt())^Long.valueOf(header.getDstPort().valueAsInt());
						//long hashCode =  UniformHashFunc.os_dietz8to3(t1, t2);
						long hashCode;
						try {
							hashCode = PassiveRDAHost.MapFromTCP2HashID(header.toHexString());
							log.main("ts: "+ts+", hash: "+hashCode);
							SenderTSTable.put(hashCode,Long.valueOf(ts));
						} catch (UnsupportedEncodingException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}				
				}	   			
	}
	    };
	    
	}
	    

		/**
		 * two address point to the same IP
		 */
		public static boolean testEqual(byte[] byteA,byte[] byteB){
			for(int i = 0; i<byteA.length;i++){				
				byte tmp =(byte) (byteA[i] ^ byteB[i]);
				if(tmp != (byte)(0x00)){
					return false;
				}
			}
			return true;
		}
		
//	/**
//	 * dump to the file
//	 */
//	public void pcapDump(String ofile, int count){
//	   	    
//	    /***************************************************************************
//	     * we create a PcapDumper and associate it with the pcap capture
//	     ***************************************************************************/
//	    //String ofile = "tmp-capture-file.cap";
//	    PcapDumper dumper = pcap.dumpOpen(ofile); // output file
//
//	    /***************************************************************************
//	     * Fouth we create a packet handler which receives packets and tells the 
//	     * dumper to write those packets to its output file
//	     **************************************************************************/
//	    PcapHandler<PcapDumper> dumpHandler = new PcapHandler<PcapDumper>() {
//
//	      public void nextPacket(PcapDumper dumper, long seconds, int useconds,
//	        int caplen, int len, ByteBuffer buffer) {
//
//	        dumper.dump(seconds, useconds, caplen, len, buffer);
//	      }
//	    };
//
//	    /***************************************************************************
//	     * Fifth we enter the loop and tell it to capture 10 packets. We pass
//	     * in the dumper created in step 3
//	     **************************************************************************/
//	    pcap.loop(count, dumpHandler, dumper);
//	                
//	    File file = new File(ofile);
//	    System.out.printf("%s file has %d bytes in it!\n", ofile, file.length());
//	                
//	    /***************************************************************************
//	     * Last thing to do is close the dumper and pcap handles
//	     **************************************************************************/
//	    dumper.close(); // Won't be able to delete without explicit close
//	    pcap.close();
//	                
//	    if (file.exists()) {
//	      file.delete(); // Cleanup
//	    }
//	}
	
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
			dumpHandler.loop(count, listener);
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
	
	public void close(){
          
/***************************************************************************
* Last thing to do is close the dumper and pcap handles
**************************************************************************/   
		dumpHandler.close();
	}
	
	/**
	 * stop
	 * call the end point, stop the measurement
	 */
	public void terminateCollect(){
			try {
				dumpHandler.breakLoop();				
				dumpHandler.close();
				log.main("terminate!");
				startRDAProcess();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
			

	}
	
	
	/**
	 * RDA process
	 */
	private void startRDAProcess() {
		// TODO Auto-generated method stub
		RDAHost.parseRecordsCache(SenderTSTable);
	}



	/**
	 * start the measure:
	 * call the end point, to begin the collection at time	
	 */
	public void startCollect(){		
		this.
		pcapStore2Hash(Long.valueOf(MaximumPackets).intValue());
	}
	
	/**
	 * get the hash
	 * @return
	 */
	public HashMapCache<Long, Long> getSenderTSTable() {
		return SenderTSTable;
	}

	public boolean restartCollect() {
		// TODO Auto-generated method stub
		return true;
	}

	
}
