package se.fnord;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static se.fnord.Protocols.stack;
import static se.fnord.TcpSessionEvent.Direction.FROM_SERVER;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import se.fnord.FilterFactory;
import se.fnord.FilterFunction;
import se.fnord.IteratorFactory;
import se.fnord.PayloadFrame;
import se.fnord.PcapReader;
import se.fnord.PcapRecord;
import se.fnord.Protocols;
import se.fnord.TcpAssembler;
import se.fnord.TcpDecoder;
import se.fnord.TcpFrame;
import se.fnord.TcpFrame.TcpFlag;
import se.fnord.TcpSessionData;
import se.fnord.TcpSessionEvent;
import se.fnord.TcpSessionId;
import se.fnord.TcpSessionMissingData;
import se.fnord.decoder.EthernetDecoder;
import se.fnord.decoder.Ipv4Decoder;
import se.fnord.decoder.Ipv4Frame;
import se.fnord.decoder.SLLDecoder;
import util.async.MathUtil;
import util.async.Util;
import util.bloom.Apache.Key;


public class TcpExtract {

	private static IteratorFactory<PcapRecord, TcpSessionEvent> decoder(final int serverPort) {
		IteratorFactory<PcapRecord, PayloadFrame> eth =
		    Protocols.<PcapRecord, PayloadFrame> select().addProtocol(1, new EthernetDecoder<PcapRecord>())
		        .addProtocol(113, new SLLDecoder<PcapRecord>()).build();

		IteratorFactory<PayloadFrame, Ipv4Frame> ip =
		    Protocols.<PayloadFrame, Ipv4Frame> select().addProtocol(0x0800, new Ipv4Decoder<>()).build();

		IteratorFactory<Ipv4Frame, TcpFrame> tcp =
		    Protocols.<Ipv4Frame, TcpFrame> select().addProtocol(6, new TcpDecoder<Ipv4Frame>()).build();

		IteratorFactory<TcpFrame, TcpSessionEvent> tcpSession =
		    stack(new TcpAssembler(), new FilterFactory<>(new FilterFunction<TcpSessionEvent, TcpSessionEvent>() {
			    @Override
			    public boolean test(TcpSessionEvent from) {
				    // return from.session().serverPort() == serverPort;
				    return true;
			    }
		    }));
		return stack(eth, ip, tcp, tcpSession);
	}

	private static final ByteBuffer MISSING_DATA = ByteBuffer.wrap("XXX".getBytes(Charset.forName("UTF-8")));

	private static void extractPackets(Path destination, Iterable<TcpSessionEvent> events) throws IOException {
		for (TcpSessionEvent f : events) {
			if (f instanceof TcpSessionData) {
				TcpSessionData d = (TcpSessionData) f;
				FileChannel w = FileChannel.open(destination.resolve(formatName("packet", d)), TRUNCATE_EXISTING, WRITE, CREATE);
				w.write(d.payload());
				w.close();
			}
		}
	}

	private static void extractStream(Path destination, Iterable<TcpSessionEvent> events) throws IOException {
		Map<TcpSessionId, FileChannel> streams_to_client = new HashMap<>();
		Map<TcpSessionId, FileChannel> streams_to_server = new HashMap<>();
		for (TcpSessionEvent f : events) {
			Map<TcpSessionId, FileChannel> streams = f.direction() == FROM_SERVER ? streams_to_client : streams_to_server;
			FileChannel w = streams.get(f.session());

			if (f instanceof TcpSessionData) {
				if (w == null) {
					w = FileChannel.open(destination.resolve(formatName("stream", f)), TRUNCATE_EXISTING, WRITE, CREATE);
					streams.put(f.session(), w);
				}
				TcpSessionData d = (TcpSessionData) f;
				ByteBuffer sendBuffer=ByteBuffer.wrap(d.session().toString().getBytes("UTF-8"));
				w.write(sendBuffer);
				//store payload
				//w.write(d.payload());
			}
			else if (f instanceof TcpSessionMissingData) {
				if (w != null)
					w.write(MISSING_DATA.slice());
			}
		}

		for (FileChannel c : streams_to_client.values())
			c.close();
		for (FileChannel c : streams_to_server.values())
			c.close();

	}

	private static String formatName(String type, TcpSessionEvent f) {
		return String.format("%s-%d-%d-%d-%s.txt", type, f.index(), f.session().clientPort(), f.session().serverPort(),
		    f.direction());
	}

	private static String formatNameUnion(String type, TcpSessionEvent f) {
		return String.format("%s-%d-%d-%s.txt", type, f.session().clientPort(), f.session().serverPort(),
		    f.direction());
	}
	
	
	public static void main00(String[] args) throws IOException {
		//pcap file dir
		PcapReader reader = PcapReader.create(Paths.get(args[0]));
		Path destination = Paths.get("tmp");
		extractStream(destination, reader.decodeAs(decoder(0)));
	}

	/**
	 * ericfu
	 * @param reader
	 * @param destination
	 * @throws IOException 
	 */
	public static void parseTCP(PcapReader reader,Path destination) throws IOException{
		
		IteratorFactory<PcapRecord, TcpSessionEvent> dec = decoder(3996);
		Iterable<TcpSessionEvent> events = reader.decodeAs(dec);
		Map<TcpSessionId, FileChannel> streams_to_client = new HashMap<>();
		Map<TcpSessionId, FileChannel> streams_to_server = new HashMap<>();

		
		
		for (TcpSessionEvent f : events) {
		
		Map<TcpSessionId, FileChannel> streams = f.direction() == FROM_SERVER ? streams_to_client : streams_to_server;
		FileChannel w = streams.get(f.session());

		if (f instanceof TcpSessionData) {
			if (w == null) {
				w = FileChannel.open(destination, TRUNCATE_EXISTING, WRITE, CREATE);
				streams.put(f.session(), w);
			}
			TcpSessionData d = (TcpSessionData) f;
			long ts=d.rootFrame().timestamp();
			TcpFrame tcpFrame1 = (TcpFrame)d.parentFrame();
			long seq = tcpFrame1.sequence();
			long ack=tcpFrame1.ackNumber();
			int check = tcpFrame1.checkSum();
			int PacketLen = tcpFrame1.originalLength();
			
			ByteBuffer sendBuffer=ByteBuffer.wrap((ts+" "+seq+" "+ack+" "+check+" "+d.session().toString()+" "+PacketLen+"\n").getBytes("UTF-8"));
			w.write(sendBuffer);
			//store payload
			//w.write(d.payload());
		}
		else if (f instanceof TcpSessionMissingData) {
			if (w != null)
				w.write(MISSING_DATA.slice());
		}
	}
		
		Iterator<PcapRecord> ier = reader.iterator();
		while(ier.hasNext()){
			PcapRecord rec = ier.next();
			long ts = rec.timestamp();
			System.out.print(ts+" ");
		}
	


		for (FileChannel c : streams_to_client.values())
			c.close();
		for (FileChannel c : streams_to_server.values())
			c.close();
		
	}

	/*
	 * 
	 * loss��������ͬ���ش����ݰ���������
		reorder����������һ�����������䣬���յ���������������ݰ��ı�����
		duplicate�������ش������ݰ���������

	 */
	public static void AnalyzeTrace(PcapReader reader,String destination) throws IOException{
		IteratorFactory<PcapRecord, TcpSessionEvent> dec = decoder(3996);
		Iterable<TcpSessionEvent> events = reader.decodeAs(dec);
		//Map<TcpSessionId, FileWriter> streams_to_client = new HashMap<>();
		//Map<TcpSessionId, FileWriter> streams_to_server = new HashMap<>();
		//packet id, ts
		ConcurrentHashMap<String,ConcurrentHashMap<Long,Long>> PacketMap = new ConcurrentHashMap<String,ConcurrentHashMap<Long,Long>>();
		PacketMap.clear();
		
		ConcurrentHashMap<Long,Integer> DupPacketMap = new ConcurrentHashMap<Long,Integer>();
		
		
		double duplicateCount=0;
		//double reorderedCount=0;
		//double lossCount=0;
		double packets=0;
		FileWriter w0 = null,w2=null;
		//long counter=0;
		if (w2 == null) {
			w2 = new FileWriter(destination+"PktSize");//FileChannel.open(destination, TRUNCATE_EXISTING, WRITE, CREATE);
			//streams.put(f.session(), w);
		}
		if (w0 == null) {
			w0 = new FileWriter(destination+"Duplicate");//FileChannel.open(destination, TRUNCATE_EXISTING, WRITE, CREATE);
			//streams.put(f.session(), w);
		}
		
		for (TcpSessionEvent f : events) {
		
		//Map<TcpSessionId, FileWriter> streams = f.direction() == FROM_SERVER ? streams_to_client : streams_to_server;
		//FileWriter w = streams.get(f.session());

		if (f instanceof TcpSessionData) {
						

			//session id, key
			//(seqNum, len) hashSet;
			//ack,���seqNum+len���ж��Ƿ���Ҫɾ��
			
			TcpSessionData d = (TcpSessionData) f;
			
			if(!PacketMap.containsKey(d.session().toString())){
				PacketMap.put(d.session().toString(), new ConcurrentHashMap<Long,Long>());
			}
			ConcurrentHashMap<Long, Long> SessionMap = PacketMap.get(d.session().toString());
			
			long ts=d.rootFrame().timestamp();
			TcpFrame tcpFrame1 = (TcpFrame)d.parentFrame();
			long seq = tcpFrame1.sequence();
			long ack=tcpFrame1.ackNumber();
			int check = tcpFrame1.checkSum();
			int PacketLen = tcpFrame1.originalLength();//.originalLength();
			
			//sender
			//if(!tcpFrame1.flags().contains(TcpFlag.ACK)){
			
			if(f.direction() == FROM_SERVER){
				//ack packet from the other side
				
				//if(tcpFrame1.flags().contains(TcpFlag.SYN)&&)
								
				//sender	
				//StringBuffer sbNextSequenceNumber= new StringBuffer();
				//sbNextSequenceNumber.append(seq);
				//sbNextSequenceNumber.append(check);
				//long vv=seq+PacketLen;
				//sbNextSequenceNumber.append(vv);
				
				//skip, if this packet's len=0; 
				//is an ack, or fast ack 
				if(PacketLen==0){
					continue;
				}
				
				//this packet has been inserted
				if(SessionMap.containsKey(seq)){
					System.out.println("===================\n"+"Duplicate! a key found"+"===================\n");
					duplicateCount++;
					if(!DupPacketMap.containsKey(seq)){
						DupPacketMap.put(seq,1);
					}
					int v=DupPacketMap.get(seq);
					DupPacketMap.put(seq,Integer.valueOf(v+1));
				}else{
					//record the packet
					System.out.println("===================\n"+"NEW! a key inserted"+"===================\n");					
					SessionMap.put(seq,ts);
					//PacketMap.put(d.session().toString(),SessionMap);
				}
				
			}else{
				//from me
				
				//StringBuffer sbSender= new StringBuffer();
				
				//sbSender.append(ack);
				
				//if ack correct, cumulative ack
				if(PacketMap.containsKey(d.session().toString())){
					
					//cumulative ack
					Iterator<Entry<Long, Long>> ier = PacketMap.get(d.session().toString()).entrySet().iterator();
					while(ier.hasNext()){
						System.out.println("ack a packet! the data packet");
						Entry<Long, Long> tx = ier.next();
						//remove all packets whose sequence number
						if(tx.getKey()<=ack){
							ier.remove();
						}
					}
				}else{
					System.err.println("ack an unseen packet");
				}
				
				//sack parse;
				//parse the sack range; from the tcp options;
				//
				
			}
						
/*			}else{
				//ack packet from the other side
				StringBuffer sbSender= new StringBuffer();
				sbSender.append(d.session().toString());
				sbSender.append(ack);
				
				//if ack correct
				if(PacketMap.containsKey(sbSender.toString())){
					System.out.println("ack a packet!");
					int repeat=PacketMap.remove(sbSender.toString());
					if(repeat>1){
						//repeated packet
						String str=(d.session().toString()+" "+sbSender.toString()+" "+repeat+"\n");
						

						
						w0.write(str);
						w0.flush();
					}
				}else{
					//System.err.println("ack an unseen packet"+sbSender.toString());
				}
				
			}*/
			Iterator<ConcurrentHashMap<Long, Long>> ier = PacketMap.values().iterator();
			long size=0;
			while(ier.hasNext()){
				size+=ier.next().size();
			}
			String str=ts+" "+size;
			System.out.println(str);
			w2.write(str+"\n");
			w2.flush();
			//String str=(ts+" "+seq+" "+ack+" "+check+" "+d.session().toString()+"\n");
			//System.out.println(str);
			
			//counter++;
			//ByteBuffer sendBuffer=ByteBuffer.wrap(str.getBytes("UTF-8"));
			//System.out.println(str);
			//w.write(sendBuffer);
			//store payload
			//w.write(d.payload());
			
			packets++;
		}
//		else if (f instanceof TcpSessionMissingData) {
//			if (w != null)
//				w.append(MISSING_DATA.slice());
//		}
	}
		
		Iterator<ConcurrentHashMap<Long, Long>> ier0 = PacketMap.values().iterator();
		long size=0;
		while(ier0.hasNext()){
			size+=ier0.next().size();
		}
		
		String duplicated="duplicate: "+duplicateCount;
		String loss="loss: "+size;
		String total="total: "+packets;
		
		w2.write(duplicated+" "+loss+" "+total+"\n");
		w2.flush();
		 Iterator<Entry<Long, Integer>> ier = DupPacketMap.entrySet().iterator();
		while(ier.hasNext()){
			Entry<Long, Integer> s = ier.next();
			w0.write(s.getKey()+" "+s.getValue()+"\n");
			w0.flush();
		}
		w0.close();
		w2.close();
	}
	
	public static long parseHolisticTCP(PcapReader reader,String destination) throws IOException{
		
		IteratorFactory<PcapRecord, TcpSessionEvent> dec = decoder(3996);
		Iterable<TcpSessionEvent> events = reader.decodeAs(dec);
		//Map<TcpSessionId, FileWriter> streams_to_client = new HashMap<>();
		//Map<TcpSessionId, FileWriter> streams_to_server = new HashMap<>();

		FileWriter w = null;
		long counter=0;
		for (TcpSessionEvent f : events) {
		
		//Map<TcpSessionId, FileWriter> streams = f.direction() == FROM_SERVER ? streams_to_client : streams_to_server;
		//FileWriter w = streams.get(f.session());

		if (f instanceof TcpSessionData) {
			if (w == null) {
				w = new FileWriter(destination);//FileChannel.open(destination, TRUNCATE_EXISTING, WRITE, CREATE);
				//streams.put(f.session(), w);
			}
			TcpSessionData d = (TcpSessionData) f;
			long ts=d.rootFrame().timestamp();
			TcpFrame tcpFrame1 = (TcpFrame)d.parentFrame();
			long seq = tcpFrame1.sequence();
			long ack=tcpFrame1.ackNumber();
			int check = tcpFrame1.checkSum();
			int PacketLen = tcpFrame1.originalLength()-6;
			String str=(ts+" "+seq+" "+ack+" "+check+" "+d.session().toString()+" "+PacketLen+" "+f.direction()+"\n");
			//System.out.println(str);
			w.write(str);
			w.flush();
			counter++;
			//ByteBuffer sendBuffer=ByteBuffer.wrap(str.getBytes("UTF-8"));
			//System.out.println(str);
			//w.write(sendBuffer);
			//store payload
			//w.write(d.payload());
		}
//		else if (f instanceof TcpSessionMissingData) {
//			if (w != null)
//				w.append(MISSING_DATA.slice());
//		}
	}

		if(w!=null){
			w.close();
		}
		return counter;
	}
	
	
	public static void parseHolisticTCP00(PcapReader reader,Path destination) throws IOException{
		
		IteratorFactory<PcapRecord, TcpSessionEvent> dec = decoder(3996);
		Iterable<TcpSessionEvent> events = reader.decodeAs(dec);
		Map<TcpSessionId, FileChannel> streams_to_client = new HashMap<>();
		Map<TcpSessionId, FileChannel> streams_to_server = new HashMap<>();
		System.out.println(destination.toAbsolutePath());
		
		//FileChannel w = FileChannel.open(destination, TRUNCATE_EXISTING, WRITE, CREATE);
		
		
		for (TcpSessionEvent f : events) {
		
		Map<TcpSessionId, FileChannel> streams = f.direction() == FROM_SERVER ? streams_to_client : streams_to_server;
		FileChannel w = streams.get(f.session());

		if (f instanceof TcpSessionData) {
			if (w == null) {
				//w = FileChannel.open(destination.resolve(formatNameUnion("stream", f)), TRUNCATE_EXISTING, WRITE, CREATE);
				w = FileChannel.open(destination, TRUNCATE_EXISTING, WRITE, CREATE);

				streams.put(f.session(), w);
				//w = FileChannel.open(destination, TRUNCATE_EXISTING, WRITE, CREATE);
			}
			
			TcpSessionData d = (TcpSessionData) f;
			System.out.println(d.toString());
			
			long ts=d.rootFrame().timestamp();
			TcpFrame tcpFrame1 = (TcpFrame)d.parentFrame();
			long seq = tcpFrame1.sequence();
			long ack=tcpFrame1.ackNumber();
			int check = tcpFrame1.checkSum();
			ByteBuffer sendBuffer=ByteBuffer.wrap((ts+" "+seq+" "+ack+" "+check+" "+d.session().toString()+"\n").getBytes("UTF-8"));
			w.write(sendBuffer);
			w.force(true);
			
			//store payload
			//w.write(d.payload());
		}
//		else if (f instanceof TcpSessionMissingData) {
//			if (w != null)
//				w.write(MISSING_DATA.slice());
//		}
	}
		
//		Iterator<PcapRecord> ier = reader.iterator();
//		while(ier.hasNext()){
//			PcapRecord rec = ier.next();
//			long ts = rec.timestamp();
//			System.out.print(ts+" ");
//		}
	


		for (FileChannel c : streams_to_client.values())
			c.close();
		for (FileChannel c : streams_to_server.values())
			c.close();
		
	}
	
	
	public static void mainParse(String[] args) throws IOException {
		Path addr = Paths.get(args[0]);
		
		PcapReader reader = PcapReader.create(addr);
		String destination = (addr.toAbsolutePath().toString()+"HolisticTCP");
		long counter = parseHolisticTCP(reader,destination);
		System.out.println("$counter"+counter);
		//parseTCP(reader,destination);
	}
	
	
	public static void main(String[] args) throws IOException {
		
		try{
			
		int choice = Integer.parseInt(args[2]);	
		if(choice==0){
		//generate trace for analysis
			mainParse(args);
		}else  if(choice==2){
			Path addr = Paths.get(args[0]);
			PcapReader reader = PcapReader.create(addr);
			String destination = (addr.toAbsolutePath().toString()+"AnalyzeTrace");
			AnalyzeTrace(reader, destination);
		}else if(choice ==3){
			Path addr = Paths.get(args[0]);
			PcapReader reader = PcapReader.create(addr);
			//String destination = (addr.toAbsolutePath().toString()+"TCP");
			parseHolisticTCP00(reader,addr);
		}

		}catch(Exception e){
			e.printStackTrace();
		}
		//PcapReader reader = PcapReader.create(Paths.get(args[0]));
		//Path destination = Paths.get("tmp");
		//parseTCP(reader,destination);

	}
}
