package util.bloom.RDA;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Scanner;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import edu.NUDT.control.PassiveCollectorPeriod;
import edu.harvard.syrah.prp.Log;
import util.async.HashMapCache;
import util.async.java.util.concurrent.ConcurrentHashMap;

public class SimpleRDAMeasurementPointSRDA {

	
	static Log log=new Log(SimpleRDAMeasurementPointSRDA.class);
	
	//RDA compute
	 //public SimpleRDATable sender;
	public SimpleRDATable sender;
	 //cache for RDA
	public ConcurrentHashMap<Long,Double> SenderTSTable;
	
	 long maximumTotalPackets=0;

	 /**
	  * complete the parsing
	  */
	public volatile boolean isParsed=false;
	 
		/**
		 * 
		 * @param _row, # rows, 
		 * @param _column
		 * @param sampleProbByColumns
		 */
		public SimpleRDAMeasurementPointSRDA(int _expectedNumEntries){
			
			//sender = new SimpleRDATable(_expectedNumEntries);	
			sender = new SimpleRDATable(_expectedNumEntries);	
			SenderTSTable = new ConcurrentHashMap<Long,Double>();
		}
		
		public SimpleRDAMeasurementPointSRDA(int _expectedNumEntries,long totalPackets,int hashNum){
			
			sender = new SimpleRDATable(_expectedNumEntries);
			
			sender.setHash(hashNum);
					
			SenderTSTable = new ConcurrentHashMap<Long,Double>();
			
			maximumTotalPackets=totalPackets;
			sender.sampleProbability=1;
			
		}
		
		public SimpleRDAMeasurementPointSRDA(int _expectedNumEntries,long totalPackets,double samplingProb){
			
			sender = new SimpleRDATable(_expectedNumEntries);
			
			SenderTSTable = new ConcurrentHashMap<Long,Double>();
			
			maximumTotalPackets=totalPackets;
			sender.sampleProbability=samplingProb;
			
		}
		
		public SimpleRDAMeasurementPointSRDA(int _expectedNumEntries,double samplingProb){
			
			sender = new SimpleRDATable(_expectedNumEntries);
			//receiver = new SimpleRDATable(_expectedNumEntries);

			SenderTSTable = new ConcurrentHashMap<Long,Double>();
			//ReceiverTSTable = new Hashtable<Long,Double>();
			//maximumTotalPackets=totalPackets;
			sender.sampleProbability=samplingProb;
			//receiver.sampleProbability=samplingProb;
		}
		
		/**
		 * new packet arrives at the sender
		 * @param id
		 * @param ts
		 */
		public void packetIncoming(long id, double ts){
			if(SenderTSTable.containsKey(id)){
				//System.err.println("contains key: "+id+", "+ts);
				return;
			}else{
				sender.insert(id, ts,1);
				SenderTSTable.put(id, (ts));
			}
		}
		
		public void packetIncomingSample(long id, double ts){
			if(SenderTSTable.containsKey(id)){
				System.err.println("contains key: "+id+", "+ts);
				return;
			}else{
				boolean inserted = sender.InsertSample(id, ts,1);
				if( inserted){SenderTSTable.put(id, ts);}
			}
		}
		
		/**
		 * ignore duplicate packet
		 * @param id
		 * @param ts
		 * @return
		 */
		public boolean packetIncomingIgnoreDuplicate(long id, 
				double ts){
			if(SenderTSTable.containsKey(id)){
				return false;
			}else{
				sender.insert(id, ts,1);
				SenderTSTable.put(id, ts);
				return true;
			}
		}
		

		
		public int decodeSetDiff(SimpleRDATable receiver){
			/**
			 * subtract, a new TBFTable
			 */
			SimpleRDATable subtract=sender.subtractIBLT(receiver);
			HashSet<Long> SenderItems = new HashSet<Long>(2);
			HashSet<Long> ReceiverItems = new HashSet<Long>(2);
			//decode the ids
			
			int siz = subtract.decodeIDs();
			
			SenderItems.clear();
			ReceiverItems.clear();
			subtract.clear();
			return siz;
		}
		
		public boolean decodeSet(SimpleRDATable receiver,HashSet<Long> SenderItems,HashSet<Long>  ReceiverItems){
			/**
			 * subtract, a new TBFTable
			 */
			
			SimpleRDATable subtract=sender.subtractIBLT(receiver);
			//log.main("subtracted!");
			//decode the ids
			
			boolean decodable=subtract.decodeIDs(SenderItems,ReceiverItems);
			//log.main("decoded!");
		//	SenderItems.clear();
		//	ReceiverItems.clear();
		//	subtract.clear();
			return decodable;
		}
		
		/**
		 * calculate the time
		 * @return
		 */
		public double[] decodeSetDiffWithTime(SimpleRDATable receiver){
			/**
			 * subtract, a new TBFTable
			 */
			HashSet<Long> SenderItems = new HashSet<Long>(2);
			HashSet<Long> ReceiverItems = new HashSet<Long>(2);
			
			long time1=System.nanoTime();
			
			SimpleRDATable subtract=sender.subtractIBLT(receiver);
			long time3 = System.nanoTime();
			
			long delay1=time3-time1;
			//decode the ids
			
			long[] delay2 = subtract.decodeIDsTime();
			
			SenderItems.clear();
			ReceiverItems.clear();
			subtract.clear();
			
			long ts=(delay1+delay2[1])/1000;
			
			double[] returns={delay2[0],ts};
			return returns;
		}
		
		/**
		 * repair the table of the sender and the receiver
		 * @return
		 */
		public void repair(SimpleRDATable receiver,Hashtable<Long,Double> ReceiverTSTable){
			/**
			 * subtract, a new TBFTable
			 */
			SimpleRDATable subtract=sender.subtractIBLT(receiver);
			HashSet<Long> SenderItems = new HashSet<Long>(2);
			HashSet<Long> ReceiverItems = new HashSet<Long>(2);
			//decode the ids
			boolean decodable=subtract.decodeIDs(SenderItems,ReceiverItems);
			System.out.println("decode: "+decodable+",s: "+SenderItems.size()+",r: "+ReceiverItems.size());

			
			//repair the sender
			if(!SenderItems.isEmpty()){
				sender.RepairTBF(SenderItems,new Hashtable<Long,Double>(SenderTSTable));
				//repair(sender,SenderItems,SenderTSTable);
			}
			//repair the receiver
			if(!ReceiverItems.isEmpty()){
				receiver.RepairTBF(ReceiverItems,ReceiverTSTable);
				//repair(receiver,ReceiverItems,ReceiverTSTable);
			}

		}
		
		/**
		 * repair directly
		 * @param SenderItems
		 * @param receiver
		 * @param ReceiverItems
		 * @param ReceiverMiss
		 */
		public void repairDirect(HashSet<Long> SenderItems,SimpleRDATable receiver,HashSet<Long> ReceiverItems,Hashtable<Long,Double> ReceiverMiss){
			/**
			 * subtract, a new TBFTable
			 */
			//SimpleRDATable subtract=sender.subtractIBLT(receiver);
			//HashSet<Long> SenderItems = new HashSet<Long>(2);
			//HashSet<Long> ReceiverItems = new HashSet<Long>(2);
			//decode the ids
			//boolean decodable=subtract.decodeIDs(SenderItems,ReceiverItems);
			//System.out.println("decode: "+decodable+",s: "+SenderItems.size()+",r: "+ReceiverItems.size());
			
			//repair the sender
			if(!SenderItems.isEmpty()){
				sender.RepairTBF(SenderItems,new Hashtable<Long,Double>(SenderTSTable));
				//repair(sender,SenderItems,SenderTSTable);
			}
			//repair the receiver
			if(ReceiverItems!=null&&!ReceiverItems.isEmpty()&&ReceiverMiss!=null&&!ReceiverMiss.isEmpty()){
				receiver.RepairTBF(ReceiverItems,ReceiverMiss);
				//repair(receiver,ReceiverItems,ReceiverTSTable);
			}

		}
		
		/**
		 * vary percent of repaired
		 * @param percent
		 */
		
		
		public double[] repairNow(double percent,SimpleRDATable receiver,Hashtable<Long,Double> ReceiverTSTable){
			/**
			 * subtract, a new TBFTable
			 */
			SimpleRDATable subtract=sender.subtractIBLT(receiver);
			HashSet<Long> SenderItems = new HashSet<Long>(2);
			HashSet<Long> ReceiverItems = new HashSet<Long>(2);
			//decode the ids
			double t1=System.currentTimeMillis();
			boolean decodable=subtract.decodeIDs(SenderItems,ReceiverItems);
			double delay = System.currentTimeMillis()-t1;
			System.out.println("decode: "+decodable+",s: "+SenderItems.size()+",r: "+ReceiverItems.size());

			
			
			//repair the sender
			if(!SenderItems.isEmpty()){
				
				Random r =new Random(System.currentTimeMillis());	
				
				Iterator<Long> ier = SenderItems.iterator();
				while(ier.hasNext()){
					long id = ier.next();
					//sender
					if(SenderTSTable.containsKey(id)){
						
						
							//remove at sender
							double ts = SenderTSTable.get(id);				
							sender.erase(id,ts,1);
							
							//receiver, remove directly
							if(ReceiverTSTable.containsKey(id)){
								System.err.println("wrong! "+id);
								ts=ReceiverTSTable.get(id);
								receiver.erase(id, ts, 1);
							}
							
					}
					
				}
				
				ier=ReceiverItems.iterator();
				Long id;
				while(ier.hasNext()){
					id=ier.next();
					
					if(ReceiverTSTable.containsKey(id)){
					
						//remove at sender
						double ts = ReceiverTSTable.get(id);				
						receiver.erase(id,ts,1);
						
						//receiver, remove directly
						if(SenderTSTable.containsKey(id)){	
							System.err.println("wrong! "+id);
							ts=SenderTSTable.get(id);
							sender.erase(id, ts, 1);
						}
						
					
					}
					
				}

				}
				
				
		
			double good = 0;
			for(int i=0;i<sender.hashTable.length;i++){
				if(//sender.hashTable[i].keyCheck==receiver.hashTable[i].keyCheck&&
						sender.hashTable[i].Counter>0&&
						sender.hashTable[i].keySum==receiver.hashTable[i].keySum){
					good+=sender.hashTable[i].Counter;
				}
			}
			
			
			double[] result={good/sender.N_HASH,delay};

			return result;
		}
		
		public double repair(double percent,SimpleRDATable receiver,Hashtable<Long,Double> ReceiverTSTable){
			/**
			 * subtract, a new TBFTable
			 */
			SimpleRDATable subtract=sender.subtractIBLT(receiver);
			HashSet<Long> SenderItems = new HashSet<Long>(2);
			HashSet<Long> ReceiverItems = new HashSet<Long>(2);
			//decode the ids
			
			boolean decodable=subtract.decodeIDs(SenderItems,ReceiverItems);
			System.out.println("decode: "+decodable+",s: "+SenderItems.size()+",r: "+ReceiverItems.size());

			
			
			//repair the sender
			if(!SenderItems.isEmpty()){
				
				Random r =new Random(System.currentTimeMillis());	
				
				Iterator<Long> ier = SenderItems.iterator();
				while(ier.hasNext()){
					long id = ier.next();
					//sender
					if(SenderTSTable.containsKey(id)){
						
						if(r.nextFloat()>percent){
							continue;							
						}else{
							//remove at sender
							double ts = SenderTSTable.get(id);				
							sender.erase(id,ts,1);
							
							//receiver, remove directly
							/*if(ReceiverTSTable.containsKey(id)){								
								ts=ReceiverTSTable.get(id);
								receiver.erease(id, ts, 1);
							}*/
						}	
					}
					
				}
				
				ier=ReceiverItems.iterator();
				Long id;
				while(ier.hasNext()){
					id=ier.next();
					
					if(ReceiverTSTable.containsKey(id)){
					if(r.nextFloat()>percent){
						continue;							
					}else{
						//remove at sender
						double ts = ReceiverTSTable.get(id);				
						receiver.erase(id,ts,1);
						
						//receiver, remove directly
						/*if(SenderTSTable.containsKey(id)){								
							ts=SenderTSTable.get(id);
							sender.erease(id, ts, 1);
						}*/
					}	
					
					}
					
				}

				}
				
				
		
			double good = 0;
			for(int i=0;i<sender.hashTable.length;i++){
				if(//sender.hashTable[i].keyCheck==receiver.hashTable[i].keyCheck&&
						sender.hashTable[i].Counter>0&&
						sender.hashTable[i].keySum==receiver.hashTable[i].keySum){
					good+=sender.hashTable[i].Counter;
				}
			}
			
			
			return good/sender.N_HASH;

		}
		
		/**
		 * average number
		 * @return
		 */
		public double getAverage(SimpleRDATable receiver){	
			return sender.getAvgTS(receiver);
		}
		
		
		/**
		 * StandardDeviation
		 * @param avg, average number
		 * @return
		 */
		public double getStandardDeviation(double avg,SimpleRDATable receiver,Hashtable<Long,Double> ReceiverTSTable){			
			return sender.getSTD(receiver, avg);
			
			//return receiver.getVarianceStatistics(sender, avg);
		}

		/**
		 * get standard deviation
		 * @param avg
		 * @param receiver
		 * @return
		 */
		public double getStandardDeviation(double avg,SimpleRDATable receiver){			
			return sender.getSTD(receiver, avg);
			
			//return receiver.getVarianceStatistics(sender, avg);
		}
		public String getBucketSize(SimpleRDATable receiver) {
			// TODO Auto-generated method stub
			return sender.expectedNumEntries+" "+receiver.expectedNumEntries+" ";
		}

		public void clear() {
			// TODO Auto-generated method stub
			isParsed = false;
			SenderTSTable.clear();
			//ReceiverTSTable.clear();
			//SenderTSTable=null;
			//ReceiverTSTable=null;
			sender.clear();
			//receiver.clear();
		}

		public double getGoodPackets(SimpleRDATable receiver) {
			// TODO Auto-generated method stub
			return sender.getGoodPackets(receiver);
		}

		/**
		 * parse the records
		 * into the cache and ts
		 * @param senderTSTable2
		 */
		public void packetIncoming(HashMapCache<Long, Long> cache) {
			// TODO Auto-generated method stub
			log.main("stored: "+cache.size());
			for(Entry<Long, Long> e:cache.entrySet()){
				long id = e.getKey();
				double ts = PassiveCollectorPeriod.scaleTS*e.getValue();//(PassiveRDAHost.scaleTS*(e.getValue()-PassiveRDAHost.TSConstant));
				SenderTSTable.put(id,ts);
				sender.insert(id, ts,1);
			}
		}
		
		public void packetIncoming(AbstractMap<Long, Long> cache) {
			// TODO Auto-generated method stub
			log.main("stored: "+cache.size());
			for(Entry<Long, Long> e:cache.entrySet()){
				long id = e.getKey();
				double ts = PassiveCollectorPeriod.scaleTS*e.getValue();//(PassiveRDAHost.scaleTS*(e.getValue()-PassiveRDAHost.TSConstant));
				SenderTSTable.put(id,ts);
				sender.insert(id, ts,1);
			}
		}

		/**
		 * compute directly
		 * @param receiverTSTable
		 * @return 
		 */
		public double[] computeDirect(Hashtable<Long, Double> receiverTSTable) {
			// TODO Auto-generated method stub
			/**
			 * read the trace
			 * return the target
			 */				
				double Ak=0;
				double AkMinusOne=0;
				double Qk=0;
				double squareRTTSum=0;
				double counter=0;
				double xk=0;
				//				
				SetView<Long> keys = Sets.intersection(SenderTSTable.keySet(),receiverTSTable.keySet());
				int i=0;
				double[]TS = {0,0};
				if(!keys.isEmpty()){
					Iterator<Long> ier = keys.iterator();
					while(ier.hasNext()){
						Long nxt = ier.next();
						TS[0]=SenderTSTable.get(nxt);
						TS[1]=receiverTSTable.get(nxt);
						 //System.out.println("line: "+lines + ", "+timestamp);
				         
				         if(Double.isInfinite(TS[0])||Double.isNaN(TS[0])||
				        		 Double.isInfinite(TS[1])||Double.isNaN(TS[1])){								
								continue;
							}				         
							//good							
							//average over the delay
							AkMinusOne = Ak;
							xk=Math.abs(TS[0]-TS[1]);
							//average
							Ak=RDAHostSimpleLossReorder.onePassAvg(i+1,Ak,xk);
							//standard deviation
							Qk=RDAHostSimpleLossReorder.onePassStandardDeviation(i+1, AkMinusOne, Ak, Qk, xk);
							i++;
							counter+=1;
							//square
							squareRTTSum+=Math.pow(xk,2); 
					}
					
				}
				//keys.clear();
				//keys=null;
				/**
						 * true
						 */
						double avg=Ak;
						double standardDeviation=Math.abs(Qk/counter);
						double squareSTD=Math.abs(squareRTTSum/counter-Math.pow(avg,2));
						//log.main("#Good: "+i+", avg: "+avg+", standardDeviation: "+standardDeviation+"\n"+", squareSTD: "+squareSTD);
						double[] result={avg,squareSTD};
						return result;
		}
}
