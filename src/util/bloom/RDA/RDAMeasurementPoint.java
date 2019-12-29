package util.bloom.RDA;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;

public class RDAMeasurementPoint {

	 RDATable sender;
	 Hashtable<Long,Double> SenderTSTable;
	
	 long maximumTotalPackets=0;
	 
		/**
		 * 
		 * @param _row, # rows, 
		 * @param _column
		 * @param sampleProbByColumns
		 */
		public RDAMeasurementPoint(int _expectedNumEntries){
			
			sender = new RDATable(_expectedNumEntries);	
			SenderTSTable = new Hashtable<Long,Double>();
		}
		
		public RDAMeasurementPoint(int _expectedNumEntries,long totalPackets,int hashNum){
			
			sender = new RDATable(_expectedNumEntries);
			
			sender.setHash(hashNum);
					
			SenderTSTable = new Hashtable<Long,Double>();
			
			maximumTotalPackets=totalPackets;
			sender.sampleProbability=1;
			
		}
		
		public RDAMeasurementPoint(int _expectedNumEntries,long totalPackets,double samplingProb){
			
			sender = new RDATable(_expectedNumEntries);
			
			SenderTSTable = new Hashtable<Long,Double>();
			
			maximumTotalPackets=totalPackets;
			sender.sampleProbability=samplingProb;
			
		}
		
		public RDAMeasurementPoint(int _expectedNumEntries,double samplingProb){
			
			sender = new RDATable(_expectedNumEntries);
			//receiver = new RDATable(_expectedNumEntries);

			SenderTSTable = new Hashtable<Long,Double>();
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
				System.err.println("contains key: "+id+", "+ts);
				return;
			}else{
				sender.insert(id, ts,1);
				SenderTSTable.put(id, ts);
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
		

		
		public int decodeSetDiff(RDATable receiver){
			/**
			 * subtract, a new TBFTable
			 */
			RDATable subtract=sender.subtractIBLT(receiver);
			HashSet<Long> SenderItems = new HashSet<Long>(2);
			HashSet<Long> ReceiverItems = new HashSet<Long>(2);
			//decode the ids
			
			int siz = subtract.decodeIDs();
			
			SenderItems.clear();
			ReceiverItems.clear();
			subtract.clear();
			return siz;
		}
		
		public boolean decodeSet(RDATable receiver,HashSet<Long> SenderItems,HashSet<Long>  ReceiverItems){
			/**
			 * subtract, a new TBFTable
			 */
			RDATable subtract=sender.subtractIBLT(receiver);

			//decode the ids
			
			boolean decodable=subtract.decodeIDs(SenderItems,ReceiverItems);
			
		//	SenderItems.clear();
		//	ReceiverItems.clear();
		//	subtract.clear();
			return decodable;
		}
		
		/**
		 * calculate the time
		 * @return
		 */
		public double[] decodeSetDiffWithTime(RDATable receiver){
			/**
			 * subtract, a new TBFTable
			 */
			HashSet<Long> SenderItems = new HashSet<Long>(2);
			HashSet<Long> ReceiverItems = new HashSet<Long>(2);
			
			long time1=System.nanoTime();
			
			RDATable subtract=sender.subtractIBLT(receiver);
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
		public void repair(RDATable receiver,Hashtable<Long,Double> ReceiverTSTable){
			/**
			 * subtract, a new TBFTable
			 */
			RDATable subtract=sender.subtractIBLT(receiver);
			HashSet<Long> SenderItems = new HashSet<Long>(2);
			HashSet<Long> ReceiverItems = new HashSet<Long>(2);
			//decode the ids
			boolean decodable=subtract.decodeIDs(SenderItems,ReceiverItems);
			System.out.println("decode: "+decodable+",s: "+SenderItems.size()+",r: "+ReceiverItems.size());

			
			//repair the sender
			if(!SenderItems.isEmpty()){
				sender.RepairTBF(SenderItems,SenderTSTable);
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
		public void repairDirect(HashSet<Long> SenderItems,RDATable receiver,HashSet<Long> ReceiverItems,Hashtable<Long,Double> ReceiverMiss){
			/**
			 * subtract, a new TBFTable
			 */
			//RDATable subtract=sender.subtractIBLT(receiver);
			//HashSet<Long> SenderItems = new HashSet<Long>(2);
			//HashSet<Long> ReceiverItems = new HashSet<Long>(2);
			//decode the ids
			//boolean decodable=subtract.decodeIDs(SenderItems,ReceiverItems);
			//System.out.println("decode: "+decodable+",s: "+SenderItems.size()+",r: "+ReceiverItems.size());
			
			//repair the sender
			if(!SenderItems.isEmpty()){
				sender.RepairTBF(SenderItems,SenderTSTable);
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
		
		
		public double[] repairNow(double percent,RDATable receiver,Hashtable<Long,Double> ReceiverTSTable){
			/**
			 * subtract, a new TBFTable
			 */
			RDATable subtract=sender.subtractIBLT(receiver);
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
				if(sender.hashTable[i].keyCheck==receiver.hashTable[i].keyCheck&&
						sender.hashTable[i].Counter>0&&
						sender.hashTable[i].keySum==receiver.hashTable[i].keySum){
					good+=sender.hashTable[i].Counter;
				}
			}
			
			
			double[] result={good/sender.N_HASH,delay};

			return result;
		}
		
		public double repair(double percent,RDATable receiver,Hashtable<Long,Double> ReceiverTSTable){
			/**
			 * subtract, a new TBFTable
			 */
			RDATable subtract=sender.subtractIBLT(receiver);
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
				if(sender.hashTable[i].keyCheck==receiver.hashTable[i].keyCheck&&
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
		public double getAverage(RDATable receiver){	
			return sender.getAvgTS(receiver);
		}
		
		
		/**
		 * StandardDeviation
		 * @param avg, average number
		 * @return
		 */
		public double getStandardDeviation(double avg,RDATable receiver,Hashtable<Long,Double> ReceiverTSTable){			
			return sender.getSTD(receiver, avg);
			
			//return receiver.getVarianceStatistics(sender, avg);
		}

		/**
		 * get standard deviation
		 * @param avg
		 * @param receiver
		 * @return
		 */
		public double getStandardDeviation(double avg,RDATable receiver){			
			return sender.getSTD(receiver, avg);
			
			//return receiver.getVarianceStatistics(sender, avg);
		}
		public String getBucketSize(RDATable receiver) {
			// TODO Auto-generated method stub
			return sender.expectedNumEntries+" "+receiver.expectedNumEntries+" ";
		}

		public void clear() {
			// TODO Auto-generated method stub
			SenderTSTable.clear();
			//ReceiverTSTable.clear();
			//SenderTSTable=null;
			//ReceiverTSTable=null;
			sender.clear();
			//receiver.clear();
		}

		public double getGoodPackets(RDATable receiver) {
			// TODO Auto-generated method stub
			return sender.getGoodPackets(receiver);
		}
		
}
