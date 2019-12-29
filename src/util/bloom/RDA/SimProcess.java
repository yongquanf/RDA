package util.bloom.RDA;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;

public class SimProcess {

	 RDATable sender;
	 RDATable receiver;
	 
	 Hashtable<Long,Double> SenderTSTable;
	 Hashtable<Long,Double> ReceiverTSTable;
	
	 long maximumTotalPackets=0;
	 
		/**
		 * 
		 * @param _row, # rows, 
		 * @param _column
		 * @param sampleProbByColumns
		 */
		public SimProcess(int _expectedNumEntries){
			
			sender = new RDATable(_expectedNumEntries);
			receiver = new RDATable(_expectedNumEntries);

			SenderTSTable = new Hashtable<Long,Double>();
			ReceiverTSTable = new Hashtable<Long,Double>();
		}
		
		public SimProcess(int _expectedNumEntries,long totalPackets,int hashNum){
			
			sender = new RDATable(_expectedNumEntries);
			receiver = new RDATable(_expectedNumEntries);
			sender.setHash(hashNum);
			receiver.setHash(hashNum);
			

			SenderTSTable = new Hashtable<Long,Double>();
			ReceiverTSTable = new Hashtable<Long,Double>();
			maximumTotalPackets=totalPackets;
			sender.sampleProbability=1;
			receiver.sampleProbability=1;
		}
		
		public SimProcess(int _expectedNumEntries,long totalPackets,double samplingProb){
			
			sender = new RDATable(_expectedNumEntries);
			receiver = new RDATable(_expectedNumEntries);

			SenderTSTable = new Hashtable<Long,Double>();
			ReceiverTSTable = new Hashtable<Long,Double>();
			maximumTotalPackets=totalPackets;
			sender.sampleProbability=samplingProb;
			receiver.sampleProbability=samplingProb;
		}
		
		public SimProcess(int _expectedNumEntries,double samplingProb){
			
			sender = new RDATable(_expectedNumEntries);
			receiver = new RDATable(_expectedNumEntries);

			SenderTSTable = new Hashtable<Long,Double>();
			ReceiverTSTable = new Hashtable<Long,Double>();
			//maximumTotalPackets=totalPackets;
			sender.sampleProbability=samplingProb;
			receiver.sampleProbability=samplingProb;
		}
		
		/**
		 * new packet arrives at the sender
		 * @param id
		 * @param ts
		 */
		public void packetIncomingSender(long id, double ts){
			if(SenderTSTable.containsKey(id)){
				System.err.println("contains key: "+id+", "+ts);
				return;
			}else{
				sender.insert(id, ts,1);
				SenderTSTable.put(id, ts);
			}
		}
		
		public void packetIncomingSenderSample(long id, double ts){
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
		public boolean packetIncomingSenderIgnoreDuplicate(long id, 
				double ts){
			if(SenderTSTable.containsKey(id)){
				return false;
			}else{
				sender.insert(id, ts,1);
				SenderTSTable.put(id, ts);
				return true;
			}
		}
		
		/**
		 * new packet arrives at the receiver
		 * @param id
		 * @param ts
		 */
		public void packetIncomingReceiver(long id,double ts){
			if(ReceiverTSTable.containsKey(id)){
				System.err.println("contains key: "+id+", "+ts);
				return;
			}else{
				receiver.insert(id, ts,1);
				ReceiverTSTable.put(id, ts);
			}
		}
		
		public void packetIncomingReceiverSample(long id, double ts){
			if(ReceiverTSTable.containsKey(id)){
				System.err.println("contains key: "+id+", "+ts);
				return;
			}else{
				boolean inserted = receiver.InsertSample(id, ts,1);
				if(inserted){ReceiverTSTable.put(id, ts);}
			}
	
		}
		
		public boolean packetIncomingReceiverIgnoreDuplicate(long id, 
				double ts){
			if(ReceiverTSTable.containsKey(id)){
				return false;
			}else{
				receiver.insert(id, ts,1);
				ReceiverTSTable.put(id, ts);
				return true;
			}
		}
		
		public int decodeSetDiff(){
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
		
		/**
		 * calculate the time
		 * @return
		 */
		public double[] decodeSetDiffWithTime(){
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
		public void repair(){
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
		 * vary percent of repaired
		 * @param percent
		 */
		
		
		public double[] repairNow(double percent){
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

			//double decodedPackets=SenderItems.size()+ReceiverItems.size();
			

			//Set<Long> us = new HashSet<Long>();
			//us.addAll(SenderTSTable.keySet());
			//us.addAll(ReceiverTSTable.keySet());
			//us.removeAll(ReceiverItems);
			//us.removeAll(SenderItems);
			
			
			
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
				
				
				//sender.RepairTBF(SenderItems,SenderTSTable);
				//repair(sender,SenderItems,SenderTSTable);
			//}
			//repair the receiver
			//if(!ReceiverItems.isEmpty()){
			//	receiver.RepairTBF(ReceiverItems,ReceiverTSTable);
				//repair(receiver,ReceiverItems,ReceiverTSTable);
			//}
			
			//iterate each good packets;
			/*Iterator<Long> ier0 = us.iterator();
			//good packets
			double good=0;
			while(ier0.hasNext()){
				Long id = ier0.next();
				byte[] kVec=HashTableEntry.ToByteArray(id);
				int bucketsPerHash = sender.hashTable.size()/sender.N_HASH;
				for(int i=0;i<sender.N_HASH;i++){
					int startEntry = i*bucketsPerHash;
					int[] h=HashTableEntry.hashC.hash(new Key(kVec),i);
					int index=startEntry+(h[0]%bucketsPerHash);
					//get two entries
					TBFTableEntry entrySender = sender.hashTable.get(index);
					TBFTableEntry entryReceiver = receiver.hashTable.get(index);
					if(entrySender.keyCheck==entryReceiver.keyCheck&&
							entrySender.keySum==entryReceiver.keySum){
						good++;
						break;
					}
						
					}//end each packet
				
			}//end all packets
			*/
			//us.clear();
			//us=null;
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
		
		public double repair(double percent){
			/**
			 * subtract, a new TBFTable
			 */
			RDATable subtract=sender.subtractIBLT(receiver);
			HashSet<Long> SenderItems = new HashSet<Long>(2);
			HashSet<Long> ReceiverItems = new HashSet<Long>(2);
			//decode the ids
			
			boolean decodable=subtract.decodeIDs(SenderItems,ReceiverItems);
			System.out.println("decode: "+decodable+",s: "+SenderItems.size()+",r: "+ReceiverItems.size());

			//double decodedPackets=SenderItems.size()+ReceiverItems.size();
			

			//Set<Long> us = new HashSet<Long>();
			//us.addAll(SenderTSTable.keySet());
			//us.addAll(ReceiverTSTable.keySet());
			//us.removeAll(ReceiverItems);
			//us.removeAll(SenderItems);
			
			
			
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
				
				
				//sender.RepairTBF(SenderItems,SenderTSTable);
				//repair(sender,SenderItems,SenderTSTable);
			//}
			//repair the receiver
			//if(!ReceiverItems.isEmpty()){
			//	receiver.RepairTBF(ReceiverItems,ReceiverTSTable);
				//repair(receiver,ReceiverItems,ReceiverTSTable);
			//}
			
			//iterate each good packets;
			/*Iterator<Long> ier0 = us.iterator();
			//good packets
			double good=0;
			while(ier0.hasNext()){
				Long id = ier0.next();
				byte[] kVec=HashTableEntry.ToByteArray(id);
				int bucketsPerHash = sender.hashTable.size()/sender.N_HASH;
				for(int i=0;i<sender.N_HASH;i++){
					int startEntry = i*bucketsPerHash;
					int[] h=HashTableEntry.hashC.hash(new Key(kVec),i);
					int index=startEntry+(h[0]%bucketsPerHash);
					//get two entries
					TBFTableEntry entrySender = sender.hashTable.get(index);
					TBFTableEntry entryReceiver = receiver.hashTable.get(index);
					if(entrySender.keyCheck==entryReceiver.keyCheck&&
							entrySender.keySum==entryReceiver.keySum){
						good++;
						break;
					}
						
					}//end each packet
				
			}//end all packets
			*/
			//us.clear();
			//us=null;
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
		public double getAverage(){	
			return sender.getAvgTS(receiver);
		}
		
		
		/**
		 * StandardDeviation
		 * @param avg, average number
		 * @return
		 */
		public double getStandardDeviation(double avg){			
			return sender.getSTD(receiver, avg);
			
			//return receiver.getVarianceStatistics(sender, avg);
		}

		public String getBucketSize() {
			// TODO Auto-generated method stub
			return sender.expectedNumEntries+" "+receiver.expectedNumEntries+" ";
		}

		public void clear() {
			// TODO Auto-generated method stub
			SenderTSTable.clear();
			ReceiverTSTable.clear();
			//SenderTSTable=null;
			//ReceiverTSTable=null;
			sender.clear();
			receiver.clear();
		}

		public double getGoodPackets() {
			// TODO Auto-generated method stub
			return sender.getGoodPackets(receiver);
		}
		
}
