package util.bloom.Exist;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;

public class FineComb {

		/**
		 * storage
		 */
	FineCombTable[] bank;
	double[] sampleProbability;	
	
	Hashtable<Long,Double> reorderedPackets;
		
	public final static SimpleLongHash sHash64 = new SimpleLongHash();
		/**
		 * parameter
		 */
		int columns=0;
		
		int rows=0;
		
		public FineComb(int column,int row){
			bank = new FineCombTable[column];
			for(int i=0;i<column;i++){
				bank[i] = new FineCombTable(row);
			}
			columns=column;
			rows=row;
			
			//reordered packets
			reorderedPackets = new Hashtable<Long,Double>();
		}
		
		public FineComb(int column,int row,double[] _sampleProbability){
			
			
			//sort in ascending order
			Arrays.sort(_sampleProbability);
			
			columns=column;
			rows=row;
			sampleProbability=Arrays.copyOf(_sampleProbability, _sampleProbability.length);
			bank = new FineCombTable[column];
						
			for(int i=0;i<column;i++){
				bank[i] = new FineCombTable(row,_sampleProbability[columns-i-1]);
			}
		}
		/**
		 * insert a packet, 
		 * @param k, key to be hashed, i.e., packet id
		 * @param ts, timestamp
		 */
		public void insertNoSample(long k,double ts){
			for(int i=0;i<columns;i++){
				bank[i].insertNoSample(k, ts);
			}
		}
		
		/**
		 * sample the packet with probability, (1/2)^m, then insert it
		 * into the bank
		 * @param k
		 * @param ts
		 */
		public boolean Insertsample(long k,double ts){
			
			boolean inserted=true;
			for(int i=0;i<columns;i++){
				boolean insertedF=bank[i].Insertsample(k, ts);
				if(!insertedF){
					inserted=false;
					break;
				}
			}
			return inserted;
		}
		
		public void deleteSample(long k,double ts){
			for(int i=0;i<columns;i++){
				bank[i].Deletesample(k, ts);
			}
		}
		
		/**
		 * we compute the buckets poisoned by only the reordered buckets
		 * @param reorderedPackets
		 * @param sender
		 * @param receiver
		 * @return
		 */
		public void repairReorderCheat(Hashtable<Long,Double> lostPackets,
				Hashtable<Long,Double> reorderedPackets, 
				FineComb sender,FineComb receiver){
			FineComb subtract = sender.subtract(receiver);
			
			int[][] badLostIndexes = badIndexesDuetoLost(lostPackets,sender);
			for(int i=0;i<rows;i++){
				//find good column			
				for(int j=0;j<columns;j++){
					//bad bucket
					if(subtract.bank[j].bank[i].keySum!=0){
						if(badLostIndexes[j][i]==0){
							repair(reorderedPackets,receiver,i,j);
						}
					}
				}
			}
			badLostIndexes=null;
		}
		
		/**
		 * repair using only the lost packets	
		 * @param reorderedPackets2
		 * @param receiver
		 */
		private void repair(Hashtable<Long, Double> reorderedPackets2,
				FineComb receiver,int row,int column) {
			// TODO Auto-generated method stub
			
			FineCombPair element = receiver.bank[column].bank[row];
			Hashtable<Long, Double> table = element.keyValues;
			Iterator<Entry<Long, Double>> ier = table.entrySet().iterator();
			while(ier.hasNext()){
				Entry<Long, Double> tmp = ier.next();
				if(reorderedPackets2.containsKey(tmp.getKey())){
					//element.remove(tmp.getKey(), tmp.getValue());
					//ier.remove();
					element.keySum ^=FineComb.sHash64.hash(ByteBuffer.allocate(8).putLong(tmp.getKey()
					).array());;
					element.Counter--;
					element.TS-=tmp.getValue();
					
					ier.remove();
				}
			}
			
			
			/*Iterator<Long> ier = reorderedPackets2.keySet().iterator();
			while(ier.hasNext()){
				Long key = ier.next();
				//if(receiver.bank[column].MapCorrect(row,key)){
					receiver.bank[column].Deletesample(key,reorderedPackets2.get(key));
				//}
			}*/
		}

		public int[][] badIndexesDuetoLost(
				Hashtable<Long, Double> lostPackets, FineComb sender){
			int[][] indexes=new int[sender.columns][sender.rows];
			for(int i=0;i<sender.columns;i++){
				for(int j=0;j<sender.rows;j++){
					indexes[i][j]=0;					
				}
			}
			Iterator<Long> ier = lostPackets.keySet().iterator();
			for(int i=0;i<sender.columns;i++){
			while(ier.hasNext()){
				Long key = ier.next();
				if(sender.bank[i].MapCorrect(1,key)){					
					indexes[i][sender.bank[i].getIndex(key)]=1;
				}
			}
			}
			return indexes;
		}
		
		/**
		 * map to the buckets
		 * @param lostPackets
		 * @param sender
		 * @param row
		 * @param column
		 * @return
		 */
		private boolean mappedByLostPackets(
				Hashtable<Long, Double> lostPackets, FineComb sender,int row,int column) {
			// TODO Auto-generated method stub
			Iterator<Long> ier = lostPackets.keySet().iterator();
			while(ier.hasNext()){
				Long key = ier.next();
				if(sender.bank[column].MapCorrect(row,key)){
					return true;
				}
			}
			return false;
		}

		
		
		

		/**
		 * repair the reordered packets
		 */
		public double repairReorderTrue(Hashtable<Long,Double> reorderedPackets, FineComb sender,FineComb receiver){
			long[] ids = new long[reorderedPackets.size()];
			Hashtable<Long,Long> MD5ToID=new Hashtable<Long,Long>(reorderedPackets.size());
			Iterator<Long> ier = reorderedPackets.keySet().iterator();
			for(int i=0;i<ids.length;i++){
				long value = ier.next(); 
				long key = Combination.getMD5HashBase64(value);						
				ids[i]=key;
				MD5ToID.put(key, value);
			}
			
			/**
			 * subtraction
			 */
	
			long key,value;
			Double timestamp;
			
			long timeStart=System.currentTimeMillis();
			
			FineComb subtract = sender.subtract(receiver);
			int rows=subtract.rows;
			int columns=subtract.columns;
			for(int i=0;i<rows;i++){
				//find good column			
				for(int j=0;j<columns;j++){
					//bad bucket
					if(subtract.bank[j].bank[i].keySum!=0){
						//repair
						//FineComb.sHash64.hash(ByteBuffer.allocate(8).putLong(id).array());
						for(int len=1;len<=ids.length;len++){
							
							long data[]=new long[len];
				        	if(Combination.printXORCombination(ids, ids.length,len, subtract.bank[j].bank[i].keySum,data)){
				        		System.out.println("final: "+Arrays.toString(data));
				        		for(int index=0;index<data.length;index++){
				        			key=data[index];
				        			value=MD5ToID.get(key);
				        			timestamp = reorderedPackets.get(value);
				        			//delete from the receiver's ts
				        			receiver.deleteSample(value,timestamp);
				        		}
				        	}
						}
						
					}
				}
				
			}
			
			long timeEnd=System.currentTimeMillis();
			
			return timeEnd-timeStart;
		}
		
		/**
		 * subtraction of two bank, other - this
		 * @param other
		 * @return
		 */
		public FineComb subtract(FineComb other){
			
			FineComb r=new FineComb(this.columns,this.rows,this.sampleProbability);
			for(int i=0;i<this.columns;i++){
				r.bank[i]=r.bank[i].subtract(other.bank[i]);
			}
			return r;
		}
		
		/**
		 * average metric
		 * @param other
		 * @return
		 */
		public double getAverage(FineComb other){
			//FineComb sub = subtract(other);
			long c=0;
			double sum=0;
			double TASum=0;
			double TBSum=0;
			
			for(int i=0;i<rows;i++){
				//find good column
				int usableColumn = -1;
				for(int j=0;j<columns;j++){
					if(this.bank[j].bank[i].Counter>0&&
							this.bank[j].bank[i].Counter==other.bank[j].bank[i].Counter&&
							this.bank[j].bank[i].keySum==other.bank[j].bank[i].keySum){
						usableColumn = j;
						TASum+=this.bank[j].bank[i].TS;
						TBSum+=other.bank[j].bank[i].TS;
						c+=this.bank[j].bank[i].Counter;
						
						break;
					}
				}//find the usable column
			}//end sum
			if(c==0){
				return -1;
			}else{
				return Math.abs(TBSum-TASum)/c;
			}
		}
		
		/**
		 * collapse adjacent bank, to compute the variance
		 * @return
		 */
		public FineComb collapseAdjacentBank(){
			int halvedSize=this.rows/2;
			FineComb r=new FineComb(this.columns,halvedSize,this.sampleProbability);
						
			for(int i=0;i<this.columns;i++){
				r.bank[i]=this.bank[i].collapseAdjacentBank();
				
			}
			return r;
		}
		
		/**
		 * standard deviation
		 * @param other11
		 * @return
		 */
		public double getStandardDeviation(FineComb other11,double avg){
			
			FineComb me = this.collapseAdjacentBank();
			FineComb you = other11.collapseAdjacentBank();
			
			
			
			int usableColumn=0;
			double TASum=0;
			double TBSum=0;
			double squaredDiff=0;
			int S=0;
			for(int i=0;i<me.rows;i++){
				usableColumn = -1;
				for(int j=0;j<me.columns;j++){
					//System.out.print("column: "+me.MB[j].bank[i].counter+", "+
				    //me.MB[j].bank[i].timestampSum);
				if(me.bank[j].bank[i].Counter>0&&
						me.bank[j].bank[i].keySum==you.bank[j].bank[i].keySum&&
						me.bank[j].bank[i].Counter==you.bank[j].bank[i].Counter){
						usableColumn = j;
						
						TASum=me.bank[usableColumn].bank[i].TS;
						TBSum=you.bank[usableColumn].bank[i].TS;
						squaredDiff+=Math.pow((TBSum - TASum),2);			
						S+=me.bank[usableColumn].bank[i].Counter;	
						break;
					}
				}//find the usable column
			}//end sum
			
			
			return Math.abs( squaredDiff/S- Math.pow(avg,2));
			/*{if(S>0){
				System.out.println("\n*************std: "+(squaredDiff/S)+"\n*************\n");
				return squaredDiff/S;
			}else{
				return -1;
			}
			*/
			
			}

		/**
		 * good packets
		 * @param sender
		 * @return
		 */
		public double getGoodPackets(FineComb receiver) {
			// TODO Auto-generated method stub
			double good=0;
			//double[] Count=new double[columns];			
			for(int i=0;i<this.columns;i++){
				good += this.bank[i].getGoodPackets(receiver.bank[i]);
			}
			
			return good/this.columns;
		}
		
	
}
