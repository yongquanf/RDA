package util.bloom.Exist;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;

import util.bloom.Apache.Key;

public class FineCombTable {

	/**
	 * seed
	 */
	static int N_index = 123422;
	/**
	 * storage
	 */
	FineCombPair[] bank;
	
	
	
	/**
	 * parameter
	 */
	int BankSize=0;
	/**
	 * for sampling packets
	 */
	int requiredLeadingZeros=0;
	
	double samplingProbability=0;
	
	public FineCombTable(int length){
		bank = new FineCombPair[length];
		BankSize=length;
		for(int i=0;i<BankSize;i++){
			bank[i]=new FineCombPair();
		}
		//sampleProbability=1;
		requiredLeadingZeros=0;
		samplingProbability=1;
	}
	
	public FineCombTable(int length,double _sampleProbability){
		bank = new FineCombPair[length];
		BankSize=length;
		for(int i=0;i<BankSize;i++){
			bank[i]=new FineCombPair();
		}
		samplingProbability=_sampleProbability;
		requiredLeadingZeros=(int)Math.round(Math.log(_sampleProbability)/Math.log(0.5))%64;
		
	}
	/**
	 * compute the cell index using the hash function
	 * @param k
	 * @return
	 */
	public int[] hash2Cell(long k){
		
		//index, number of leading zeros
		int []r=new int[2];
		
		byte[] kVec=IBLTEntry.ToByteArray(k);
		long tmp = FineComb.sHash64.hash(ByteBuffer.allocate(8).putLong(k).array())%64;
		
		long keyHash=IBLTEntry.LongHashFunction4PKeyHash.hashLong(k);
		
		
		//int[] h=IBLTEntry.hashC.hash(new Key(kVec),N_index);
		r[0] = (int)keyHash%BankSize;
		r[1] = Long.numberOfLeadingZeros(tmp);
		return r;
		
		//index, number of leading zeros
		/*int []r=new int[2];
		
		byte[] kVec=HashTableEntry.ToByteArray(k);
		int[] h=HashTableEntry.hashC.hash(new Key(kVec),N_index);
		r[0] = h[0]%BankSize;
		r[1] = Integer.numberOfLeadingZeros(h[0]);
		return r;*/
	}
	/**
	 * insert a packet, 
	 * @param k, key to be hashed, i.e., packet id
	 * @param ts, timestamp
	 */
	public void insertNoSample(long k,double ts){
		int[] index=hash2Cell(k);
		bank[index[0]].insert(k,ts);
	}
	
	/**
	 * sample the packet with probability, (1/2)^m, then insert it
	 * into the bank
	 * @param k
	 * @param ts
	 */
	public boolean Insertsample(long k,double ts){
		int[] index=hash2Cell(k);
		if(index[1]>=requiredLeadingZeros){
			bank[index[0]].insert(k,ts);
			//System.out.println("inserted!");
			return true;
		}else{
			return false;
		}
	}
	
	public boolean Deletesample(long k,double ts){
		int[] index=hash2Cell(k);
		if(index[1]>=requiredLeadingZeros){
			bank[index[0]].remove(k,ts);
			//System.out.println("inserted!");
			return true;
		}else{
			return false;
		}
	}
	
	
	/**
	 * subtraction of two bank, other - this
	 * @param other
	 * @return
	 */
	public FineCombTable subtract(FineCombTable other){
		assert(BankSize==other.bank.length);
		FineCombTable r=new FineCombTable(BankSize,this.samplingProbability);
		for(int i=0;i<BankSize;i++){
			r.bank[i].Counter = other.bank[i].Counter-this.bank[i].Counter;
			r.bank[i].TS = other.bank[i].TS-this.bank[i].TS;
			r.bank[i].keySum=this.bank[i].keySum^other.bank[i].keySum;
		}
		return r;
	}
	
	/**
	 * average metric
	 * @param other
	 * @return
	 */
	public double getAverage(FineCombTable other){
		FineCombTable sub = subtract(other);
		long c=0;
		double sum=0;
		for(int i=0;i<BankSize;i++){
			if(other.bank[i].Counter!=0&&sub.bank[i].keySum==0&&sub.bank[i].Counter==0){
				c+=other.bank[i].Counter;
				sum+=Math.abs(sub.bank[i].TS);
			}
		}
		//System.out.println("$BLDA: sum: "+sum+", c: "+c);
		if(c==0){
			return -1;
		}else{
			return sum/c;
		}
	}
	
	/**
	 * collapse adjacent bank, to compute the variance
	 * @return
	 */
	public FineCombTable collapseAdjacentBank(){
		int halvedSize=BankSize/2;
		FineCombTable r=new FineCombTable(halvedSize);
		for(int i=0;i<halvedSize;i++){
			r.bank[i].Counter=this.bank[i*2].Counter+this.bank[i*2+1].Counter;
			r.bank[i].TS=this.bank[i*2].TS-this.bank[i*2+1].TS;
			r.bank[i].keySum=this.bank[i*2].keySum^this.bank[i*2+1].keySum;
		}
		return r;
	}
	
	/**
	 * standard deviation
	 * @param other11
	 * @return
	 */
	public double getStandardDeviation(FineCombTable other11,double avg){
		return  Math.abs(getFScoreByCollapse(other11) - Math.pow(avg,2));
	}
	public double getFScoreByCollapse(FineCombTable other11){
		FineCombTable me = this.collapseAdjacentBank();
		FineCombTable you = other11.collapseAdjacentBank();
		
		int usableColumn=0;
		double TASum=0;
		double TBSum=0;
		double squaredDiff=0;
		int S=0;
		for(int i=0;i<me.BankSize;i++){
			//only choose good pair
			
				if(me.bank[i].Counter>0&&
				me.bank[i].keySum==you.bank[i].keySum&&
				me.bank[i].Counter==you.bank[i].Counter){
					usableColumn=i;
					TASum=me.bank[usableColumn].TS;
					TBSum=you.bank[usableColumn].TS;
					
					//System.out.println("$ "+TASum+", "+TBSum);
					
					squaredDiff+=Math.pow((TBSum - TASum),2);					
					S+=me.bank[usableColumn].Counter;	
				
			}//find the usable column
		}
		//System.out.println("$BLDA: squaredDiff: "+squaredDiff+", S: "+S);
		if(S>0){
			return squaredDiff/S;
		}else{
			return -1;
		}
	}

	public double getGoodPackets(FineCombTable sender) {
		// TODO Auto-generated method stub
		double good=0;
		FineCombTable sub = subtract(sender);
		long c=0;
		double sum=0;
		for(int i=0;i<BankSize;i++){
			if(sender.bank[i].Counter!=0
					&&sub.bank[i].keySum==0&&sub.bank[i].Counter==0){
				good+=sender.bank[i].Counter;
				
			}
		}
		return good;
	}
	/**
	 * map to the row
	 * @param row
	 * @param key
	 * @return
	 */
	public boolean MapCorrect(int row, Long key) {
		// TODO Auto-generated method stub
		int[] index=hash2Cell(key);
		if(index[1]>=requiredLeadingZeros){			
			//System.out.println("inserted!");
			return true;
		}else{
			return false;
		}
	}

	public int getIndex(Long key) {
		// TODO Auto-generated method stub
		int[] index=hash2Cell(key);
		return index[0];
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
			FineCombTable sender,FineCombTable receiver){
		FineCombTable subtract = sender.subtract(receiver);
		
		int[]badLostIndexes = badIndexesDuetoLost(lostPackets,sender);
		for(int i=0;i<bank.length;i++){
				//bad bucket
				if(subtract.bank[i].keySum!=0){
					if(badLostIndexes[i]==0){
						repair(reorderedPackets,receiver,i);
					}
				}

		}
		badLostIndexes=null;
	}
	
	/**
	 * repair the reordered packets
	 */
	public double repairReorderTrue(Hashtable<Long,Double> reorderedPackets, FineCombTable sender,
			FineCombTable receiver){
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
		
		FineCombTable subtract = sender.subtract(receiver);
		int rows=subtract.BankSize;
		
		for(int i=0;i<rows;i++){
			//find good column			
				//bad bucket
				if(subtract.bank[i].keySum!=0){
					//repair
					//FineComb.sHash64.hash(ByteBuffer.allocate(8).putLong(id).array());
					for(int len=1;len<=ids.length;len++){						
						long data[]=new long[len];
			        	if(Combination.printXORCombination(ids, ids.length,len, subtract.bank[i].keySum,data)){
			        		//System.out.println("final: "+Arrays.toString(data));
			        		for(int index=0;index<data.length;index++){
			        			key=data[index];
			        			value=MD5ToID.get(key);
			        			timestamp = reorderedPackets.get(value);
			        			//delete from the receiver's ts
			        			receiver.Deletesample(value,timestamp);
			        		}
			        	}
					}
					
				}

			
		}
		
		MD5ToID.clear();
		long timeEnd=System.currentTimeMillis();
		
		return timeEnd-timeStart;
	}
	
	/**
	 * repair using only the lost packets	
	 * @param reorderedPackets2
	 * @param receiver
	 */
	private void repair(Hashtable<Long, Double> reorderedPackets2,
			FineCombTable receiver,int row) {
		// TODO Auto-generated method stub
		
		FineCombPair element = receiver.bank[row];
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
	}

	
	public int[] badIndexesDuetoLost(Hashtable<Long, Double> lostPackets, FineCombTable sender){
		int[] indexes=new int[sender.bank.length];

			for(int j=0;j<indexes.length;j++){
				indexes[j]=0;					
			}
		Iterator<Long> ier = lostPackets.keySet().iterator();
		while(ier.hasNext()){
			Long key = ier.next();
			if(sender.MapCorrect(1,key)){					
				indexes[sender.getIndex(key)]=1;
			}
		}

		return indexes;
	}
}
