package util.bloom.Exist;

import java.util.Hashtable;

public class BasicFineComb {
	
	FineCombTable sender;
	FineCombTable receiver;
	double sampleProbability;//0-1
	
	public Hashtable<Long,Double> reorderedPackets;
	public Hashtable<Long,Double> LostPackets;
	
	public BasicFineComb(int length,double _sampleProbability){
		sender = new FineCombTable(length,_sampleProbability);
		receiver = new FineCombTable(length,_sampleProbability);

		reorderedPackets = new Hashtable<Long,Double>();
		LostPackets = new Hashtable<Long,Double>();
	}
	
	/**
	 * add reordered packets
	 * @param id
	 * @param ts
	 */
	public void addReordered(long id, double ts){
		this.reorderedPackets.put(id, ts);
	}
	
	public void addLost(long id, double ts){
		this.LostPackets.put(id, ts);
	}
	
	/**
	 * new packet arrives at the sender
	 * @param id
	 * @param ts
	 */
	public boolean packetIncomingSender(long id,double ts){
		return sender.Insertsample(id, ts);
	}
	
	/**
	 * new packet arrives at the receiver
	 * @param id
	 * @param ts
	 */
	public boolean packetIncomingReceiver(long id,double ts){
		return receiver.Insertsample(id, ts);
	}
	
	
	/**
	 * average number
	 * @return
	 */
	public double getAverage(){
		return receiver.getAverage(sender);
	}
	/**
	 * StandardDeviation
	 * @param avg, average number
	 * @return
	 */
	public double getStandardDeviation(double avg){
		return receiver.getStandardDeviation(sender, avg);
	}

	public double getGoodPackets() {
		// TODO Auto-generated method stub
		return receiver.getGoodPackets(sender);
	}

	public void packetIncomingSenderNoSample(long id, double ts) {
		// TODO Auto-generated method stub
		 sender.insertNoSample(id, ts);
	}

	/**
	 * repair the reordered packets
	 * @return
	 */
	public void repairReceiverReordered(){
		receiver.repairReorderCheat(LostPackets,reorderedPackets,sender, receiver);
	}

	/**
	 * repair the reordered packets
	 * @return
	 */
	public double repairReceiverReorderedTrue(){
		return receiver.repairReorderTrue(reorderedPackets,sender, receiver);
	}
	
	public void packetIncomingReceiverNoSample(long id, double ts) {
		// TODO Auto-generated method stub
		receiver.insertNoSample(id, ts);
	}
}
