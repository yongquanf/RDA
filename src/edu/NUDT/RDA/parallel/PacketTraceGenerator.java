package edu.NUDT.RDA.parallel;

import util.async.Weibull;
import edu.harvard.syrah.prp.Log;
import eduni.simjava.distributions.*;

/**
 * generate packet trace
 * @author ericfu
 *
 */
public class PacketTraceGenerator {

	long seed=21213233;
	double NextTime=0;
	long NextID=0;
	long weibullMultiplier=10;
	Weibull weibuller=null;
	Sim_uniform_obj uniformDrop=null;
	Sim_uniform_obj uniformReorder=null;
	//loss
	public double dropProbability=0;//0-1
	public double reorderProbability=0;//0-1
	/**
	 * input
	 * @param scale, alpha
	 * @param shape, beta [0.6,0.8]
	 * @param initialTS
	 * @param _startID
	 */
	public PacketTraceGenerator(double scale, double shape,double _dropProbability,double _reorderProbability,long initialTS,long _startID){
		seed=System.currentTimeMillis();
		weibuller = new Weibull("Delay",scale,shape,System.currentTimeMillis());
		uniformDrop = new Sim_uniform_obj("drop",0,1,System.currentTimeMillis());
		uniformReorder = new Sim_uniform_obj("reorder",0,1,System.currentTimeMillis());
		dropProbability = _dropProbability;
		reorderProbability = _reorderProbability;
		NextTime =initialTS;
		NextID = _startID;
	}
	
	/**
	 * generate the packet trace by the weibull distribution
	 * @return, id, sendTimeStamp,receiveTimeStamp
	 */
	public double[] NextWeibullPacket(){
		if(weibuller==null){System.err.println("Weibull empty");return null;}
		double delay=weibuller.sample();
		//System.out.println("weibull: "+delay);
		double [] value=new double[3];
		//id
		value[0]=NextID++;
		//send time
		value[1]=NextTime;
		double networkDelay=Math.round(delay*weibullMultiplier);
		//delivery time
		value[2]=NextTime+networkDelay;
		NextTime+=NextInterPacketArrivalDelay(networkDelay);
		return value;
	}
	
	public double[] NextWeibullPacket(double timeT){
		if(weibuller==null){System.err.println("Weibull empty");return null;}
		double delay=weibuller.sample();
		//System.out.println("weibull: "+delay);
		double [] value=new double[3];
		//id
		value[0]=NextID++%Long.MAX_VALUE;
		//send time
		value[1]=Math.abs(timeT);
		double networkDelay=Math.abs(Math.round(delay*weibullMultiplier));
//		while(networkDelay<=1){
//			networkDelay=Math.round(weibuller.sample()*weibullMultiplier);
//		}
		//delivery time
		value[2]=value[1]+networkDelay;
		NextTime=value[1];
		return value;
	}
	
	public double NextWeibullPacketDelay(){
		if(weibuller==null){System.err.println("Weibull empty");return -1;}
		double delay=weibuller.sample();
		double vv=(delay*weibullMultiplier);
		//System.out.println("weibull: "+vv);
		return vv;
	}
	
	/**
	 * 2, good, 0, drop, 1 reorder
	 * @return
	 */
	public int dropOrReorder(){
		
		double p = Math.abs(uniformDrop.sample());
		//System.out.println("p: "+p+"drop: "+dropProbability+", reorder: "+reorderProbability);
		if(p<=dropProbability+reorderProbability){
			if(uniformReorder.sample()<=dropProbability/(dropProbability+reorderProbability)){
				return 0;
			}else{
				return 1;
			}
		}else{
			return 2;
		}
	}
	
	/**
	 * inter-Arrival Packet Delay
	 * @return
	 */
	public double NextInterPacketArrivalDelay(double networkDelay){
		return (networkDelay*1.5);
	}
	
	/**
	 * drop packet, uniform distribution
	 * @return
	 */
	public boolean dropPacketOld(){
		if(uniformDrop.sample()<=dropProbability){
			//drop
			return true;
		}else{
			return false;
		}
	}
	
	/**
	 * the packet is received by the receiver, but not the sender, at the beginning, ending
	 * @return
	 */
	public boolean reorderPacketOld(){
		if(uniformReorder.sample()<=reorderProbability){
			//reorder
			return true;
		}else{
			return false;
		}
	}
}
