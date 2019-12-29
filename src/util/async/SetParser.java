package util.async;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import util.async.Util;
import util.bloom.Apache.Key;

import edu.harvard.syrah.prp.Log;

public class SetParser {
	//log
	static Log log =new Log(SetParser.class);
	
	
	//current nodes, p2p
	List<Integer> P2PNodes=null;
	
	//social
	List<Integer> nodes =null;
	/**
	 * store the list of neighbors of each user
	 */
	public static Hashtable<Integer,Set<Integer>> NeighborTable=null;
	
	//=========================================
	
	//user, files
	public static Hashtable<Integer,Set<Integer>> userFiles=null;
	
	/**
	 * max # neighbors
	 */
	int maxNeighbor=-1;
	
	static SetParser one=null;
	
	private SetParser(){
		
	NeighborTable=new Hashtable<Integer,Set<Integer>>(2);
		
		//=========================================
		
		//user, files
	userFiles=new Hashtable<Integer,Set<Integer>>(2);
	
	}
	
	/**
	 * get the instance
	 * @return
	 */
	public static SetParser getInstance(){
		if(one==null){
			one=new SetParser();
		}
		return one;
	}
	
	
	/**
	 * choose two users
	 * @param choice, 1 social network, 2 p2p
	 * @return
	 */
	public List<Key>[] getTwoRandomSet(int choice){
		if(choice==1){
			return getTwoRandomUsersSocialGraph();
		}
		if(choice==2){
			return getTwoRandomUsersP2P();
		}
		return null;
	}
	
	/**
	 * clear it
	 */
	public void clear(){
		if(P2PNodes!=null){
			P2PNodes.clear();
			P2PNodes=null;
			
		}
		
		if(nodes!=null){
			nodes.clear();
			nodes=null;
		}
		
		if(NeighborTable!=null){
			Iterator<Set<Integer>> ier = NeighborTable.values().iterator();
			while(ier.hasNext()){
				ier.next().clear();
			}
			NeighborTable.clear();
			NeighborTable=null;			
		}
		
		if(userFiles!=null){
			Iterator<Set<Integer>> ier = userFiles.values().iterator();
			while(ier.hasNext()){
				ier.next().clear();
			}
			userFiles.clear();
			userFiles=null;	
		}
		
		
		
	}
	
	/**
	 * read the social graph
	 */
	public List<Key>[] getTwoRandomUsersSocialGraph(){
		
	//log.info("total nodes: "+nodes.size());	
	//randomly choose two users,
	Integer nodeA=(Integer)Util.getRandomItem(nodes);
	Integer nodeB=(Integer)Util.getRandomItem(nodes);
	while(nodeA==nodeB){
		//log.error("same node");
		nodeA=(Integer)Util.getRandomItem(nodes);
		nodeB=(Integer)Util.getRandomItem(nodes);
	}
	//non-existence
	if(!NeighborTable.containsKey(nodeA)||!NeighborTable.containsKey(nodeB)){
		log.warn("nonexistent nodes");
		return null;
	}
	//calculate the intersection of ground truth
	Set<Integer> nnsA = NeighborTable.get(nodeA);
	Set<Integer> nnsB = NeighborTable.get(nodeB);
	
	
	//no neighbors
	if(nnsA==null||nnsB==null||nnsA.isEmpty()||nnsB.isEmpty()){
		log.warn("no neighbors are found");
	}

//get the keys
List<Key> NodeA_Neighbor= Util.getKeysInteger(nnsA);
List<Key> NodeB_Neighbor= Util.getKeysInteger(nnsB);
//failed
if(NodeA_Neighbor==null||NodeB_Neighbor==null){
	log.warn("no key found for: "+nodeA+", "+nodeB);
}

List<Key>[]aa= new List[2];
aa[0]=new ArrayList(NodeA_Neighbor);
aa[1]=new ArrayList(NodeB_Neighbor);	


NodeA_Neighbor.clear();
NodeA_Neighbor=null;
NodeB_Neighbor.clear();
NodeB_Neighbor=null;


	return aa;
	/*
	
	//no neighbors
	if(nnsA==null||nnsB==null||nnsA.isEmpty()||nnsB.isEmpty()){
		log.warn("no neighbors are found");
		return null;
	}
	

	//get the keys

	List<Key> NodeA_Neighbor= Util.getKeysInteger(nnsA);
	List<Key> NodeB_Neighbor= Util.getKeysInteger(nnsB);
	//failed
	if(NodeA_Neighbor==null||NodeB_Neighbor==null){
		log.warn("no key found for: "+nodeA+", "+nodeB);
		return null;
	}
	
	List<Key>[]aa= new List[2];
	aa[0]=new ArrayList(NodeA_Neighbor);
	aa[1]=new ArrayList(NodeB_Neighbor);	
	
	
	NodeA_Neighbor.clear();
	NodeA_Neighbor=null;
	NodeB_Neighbor.clear();
	NodeB_Neighbor=null;
	
	
		return aa;*/
	
	}
	
	
	
	
	
	
	//parse the facebook data
	
	//parse undirectional adjacency list
	public void parseNeighborsSocialGraph(String fname){
		
		maxNeighbor=-1;
		try {
			BufferedReader bf=new BufferedReader(new FileReader(fname));
			
			String thisLine;
			Integer indexOfNodes=0;			
			while ((thisLine = bf.readLine()) != null) { // while loop begins here
			
				//parse the users
				String[] users= getTwoUsers(thisLine);
				//failed case
				if(users==null){
					log.warn("empty users");
					continue;
				}
				//integer
				int A=Integer.parseInt(users[0]);
				int B=Integer.parseInt(users[1]);
				//A
				if(!NeighborTable.containsKey(A)){
					Set<Integer> tmp=new HashSet<Integer>();
					tmp.add(B);
					NeighborTable.put(A, tmp);
				}else{
					Set<Integer> tmp=NeighborTable.get(A);
					tmp.add(B);
					NeighborTable.put(A, tmp);
				}
				//B
				if(!NeighborTable.containsKey(B)){
					Set<Integer> tmp=new HashSet<Integer>();
					tmp.add(A);
					NeighborTable.put(B, tmp);
				}else{
					Set<Integer> tmp=NeighborTable.get(B);
					tmp.add(A);
					NeighborTable.put(B, tmp);
				}				
			}
			
			
			bf.close();
			
			clearEmptyNodes();
			
			nodes= new ArrayList<Integer>(NeighborTable.keySet());
			
			Iterator<Set<Integer>> ier = NeighborTable.values().iterator();
			while(ier.hasNext()){
				Set<Integer> tt = ier.next();
				if(maxNeighbor<tt.size()){
					maxNeighbor=tt.size();
				}
				
			}
			
			
			
		}catch(Exception e){
			e.printStackTrace();			
		}
	
	}
	
	//=========================================
	/**
	 * parse the p2p file
	 * @param infile
	 */
	public void parseP2P(String infile){
		//parse the input
		parseNeighbors(infile);	
		//clear
		clearEmptyNodes();
		
		 P2PNodes= new ArrayList<Integer>(userFiles.keySet());
		 maxNeighbor=0;
			Iterator<Set<Integer>> ier = userFiles.values().iterator();
			while(ier.hasNext()){
				Set<Integer> tt = ier.next();
				if(maxNeighbor<tt.size()){
					maxNeighbor=tt.size();
				}
				
			}
		 
	}
	
	public void getStatisticsP2P(){
		log.info("users: "+P2PNodes.size());
		
		HashSet<Integer> files = new HashSet<Integer>();
		
		
		Iterator<Set<Integer>> ier = userFiles.values().iterator();
		while(ier.hasNext()){
			Set<Integer> ss = ier.next();
			files.addAll(ss);			
		}
		
		log.info("files: "+files.size());
		files.clear();
		files=null;
		
		
	}
	
	/**
	 * get two set of keys from the P2P application
	 * @return
	 */
	public List<Key>[] getTwoRandomUsersP2P(){
		
		if(P2PNodes==null){
			log.error("no P2P files trace");
			return null;
		}
		
		//randomly choose two users,
		Integer nodeA=(Integer)Util.getRandomItem(P2PNodes);
		Integer nodeB=(Integer)Util.getRandomItem(P2PNodes);
		while(nodeA==nodeB){
			//log.warn("same node: "+nodeA);
			nodeA=(Integer)Util.getRandomItem(P2PNodes);
			nodeB=(Integer)Util.getRandomItem(P2PNodes);
		}
		//non-existence
		if(!userFiles.containsKey(nodeA)||!userFiles.containsKey(nodeB)){
			log.warn("nonexistent nodes");
			return null;
		}
		//calculate the intersection of ground truth
		Set<Integer> nnsA = userFiles.get(nodeA);
		Set<Integer> nnsB = userFiles.get(nodeB);
		
		//no neighbors
		if(nnsA==null||nnsB==null||nnsA.isEmpty()||nnsB.isEmpty()){
			log.warn("no neighbors are found");
		}
	
	//get the keys
	List<Key> NodeA_Neighbor= Util.getKeysInteger(nnsA);
	List<Key> NodeB_Neighbor= Util.getKeysInteger(nnsB);
	//failed
	if(NodeA_Neighbor==null||NodeB_Neighbor==null){
		log.warn("no key found for: "+nodeA+", "+nodeB);
	}
	
	List<Key>[]aa= new List[2];
	aa[0]=new ArrayList(NodeA_Neighbor);
	aa[1]=new ArrayList(NodeB_Neighbor);	
	
	
	NodeA_Neighbor.clear();
	NodeA_Neighbor=null;
	NodeB_Neighbor.clear();
	NodeB_Neighbor=null;
	
	
		return aa;
	}
	
	
	//clear empty elements
	public void clearEmptyNodes(){
		int cleared=0;
		
		if(NeighborTable!=null&&!NeighborTable.isEmpty()){
		Iterator<Entry<Integer, Set<Integer>>> ier = NeighborTable.entrySet().iterator();
		Entry<Integer, Set<Integer>> tmp;
		while(ier.hasNext()){
			tmp=ier.next();
			if(tmp.getValue().isEmpty()){
				ier.remove();
				cleared++;
			}
		}
		}
		
		
		if(userFiles!=null&&!userFiles.isEmpty()){
			Iterator<Entry<Integer, Set<Integer>>> ier = userFiles.entrySet().iterator();
			Entry<Integer, Set<Integer>> tmp;
			while(ier.hasNext()){
				tmp=ier.next();
				if(tmp.getValue().isEmpty()){
					ier.remove();
					cleared++;
				}
			}
			
		}
		
		log.info("cleared: "+cleared);
		
	}
	
	
	public void parseNeighbors(String fname){
		
		try {
			BufferedReader bf=new BufferedReader(new FileReader(fname));
			
			String thisLine;
			Integer indexOfNodes=-10;
		
			
			while ((thisLine = bf.readLine()) != null) { // while loop begins here
			
				//parse the users
				String[] users= getTwoUsers(thisLine);
				//failed case
				if(users==null){
					log.warn("empty users");
					continue;
				}
				if(thisLine.startsWith("ID")){
					//a new user
					indexOfNodes=Integer.parseInt(users[1]);
					
					//log.info("ID: "+indexOfNodes);
					
				}else if(thisLine.contains(" FID ")){
					//only record nodes with files
					if(!userFiles.containsKey(indexOfNodes)){
						userFiles.put(indexOfNodes, new HashSet<Integer>(2));
					}
					
					//the file id of a user
					//log.info("FID: "+users[1]+", id: "+indexOfNodes);
					Set<Integer> list = userFiles.get(indexOfNodes);
					list.add(Integer.parseInt(users[1]));
					userFiles.put(indexOfNodes,list);
				}
				
			}//end of each readline
		
			bf.close();
			

			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * parse the common files
	 * @param fname
	 * @param bufferedWriter
	 * @throws IOException
	 */
	public void parseCommonFiles(String fname,BufferedWriter bufferedWriter) throws IOException{
		
		
		try {
			int NoFiles;
			BufferedReader bf=new BufferedReader(new FileReader(fname));
			
			String thisLine;
			Integer indexOfNodes=-10;
		
			
			while ((thisLine = bf.readLine()) != null) { // while loop begins here
			
				//parse the users
				String[] users= getTwoUsers(thisLine);
				//failed case
				if(users==null){
					log.warn("empty users");
					continue;
				}
				
					 //NoFiles=Integer.parseInt(users[1]);
					 bufferedWriter.append(users[2]);
					 bufferedWriter.newLine();
					 bufferedWriter.flush();
				
			}//end of each readline
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * get two users A and B
	 * @param thisLine
	 * @return
	 */
	public static String[] getTwoUsers(String thisLine) {
		// TODO Auto-generated method stub
		//match the space
		thisLine=thisLine.trim();
		String[] tmp = thisLine.split("\\s+");
		return tmp;
	}

	public static String[] getKeyParameters(String thisLine) {
		thisLine=thisLine.trim();
		String[] tmp = thisLine.split("[\\s:();]+");
		return tmp;
	}
	
	
	/**
	 * get the maximal number of items
	 * @return
	 */
	public int getMaxNeighbors() {
		// TODO Auto-generated method stub
		return maxNeighbor;
	}
	
	public static void main(String[] args){
		
		String str="2000 1.0000e-15 2: 4.2719e-18 (4096 64) (6 4)";
		String[] a = getKeyParameters(str);
		if(a!=null&&a.length>0){
			for(int i=0;i<a.length;i++){
				System.out.println(a[i]);
			}
		}
	}
}
