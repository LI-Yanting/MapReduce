
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MASTER {
	public static void main(String[] args) throws IOException, InterruptedException {
		ArrayBlockingQueue<String> arrayBlockingQueue = new ArrayBlockingQueue<String>(100);
		ArrayList<String> Sx = new ArrayList<String>();
		Sx.add("S0.txt");
		Sx.add("S1.txt");
		Sx.add("S2.txt");
		ArrayList<String> activeMachines = findMachines("c133.txt", arrayBlockingQueue);
		System.out.println("Numbers of machine available: "+activeMachines.size());
//		Q49,50
		HashMap<String, String> UMx_Machines = distribute(Sx, activeMachines);
		System.out.println("Question 50 - distribute:\nUMx_Machines: ");
		printMap(UMx_Machines);
		
//		Question 51: Mapping
		System.out.println("\nQuestion 51 - Lance SLAVEs and get result:\nNumbers of machines: " + UMx_Machines.size());
		ArrayList<ReadThread> SxToUMxThreads = new ArrayList<ReadThread>();
		TreeMap<String, String> key_UMx = new TreeMap<String, String>();
		for (Map.Entry<String, String> entry : UMx_Machines.entrySet()) {
			String[] cmd = {"ssh", entry.getValue(), "java", "-jar", "/tmp/yali/slave.jar", "0", "/tmp/yali/splits/"+ entry.getKey()};
			ReadThread readThread = executeCmd(cmd,3);
			SxToUMxThreads.add(readThread);
//			readThread.join();
			// Question 52:
			String fileId = entry.getKey().replaceAll("[^0-9]", "");  // Ex: S0 -> 0
			String keys = readThread.getKeys();
			// Do with UM0:
			keyToUMxs(key_UMx, keys, fileId);
		}
		
//		TreeMap<String, String> key_UMx = keyToUMx(UMx_Machines);
		System.out.println("key_UMx: ");
		printMap(key_UMx);
		
//		Question 53:
		if(!(UMx_Machines.isEmpty()||key_UMx.isEmpty())) {
			System.out.println("Phase de MAP terminée.");
		}
		
//		Question 54-56: Shuffle
//		String 
		HashMap<String, String> key_Machines = distribute(key_UMx, activeMachines);
		sendUMx(key_Machines, key_UMx, UMx_Machines);
		printMap(key_Machines);
		UMxToSMx(key_Machines, key_UMx);
		
	}

	public static ReadThread executeCmd(String[] cmd, int timeout) 
			throws IOException, InterruptedException {
		ArrayBlockingQueue<String> abq = new ArrayBlockingQueue<String>(100);
		ProcessBuilder pb = new ProcessBuilder(cmd);
		Process shell = pb.start();
		ReadThread stdRead = new ReadThread(shell.getInputStream(), abq);
		stdRead.start();
		
		if(abq.poll(timeout, TimeUnit.SECONDS) == null) {
			stdRead.setFlagTimeout(true);
			shell.destroy();
			stdRead.interrupt();
			System.out.println("Time out!");
		}
		return stdRead;
	}
	
	public static ArrayList<String> findMachines(String machineFileName, ArrayBlockingQueue<String> Abq) 
			throws IOException, InterruptedException {
		ArrayList<String> activeMachines = new ArrayList<String>();
//		Read machine names
		BufferedReader br = new BufferedReader(new FileReader(machineFileName));
		ArrayList<ReadThread> readThreads = new ArrayList<ReadThread>();
		String line;
		int id = 0;
		while((line=br.readLine()) != null) {
			ProcessBuilder pb = new ProcessBuilder("ssh", "yali@"+line, "hostname");
			Process shell = pb.start();
			ReadThread stdRead = new ReadThread(shell.getInputStream(), Abq);
			stdRead.start();
			readThreads.add(stdRead);
			id++;
		}
		br.close();
//		Wait for 3 seconds and check the first machine is active or not:
		String firstMachine = Abq.poll(3, TimeUnit.SECONDS);
//		If all the machines are block:
		if(firstMachine == null) {
			System.out.println("No machine available!");
		}
		else {	//If there are machines available, wait for all the threads finish: 
			activeMachines.add(firstMachine);
			for(int i=0; i<readThreads.size(); i++) {
				readThreads.get(i).join();
			}
		}
		int len_Abq = Abq.size();
		for(int i=0; i<len_Abq; i++) {
			activeMachines.add(Abq.poll());
		}
		return activeMachines;
	}
	
	/**
	 * For question 50
	 * @param UMx
	 * @param activeMachines
	 * @return
	 * @throws IOException 
	 */
	public static HashMap<String, String> distribute(ArrayList<String> files, ArrayList<String> machines) throws IOException {
		HashMap<String, String> UMx_Machines = new HashMap<String, String>();
		int len_UMx = files.size();
		int len_machines = machines.size();
		for(int i=0; i<len_UMx; i++) {
			String fileName = files.get(i);
			String machineName = machines.get(i%len_machines);
			UMx_Machines.put(fileName, machineName);
			ProcessBuilder pb = new ProcessBuilder("bash","-c","ssh " + machineName + 
					" \"mkdir -p /tmp/yali/splits/\"; scp splits/" + fileName + " " + machineName + ":/tmp/yali/splits/");
			pb.start();
		}
		return UMx_Machines;
	}

	/**
	 * 56
	 * @param keyMap
	 * @param machines
	 * @return
	 * @throws IOException
	 */
	public static HashMap<String, String> distribute(TreeMap<String, String> keyMap, ArrayList<String> machines) throws IOException {
		HashMap<String, String> key_Machines = new HashMap<String, String>();
		int len_map = keyMap.size();
		int len_machines = machines.size();
		int i=0;	//For allocating active machines
		for (Map.Entry<String, String> entry : keyMap.entrySet()) {
			String objMachineName = machines.get(i%len_machines);
			key_Machines.put(entry.getKey(), objMachineName);
			i++;
		}
		return key_Machines;
	}
	
	public static void sendUMx(HashMap<String, String> key_Machines, 
			TreeMap<String, String> key_UMx, HashMap<String, String> UMx_Machines) throws IOException {
		HashMap<String, String> machine_files = new HashMap<String, String>();
		for (Map.Entry<String, String> entry : key_Machines.entrySet()) {
			// For each key, find the object machine and then copy UMx files from different source machine to this object machine:
			String key = entry.getKey();
			String objMachine = entry.getValue();
			String[] UMx = key_UMx.get(key).split(" ");
			for(int i=0; i<UMx.length; i++) {
//				// Check object machine contains this UMx file or not
//				if(! machine_files.get(objMachine).contains(UMx[i])){
//
//				}
				// Find source machine
				String srcMachine = UMx_Machines.get(UMx[i]);
				// Copy from source machine to object machine
				String cmd_mkdir = "ssh " + objMachine + " \"mkdir -p /tmp/yali/maps/\";";
				String cmd_scp = "scp " + srcMachine + ":/tmp/yali/maps/" + "" + ".txt" + " " + objMachine + ":/tmp/yali/maps/";
				ProcessBuilder pb = new ProcessBuilder("bash","-c",cmd_mkdir+cmd_scp);
				pb.start();
			}
		}
	}
	
	public static void UMxToSMx(HashMap<String, String> key_Machines, TreeMap<String, String> key_UMx) 
			throws IOException, InterruptedException {
		System.out.println("\nQuestion 56 - Lance SLAVEs for shuffle and reduce:\nNumbers of machines. ");
		ArrayList<ReadThread> UMxToSMxThrs = new ArrayList<ReadThread>();
		
		int id = 0;
		for (Map.Entry<String, String> entry : key_Machines.entrySet()) {
			String[] cmd = {"ssh", entry.getValue(), "java", "-jar", "/tmp/yali/slave.jar", "1", key_UMx.get(entry.getKey()), String.valueOf(id++)};
			System.out.println(id);
//			ReadThread readThread = executeCmd(cmd,3);
			
			ArrayBlockingQueue<String> abq = new ArrayBlockingQueue<String>(100);
			ProcessBuilder pb = new ProcessBuilder(cmd);
			Process shell = pb.start();
			ReadThread readThread = new ReadThread(shell.getInputStream(), abq);
			readThread.start();
			UMxToSMxThrs.add(readThread);
			readThread.join();
			// Question 56:
			String content = readThread.getKeys();
			System.out.println(content);
		}
	}

	/**
	 * For question 52: Input is a String contains all the words in a UMx file, for example, in UM1, is "Car River"
	 * @param key_UMx
	 * @param line
	 */
	public static void keyToUMxs(TreeMap<String, String> key_UMx, String keys, String fileId)
	{
		String[] words = keys.split(" ");
		fileId = "UM" + fileId;
		for(int i=0; i<words.length; i++) {
//			for each word: first find in the map, if find, get value and add filename; if not, add
			if(words[i] != null) {
				if(key_UMx.containsKey(words[i])) {
					String files = key_UMx.get(words[i]);
					files = files + " " + fileId;
					key_UMx.put(words[i],files);
				}
				else {
					String files = fileId;
					key_UMx.put(words[i],files);
				}
			}
		}
	}
		
	public static void printMap(Map<String, String> mp) {
		for (Map.Entry<String, String> entry : mp.entrySet()) {
			System.out.println(entry.getKey() + " - " + entry.getValue());
		}
	}
}

class ReadThread extends Thread {
	InputStream is = null;
	ArrayBlockingQueue<String> abq = null;
	String keys;
	boolean flagTimeout;
	
	public ReadThread(InputStream inputStream, ArrayBlockingQueue<String> que) {
		super();
		is = inputStream;
		abq = que;
		keys = new String();
		flagTimeout = false;
	}
	
	public void run() {
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		String line;
		try {
			while((line = br.readLine()) != null) {
				abq.put(line);
				keys = keys + line + " ";
//					System.out.println("In run(): "+line);
			}
			br.close();
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public String getKeys() {
		return keys;
	}
	public void setFlagTimeout(boolean flag) {
		flagTimeout = flag;
	}
	public boolean getFlagTimeout(){
		return flagTimeout;
	}
}
