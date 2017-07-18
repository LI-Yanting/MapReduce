import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

public class SLAVE {
	public static void main(String[] args) throws IOException {		
		if(args[0].equals("0")) {
			String fileName = args[1];
			String fileId = fileName.replaceAll("[^0-9]", "");
//			Mode 0: Sx to UMx
			ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", "/tmp/yali/maps/");
			pb.start();
			String outputFile = "/tmp/yali/maps/UM" + fileId;
			TreeMap<String, Integer> UMx = readToUM(fileName);
			outputToUM(UMx,outputFile);
		}
		else if(args[0].equals("1")) {
			//wait
//			System.out.println("Wait for next step!");
			String key = args[1];
			String UMxs = args[2];
			String fileId = args[3];
			outputToSM(key, UMxs, fileId);
		}
	}


	/**
	 * For mode 0: read file and generate UMx, Sx to UMx
	 * @param Sx
	 * @return
	 * @throws IOException
	 */
	public static TreeMap<String, Integer> readToUM(String Sx) throws IOException {
		TreeMap<String, Integer> UM = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
//		Read file:
		BufferedReader br = new BufferedReader(new FileReader(Sx));
		String line;
		String [] words;
		while((line=br.readLine()) != null) {
			words = line.split(" ");
			for(String w : words) {
				if(w.isEmpty()) continue;
				
				Integer n = UM.get(w);
				if(n==null) n=1;
				else n+=1;
				UM.put(w, n);
			}
		}
		br.close();	
		return UM;
	}

	/**
	 * For mode 0: output the map to UMx
	 * @param occurrence
	 * @param outFilePath
	 * @param outputFile
	 * @throws IOException
	 */
	public static void outputToUM(TreeMap<String, Integer> UM, String outputFile) throws IOException {
//		Output to file:
		File fout = new File(outputFile+".txt");
		FileOutputStream fos = new FileOutputStream(fout);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		
		bw.newLine();
////		For Question 49:
//		for (Entry<String, Integer> entry : UM.entrySet()) {
//			Integer value = entry.getValue();
//			for(int i=0; i<value; i++) {
//				System.out.println(entry.getKey() + " 1");
//				bw.write(entry.getKey() + " 1");
//				bw.newLine();
//			}
//		}
//		For Question 51:
		for (Entry<String, Integer> entry : UM.entrySet()) {
			System.out.println(entry.getKey());
			Integer value = entry.getValue();
			for(int i=0; i<value; i++) {
				bw.write(entry.getKey() + " 1");
				bw.newLine();
			}
		}
		bw.close();
	}
	
	public static void outputToSM(String key, String UMxs, String fileId) 
			throws IOException {
//		Output to file:
		String outputFile = "/tmp/yali/maps/SM" + fileId + ".txt";
		String filePath = "/tmp/yali/maps/";
		File fout = new File(outputFile);
		FileOutputStream fos = new FileOutputStream(fout);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		bw.newLine();
		
		String[] UMx = UMxs.split(" ");
		for(int i=0; i<UMx.length; i++) {
			// For each UMx: 
			BufferedReader br = new BufferedReader(new FileReader(filePath + UMx[i] + ".txt"));
			String line;
			while((line=br.readLine())!=null) {
				if(line.contains(key)) {
					System.out.println(line);
					bw.write(line);
					bw.newLine();
				}
			}
			br.close();
		}
		bw.close();
	}
	
////	Question before:
//	public static void main(String[] args) {
//		String mode = args[0];
//		if(mode.equals("0")){
//			int a,b,res;
//			a=3;
//			b=5;
//			res=a+b;
////			try {
////				Thread.sleep(5000);
////			} catch (InterruptedException e) {
////				// TODO Auto-generated catch block
////				e.printStackTrace();
////			}
//			System.out.println(res);
//		}
//		else System.out.println("Mode error!");
//
//	}
}
