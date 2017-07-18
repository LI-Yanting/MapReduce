
import java.io.*;

public class DEPLOY {
	public static class ReadThread extends Thread{
		InputStream is = null;
		
		public ReadThread (InputStream inputStream) {
			super();
			is = inputStream;
		}
		
		public void run() {
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String line;
			try {
				while((line = br.readLine()) != null) {
					System.out.println(line);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public static void main(String[] args) throws IOException {
		ReadThread[] readThreads = new ReadThread[15];
		int i=1;
		BufferedReader br = new BufferedReader(new FileReader("c133.txt"));
		String line;
		String localPath = "";
		ProcessBuilder pb, pbRead;
		Process shell;
		while((line=br.readLine()) != null) {
//			System.out.println("yali@"+line+".enst.fr");
//			pb = new ProcessBuilder("ssh", "yali@"+line+".enst.fr", "hostname");
			pb = new ProcessBuilder("ssh", "yali@"+line+".enst.fr", "mkdir -p /tmp/yali/");
			pb.start();
			pbRead = new ProcessBuilder("scp", localPath+"slave.jar", "yali@"+line+".enst.fr:/tmp/yali/");
			shell = pbRead.start();
			InputStream shellIn = shell.getInputStream();
			readThreads[i] = new ReadThread(shellIn);
			readThreads[i].start();
		}
		br.close();
	}
}
