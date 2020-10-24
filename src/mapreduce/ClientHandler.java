package mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientHandler implements Runnable {
	private Socket client;
	private BufferedReader in;
	private PrintWriter out;
	private String [] filePaths;
	private final AtomicBoolean finished = new AtomicBoolean(false);
	private final long sleepDuration = 1000;
	private int workerNum;
	private final AtomicBoolean successful = new AtomicBoolean(false);
	
	public ClientHandler(Socket socket, int workerNum) throws IOException {
		this.client = socket;
		this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		this.out = new PrintWriter(socket.getOutputStream(), true);
		this.workerNum = workerNum;
	}
	
	
	public String[] getFilePaths(){
		return this.filePaths;
	}
	
	public boolean isFinished(){
		return this.finished.get();
	}
	
	public int getWorkerNum(){
		return this.workerNum;
	}

	@Override
	public void run() {
		try {
			//System.out.printf("ClientHandler for worker %d firing up.\n", this.workerNum);
			this.out.println("ping"); // Send out first ping
			while(true) {
				Thread.sleep(sleepDuration);
				//System.out.printf("ClientHandler for worker %d reading line.\n", this.workerNum);
				String clientResponse = this.in.readLine(); // Will this wait for a response?
				//System.out.printf("ClientHandler for worker %d received: %s\n", this.workerNum, clientResponse);
				
				if(clientResponse == null) { // Presumably means no response
					System.out.printf("ClientHandler for worker %d received 'null'.\n", this.workerNum);
					break;
				}
				else if(clientResponse.contains("Filepaths")) {
					this.filePaths = this.in.readLine().split("\t");
					System.out.printf("ClientHandler for worker %d received filepaths: \n", this.workerNum);
					System.out.println(Arrays.toString(this.filePaths));
					
					this.out.println("received"); // Client will not try to read again
					this.successful.set(true);
					break;
				}
				else if(clientResponse.contains("Finished")) {
					System.out.printf("ClientHandler for worker %d received 'Finished'.\n", this.workerNum);
					this.out.println("received");
					this.successful.set(true);
					break;
				}
				else if(clientResponse.contains("ping")){
					// We got a ping of some sort, continue.
					System.out.printf("ClientHandler for worker %d got a ping.\n", this.workerNum);
					this.out.println("ping");
				}
				else {
					// What to do here?
				}
			}
		} catch (Exception e) {
			this.finished.set(true);
			e.printStackTrace();
		} finally {
			this.finished.set(true);
		}
	}


	public boolean successful() {
		// TODO Auto-generated method stub
		return this.successful.get();
	}

}
