package mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs in a thread of the 'master' process, handles the work of communicating
 * with a particular worker process over a socket.
 */
public class ClientHandler implements Runnable {
	private Socket client;
	private BufferedReader in;
	private PrintWriter out;
	private String[] filePaths;
	private final AtomicBoolean finished = new AtomicBoolean(false);
	private final AtomicBoolean successful = new AtomicBoolean(false);
	private final int sleepDuration = 2000; // In milliseconds
	private int workerNum;
	private boolean reducer = false;

	/**
	 * Constructor, for cases where the socket input and output stream are not
	 * opened prior to creation of the ClientHandler.
	 * 
	 * @param socket    - the socket created while listening server-side
	 * @param workerNum - the worker with whom this will communicate
	 */
	public ClientHandler(Socket socket, int workerNum) throws IOException {
		this.client = socket;
		this.client.setSoTimeout(sleepDuration);
		this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		this.out = new PrintWriter(socket.getOutputStream(), true);
		this.workerNum = workerNum;
	}

	/**
	 * Constructor, for cases where the socket input and output stream are opened
	 * prior to creation of the ClientHandler.
	 * 
	 * @param socket    - the socket created while listening server-side
	 * @param workerNum - the worker with whom this will communicate
	 * @param in        - the input stream reader for the socket
	 * @param out       - the output stream writer for the socket
	 * @param reducer   - 'true' if the worker is a reducer worker
	 */
	public ClientHandler(Socket socket, int workerNum, BufferedReader in, PrintWriter out, boolean reducer)
			throws IOException {
		this.client = socket;
		this.client.setSoTimeout(sleepDuration);
		this.in = in;
		this.out = out;
		this.workerNum = workerNum;
		this.reducer = reducer;
	}

	/**
	 * Returns the file paths received from a mapper worker in a case of a
	 * successful worker task completion.
	 * 
	 * @return the string array of file paths received from the worker, or 'null' if
	 *         no such paths have yet been received.
	 */
	public String[] getFilePaths() {
		return this.filePaths;
	}

	/**
	 * @return 'true' if the ClientHandler has finished communicating with the
	 *         worker, whether or not the exchange was successful.
	 */
	public boolean isFinished() {
		return this.finished.get();
	}

	public int getWorkerNum() {
		return this.workerNum;
	}

	public boolean successful() {
		return this.successful.get();
	}

	/**
	 * Communicates with the worker, sending pings until either there is a timeout
	 * waiting to hear a ping back, or until some expected string is received from
	 * the worker indicating successful completion of its work.
	 */
	@Override
	public void run() {
		try {
			// If reducer worker, want to first ensure it received file paths
			if (this.reducer) {
				while (true) {
					String line = in.readLine();

					// Received something incorrect, or broken connection
					if(line == null || !line.contains("received")) {
						throw new IOException();
					}
					else {
						//System.out.println("CH heard that reducer received fps.");
						break;
					}
				}
			}

			this.out.println("ping"); // Send out first ping
//			long startTime;
//			long endTime;
			while (true) {
//				startTime = System.nanoTime();
				String clientResponse = this.in.readLine();

				if (clientResponse == null) {
					// Input stream is closed client-side
					System.out.printf("ClientHandler for worker %d received 'null'. Worker will need to be restarted.\n", this.workerNum);
					break;
				} else if (clientResponse.contains("ping")) {
					//System.out.printf("ClientHandler for worker %d got a ping.\n", this.workerNum);
					this.out.println("ping");
				} else if (clientResponse.contains("Filepaths")) {
					this.filePaths = this.in.readLine().split("\t");
					// Let worker know the paths were received
					//System.out.println("Received filepaths...");
					this.out.println("received");
					this.successful.set(true);
					break;
				} else if (clientResponse.contains("Finished")) {
					//System.out.println("Received 'Finished' from reducer.");
					this.out.println("received");
					this.successful.set(true);
					break;
				} else {
					System.out.printf("ClientHandler for worker %d received unexpected string: %s.\n", this.workerNum,
							clientResponse);
				}
			}
		} catch (SocketException | SocketTimeoutException e) {
			System.out.printf("Master timed out waiting for a response from worker %d.\n", this.workerNum);
		} catch (IOException e) {
			e.printStackTrace();
			System.out.printf("Other IOException encountered in communicating with worker %d.\n", this.workerNum);
		} finally {
			this.finished.set(true);
			this.out.close();
		}
	}

}
