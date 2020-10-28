package mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

/**
 * Runs in a thread of a worker process, handles the work of replying to pings
 * from the master to indicate the worker is still reachable.
 */
public class ServerConnection implements Runnable {
	private Socket socket;
	private BufferedReader in;
	private PrintWriter out;
	private final long timeout = 4000; // In milliseconds

	/**
	 * Constructor, for cases where the socket input and output stream are not
	 * opened prior to creation of the ServerConnection.
	 * 
	 * @param socket - the socket opened by the worker
	 */
	public ServerConnection(Socket socket) throws IOException {
		this.socket = socket;
		this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		this.out = new PrintWriter(socket.getOutputStream(), true);
	}

	/**
	 * Constructor, for cases where the socket input and output stream are opened
	 * prior to creation of the ServerConnection.
	 * 
	 * @param socket - the socket opened by the worker
	 * @param in     - the input stream reader for the socket
	 * @param out    - the output stream writer for the socket
	 */
	public ServerConnection(Socket socket, BufferedReader in, PrintWriter out) throws IOException {
		this.socket = socket;
		this.socket.setSoTimeout((int) timeout);
		this.in = in;
		this.out = out;
	}

	/**
	 * Communicates with the master, sending pings each time a ping
	 * is received from the master.
	 */
	@Override
	public void run() {
		try {
			while (!Thread.interrupted()) {
				String serverResponse = this.in.readLine();

				// Received something unexpected, or broken connection
				if(serverResponse == null || !serverResponse.equals("ping")){
					throw new IOException();
				}
				// Received a ping from the master
				this.out.println("ping");
			}
		} catch (SocketException | SocketTimeoutException e) {
			System.err.printf("Timed out waiting for a ping from master.\n");
		}  catch (IOException e) {
			e.printStackTrace();
		} finally {
			 // One final ping, for good measure
			this.out.println("ping");
		}

	}

}
