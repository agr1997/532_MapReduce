package mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ServerConnection implements Runnable {
	private Socket socket;
	private BufferedReader in;
	private PrintWriter out;
	// private final AtomicBoolean running = new AtomicBoolean(false);

	public ServerConnection(Socket socket) throws IOException {
		this.socket = socket;
		this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		this.out = new PrintWriter(socket.getOutputStream(), true);
	}

	@Override
	public void run() {
		// Listens for pings, and responds when it gets a ping
		try {
			while (!Thread.interrupted()) {
				if (!this.socket.isClosed()) {
					String serverResponse = this.in.readLine();

					if (serverResponse != null && serverResponse.equals("ping")) {
						this.out.println("ping");
					}
				} else
					break;
			}
			// throw new InterruptedException();
		} catch (IOException e) {
			// this.out.println("ping"); // One final ping?
			e.printStackTrace();
		} finally { // Not 100% sure this will execute when the thread is interrupted
			if (!this.socket.isClosed()) {
				this.out.println("ping"); // One final ping?
			}
		}

	}

}
