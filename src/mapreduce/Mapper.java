package mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;

/*
 * Users define their map functions by creating a class which extends this 
 * abstract Mapper class and implements the abstract function "map". The rest
 * of the code in this class facilitates the work of a mapper worker.
 */
public abstract class Mapper {
	private static final int PORT = 9002;
	private int numWorkers;
	private BufferedWriter[] writers;
	private Path[] intermediateFilePaths;
	private long inputSize;
	private Path inputFilePath;
	private Path intermediateDir;
	private int workerNumber;
	
	public abstract void map(MapInput input); // Implemented by the user
	
	/*
	 * This function carries out the work of a single mapper worker, which
	 * involves finding the appropriate byte ranges of the input file to 
	 * read and creating a corresponding MapInput object, and finally invoking
	 * the user-defined "map" function.
	 */
	public void execute() throws IOException {
		// TODO What could go wrong with casting inputSize from long down to int here?
		int inputSize = Math.toIntExact(this.inputSize);
		
		// Suggested byte offsets
		// TODO Do we need a ceiling operation on inputSize / this.numWorkers here?
		int start = this.workerNumber * inputSize / this.numWorkers;
		int end = Math.min((this.workerNumber + 1) * inputSize / this.numWorkers, inputSize-1);
		int windowWidth = end-start+1;
		System.err.printf("Worker: %d\tStart: %d\tEnd: %d\tWindow Width: %d\n", this.workerNumber, start, end, windowWidth);
		// Note: in the case that n=1, then end= 1 * inputSize / 1 = inputSize
		// So in total should read from byte 0=start up to and including byte (inputSize-1)=end
		// and a given worker will reader from as early as the (start+1)-th byte up to and including the end-th
		
		FileInputStream fileStream = null;
		try {
			fileStream = new FileInputStream(this.inputFilePath.toString());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		InputStreamReader inputReader = new InputStreamReader(fileStream);
		BufferedReader reader = new BufferedReader(inputReader);
		
		// Adjust start
		reader.skip(start); // The first byte we read will be the (start)-th, counting up from zero
		reader.mark(inputSize); // TODO probably a smarter readAheadLimit to choose here
		if(start != 0) {
			// Find the next newline starting from start
			try {
				String line = reader.readLine();
				if(line == null || (line.length() >= windowWidth)) {
					throw new IOException();
				}
				int i = line.length(); // skip(start+i) would make the next char read in '\n'
				reader.mark(inputSize);
				reader.skip(end - (start + i + 1)); // Now skip to the coarse byte boundary for 'end'
				start = start + i + 1;
			} catch (IOException e) { // TODO IOException is not really the most precise exception to use here
				System.out.printf("Worker %d reached 'end' "
						+ "without seeing a newline while adjusting 'start'.\n", this.workerNumber);
				e.printStackTrace();
			}
		} else {
			reader.skip(end);
		}
		
		// Adjust end
		if(end != inputSize-1) {
			try {
				String line = reader.readLine();
				if(line == null || (line.length() >= windowWidth)) {
					throw new IOException();
				}
				int i = line.length();
				end = end + i;
			} catch (IOException e) {
				System.out.printf("Worker %d passed worker %d's 'end' without seeing a newline "
						+ "while adjusting its own 'end'.\n", this.workerNumber, this.workerNumber+1);
				e.printStackTrace();
			}
		}
		
		reader.reset(); // Should take us back to the updated starting point for this chunk
		int numCharsToRead = end-start+1;
		System.err.printf("Worker: %d\tStart: %d\tEnd: %d\tnumCharsToRead: %d\n", this.workerNumber, start, end, numCharsToRead);
		MapInput mapInput = new MapInput(reader, numCharsToRead);
		
		// Set up writers
		this.setIntermediateFilePaths();
		this.setWriters();
		
		// Pass reader to user-defined map
		this.map(mapInput);
		
		// Close writer and reader streams
		this.closeAll();
		reader.close();
	}
	
	/*
	 * Emit hashes the incoming key to determine which reducer worker it will
	 * be processed by, and then writes the (kev,value) pair to the corresponding 
	 * buffer for that reducer worker.
	 */
	public void emit(String key, String value) throws IOException {
		//System.out.printf("Mapper emitting key: %s, value %s", key, value);
		
		// Compute h = hash(key) % this.numWorkers
		int h = Math.abs(key.hashCode()) % this.numWorkers;
		
		String output = "(" + key + "%&%" + value + ")" + "\n";
		
		// Write "(key, value)" to the h-th BufferedReader for this mapper
		BufferedWriter writer = this.writers[h];
		writer.write(output);
	}
	
	public void setN(int n) {
		this.numWorkers = n;
	}
	
	
	public void setInputSize(long size) {
		this.inputSize = size;
	}
	
	
	public void setInputFilePath(Path inputFilePath) {
		this.inputFilePath = inputFilePath;
	}
	
	public void setWorkerNumber(int k) {
		this.workerNumber = k;
	}
	
	public void setIntermediate(Path intermediateDir) {
		this.intermediateDir = intermediateDir;
	}
	
	private void setIntermediateFilePaths() {
		Path dir = intermediateDir;
		this.intermediateFilePaths = new Path[this.numWorkers];
		for(int i=0; i < this.numWorkers; i++) {
			String workerExtension = "mapper" + "-" + this.workerNumber + "-" + i;
			this.intermediateFilePaths[i] = dir.resolve(workerExtension);
		}
	}
	
	private String[] getIntermediateFilePaths() {
		String[] paths = new String[this.numWorkers];
		for(int i=0; i<this.numWorkers; i++) {
			paths[i] = this.intermediateFilePaths[i].toString();
		}
		return paths;
	}
	
	private void setWriters() {
		this.writers = new BufferedWriter[this.numWorkers];
		for(int i=0; i < this.numWorkers; i++) {
			String currFile = this.intermediateFilePaths[i].toString();
			try {
				this.writers[i] = new BufferedWriter(new FileWriter(currFile));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void closeAll() {
		for(int i=0; i < this.numWorkers; i++) {
			try {
				this.writers[i].close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws IOException {
		String udfMapperClassName = args[0];
		int numWorkers = Integer.parseInt(args[1]);
		int workerNum = Integer.parseInt(args[2]);
		long inputSize = Long.parseLong(args[3]);
		Path intermediateDirName = Paths.get(args[4]);
		Path inputFilePath = Paths.get(args[5]);
		
		Socket clientSocket = new Socket("localhost", PORT);
		
		BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
//		out.printf("%d\n", workerNum); // Send worker number
		
		Thread pinger = new Thread(new ServerConnection(clientSocket));
		pinger.start();
		
		Mapper m = null;
		try {
			Class udfMapperClass = Class.forName(udfMapperClassName);
			try {
				m = (Mapper) udfMapperClass.getConstructor().newInstance();
				
				m.setN(numWorkers); // Give mapper the # of workers
				m.setWorkerNumber(workerNum); // Give mapper its worker number
				m.setInputSize(inputSize); // Give mapper the file size
				m.setIntermediate(intermediateDirName);
				m.setInputFilePath(inputFilePath);
				
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException | SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try {
			m.execute();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String[] paths = m.getIntermediateFilePaths();
		
		// Stop pinging
		pinger.interrupt();
		try {
			pinger.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String fp = "Filepaths\n" + String.join("\t", paths);
		out.println(fp);
		
		// Wait to hear "received"
		while(true) {
			String line = in.readLine();
			
			if(line != null && line.contains("received")) {
				System.err.printf("Received msg from server\n");
				break;
			}
		}
		
		//out.close();
		clientSocket.close();
	}
	
}
