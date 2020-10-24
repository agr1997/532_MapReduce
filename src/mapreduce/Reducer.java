package mapreduce;

import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.Socket;
import java.nio.file.Path;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.code.externalsorting.ExternalSort;
import static java.nio.file.StandardOpenOption.*;
import java.nio.channels.FileChannel;

/*
 * Users define their reduce functions by creating a class which 
 * extends this abstract Mapper class and implements the abstract 
 * function "reduce". The rest of the code is necessary to carry
 * out the work of a reducer.
 */
public abstract class Reducer {
	// private int numWorkers;
	private static final int PORT = 9002;
	private int workerNumber;
	private BufferedWriter writer;
	private Path intermediateDir;
	private Path finalDir;
	private Path mergeSortFilePath;
	private Path finalFilePath;

	public abstract void reduce(ReduceInput input); // Implemented by the user

	/*
	 * A function which obtains the intermediate files produced by the mappers and
	 * sorts them by key using an external sorting library from Google.
	 */
	public void mergeAndSort(String[] intermediateFilePaths) throws IOException {
		Path mergePath = this.intermediateDir.resolve("merge-reducer-" + workerNumber); // merged file
		this.mergeSortFilePath = this.intermediateDir.resolve("sorted-reducer-" + workerNumber); // merged&sorted file

		try (FileChannel out = FileChannel.open(mergePath, CREATE, WRITE)) {
			for (String path : intermediateFilePaths) {
				try (FileChannel in = FileChannel.open(Paths.get(path), READ)) {
					for (long p = 0, l = in.size(); p < l;)
						p += in.transferTo(p, l - p, out);
				}
			}
		}

		// Using external sort library
		ExternalSort.mergeSortedFiles(ExternalSort.sortInBatch(new File(mergePath.toString())),
				new File(this.mergeSortFilePath.toString()));
	}

	/*
	 * A function which carries out the work of a given reducer worker, which
	 * receives and sorts the intermediate files from the mappers and invokes the
	 * user-defined reduce function on those results.
	 */
	public void execute(String[] intermediateFilePaths) throws IOException {
		// setting few private variables uninstantitated from MRSpecification
		// TODO Maintain consistency. Call from MRSpec?
		this.setFinalFilePath();
		this.setMergeSortFilePath();

		// merge mapper files and sort
		mergeAndSort(intermediateFilePaths);
		// try and see if merge_sort file present
		FileInputStream fileStream = null;
		try {
			fileStream = new FileInputStream(this.mergeSortFilePath.toString());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		InputStreamReader inputReader = new InputStreamReader(fileStream);
		BufferedReader reader = new BufferedReader(inputReader);
		ReduceInput reduceInput = new ReduceInput(reader);
		// setting writers
		this.setWriter();
		// pass merge_sort_file as reduceInput to reduce fn
		this.reduce(reduceInput);
		reader.close();
		this.closeWriter();
	}

	// emit function as per MR interface
	// acts as a line builder for the reducer
	public void emit(String key, String value) throws IOException {
		String output = "(" + key + "%&%" + value + ")" + "\n";
		//String output = "" + key + " " + value + "\n";
		try {
			this.writer.write(output);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// setter for workerNumber
	public void setWorkerNumber(int k) {
		this.workerNumber = k;
	}

	// setter for intermediate Dir
	public void setIntermediate(Path intermediateDir) {
		this.intermediateDir = intermediateDir;
	}

	// setter for finalDir
	public void setFinal(Path finalDir) {
		this.finalDir = finalDir;
	}

	// setter for finalFilePath
	public void setFinalFilePath() {
		Path dir = finalDir;
		String reducerExtension = "reducer-" + this.workerNumber;
		this.finalFilePath = dir.resolve(reducerExtension);
	}

	// setter for mergeSortFilePath
	public void setMergeSortFilePath() {
		Path dir = intermediateDir;
		String reducerExtension = "reducer-" + this.workerNumber;
		this.mergeSortFilePath = dir.resolve(reducerExtension);
	}

	// getter for finalFilePath
	public Path getFinalFilePath() {
		return this.finalFilePath;
	}

	// setting writers for concurrent tasks
	public void setWriter() {
		try {
			this.writer = new BufferedWriter(new FileWriter(this.finalFilePath.toString()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// close all writers after reducer tasks are complete
	public void closeWriter() {
		try {
			this.writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		String udfReducerClassName = args[0];
		int workerNum = Integer.parseInt(args[1]);
		Path intermediateDirName = Paths.get(args[2]);
		Path finalDir = Paths.get(args[3]);

		Socket clientSocket = new Socket("localhost", PORT);

		BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

		String[] intermediateFilePaths = null;
		while (true) {
			String serverMsg = in.readLine();

			if (serverMsg != null && serverMsg.contains("Filepaths")) {
				intermediateFilePaths = in.readLine().split("\t");
				break;
			}
		}
		
		Thread pinger = new Thread(new ServerConnection(clientSocket));
		pinger.start();
//		ExecutorService pinger= Executors.newSingleThreadExecutor();
//		pinger.execute(new ServerConnection(clientSocket));

		Reducer r = null;
		try {
			Class udfReducerClass = Class.forName(udfReducerClassName);
			try {
				r = (Reducer) udfReducerClass.getConstructor().newInstance();

				r.setWorkerNumber(workerNum); // Give mapper its worker number
				r.setIntermediate(intermediateDirName);
				r.setFinal(finalDir);

			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException | SecurityException e) {
				e.printStackTrace();
			}
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}

		try {
			r.execute(intermediateFilePaths);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Submit intermediate file paths to master
		System.err.printf("reducer %d finished, stopping pinger\n", workerNum);
//		pinger.shutdownNow();
		
		pinger.interrupt();
		try {
			pinger.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		out.println("Finished");		
		while(true) {
			String line = in.readLine();
			
			if(line.contains("received")) {
				System.err.printf("Received msg from server\n");
				break;
			}
		}
		
		clientSocket.close();
	}
}
