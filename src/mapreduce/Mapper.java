package mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;

//Does this NEED to be abstract? Why/why not
public abstract class Mapper {
	private int numWorkers;
	private BufferedWriter[] writers;
	private Path[] intermediateFilePaths;
	private long inputSize;
	private Path inputFilePath;
	private Path intermediateDir;
	private int workerNumber;
	
	public abstract void map(MapInput input);
	
	public void execute() throws IOException {
		// TODO What could go wrong with casting inputSize from long down to int here?
		int inputSize = Math.toIntExact(this.inputSize);
		
		// Suggested byte offsets
		// TODO Do we need a ceiling operation on inputSize / this.numWorkers here?
		int start = this.workerNumber * inputSize / this.numWorkers;
		int end = Math.min((this.workerNumber + 1) * inputSize / this.numWorkers, inputSize-1);
		int windowWidth = end-start+1;
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
		MapInput mapInput = new MapInput(reader, numCharsToRead);
		
		// Set up writers
		this.setIntermediateFilePaths();
		this.setWriters();
		
		// Pass reader to map()
		this.map(mapInput);
		
		// Close writer and reader streams
		this.closeAll();
		reader.close();
	}
	
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
	
	private Path[] getIntermediateFilePaths() {
		return this.intermediateFilePaths;
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
	
}
