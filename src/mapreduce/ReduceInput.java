package mapreduce;

import java.io.BufferedReader;
import java.io.IOException;

/*
 * A possibly unnecessary wrapper around a BufferedReader before
 * the reader is passed on to a reducer worker.
 * We are still determining whether this class is needed.
 */
public class ReduceInput {
	private BufferedReader reader;
	
	public ReduceInput(BufferedReader reader) {
		this.reader = reader;
	}
	
	public String readLine() throws IOException {
		String line = reader.readLine();
		if(line != null) return line;
		else return null;
	}

}