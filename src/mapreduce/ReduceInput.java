package mapreduce;

import java.io.BufferedReader;
import java.io.IOException;

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