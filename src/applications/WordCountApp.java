package applications;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import mapreduce.MapInput;
import mapreduce.ReduceInput;
import mapreduce.MapReduce;
import mapreduce.MapReduceSpecification;
import mapreduce.Mapper;
import mapreduce.Reducer;

/**
 * A simple application which reads in a text file and obtains key-value pairs
 * which consist of the unique words in the text file and their counts.
 */
public class WordCountApp {

	public static class WordCounter extends Mapper {

		public void map(MapInput input) throws IOException {
			String line = null;
			line = input.readLine();
			while (line != null) {
				// Tokenize based simply on spaces
				String[] words = line.trim().split(" ");

				for (String word : words) {
					emit(word, "1");
				}

				line = input.readLine();
			}
		}
	}

	public static class Adder extends Reducer {
		@Override
		public void reduce(ReduceInput input) throws IOException {
			int total = 0;
			// Sum all the values for input.getKey()
			while (!input.done()) {
				total += Integer.parseInt(input.value());
				input.nextValue();
			}

			// Emit one (key, count) pair
			emit(String.valueOf(total));
		}
	}

	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			System.out.println("Error: Write the name of a config file on the command line.");
			System.exit(1);
		}
		String cfgRelPath = "./config/" + args[0];
		String cfgAbsPath = Paths.get(cfgRelPath).toRealPath(LinkOption.NOFOLLOW_LINKS).toString();

		// Load specification from config file
		MapReduceSpecification spec = new MapReduceSpecification(cfgAbsPath, " Word Count");

		// Set up job and execute
		MapReduce mr = new MapReduce(spec, WordCounter.class, Adder.class);
		mr.execute(args);
		
		System.exit(0);
	}

}
