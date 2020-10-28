package applications;

import java.io.*;
import java.nio.file.*;
import mapreduce.*;

/**
 * A test application which processes a text file annotated with line numbers
 * and outputs lines that have > 10 words.
 */
public class GrepApp {
	public static class GrepMapper extends Mapper {

		final int MIN_LEN = 11;

		@Override
		public void map(MapInput input) throws IOException {
			String line = null;
			line = input.readLine();
			System.out.println(line);
			while (line != null) {
				// Skip over white space to next word
				String[] words = line.trim().split(" ");

				// If the line isn't long enough, continue.
				// First word is line number, not part of sentence.
				if (words.length < MIN_LEN + 1) {
					line = input.readLine();
					continue;
				}

				// Recreate sentence
				String lineNum = "";
				String sentence = "";
				for (int i = 0; i < words.length; i++) {
					if (i == 0) {
						lineNum = words[i];
					} else {
						sentence += words[i] + " ";
					}
				}
				sentence = sentence.trim();

				// Emit line
				emit(lineNum, sentence);
				// Read next line
				line = input.readLine();
			}
		}
	}

	public static class GrepReducer extends Reducer {
		@Override
		public void reduce(ReduceInput input) throws IOException {
			String value = "";
			// Should only be 1 value, the line.
			while (!input.done()) {
				value = input.value();
				input.nextValue();
			}
			emit(String.valueOf(value));
		}
	}

	public static void main(String[] args) throws IOException {
		if (args[0] == null) {
			System.out.println("Write the name of a config file on the command line.");
		}
		String cfgRelPath = "./config/" + args[0];
		String cfgAbsPath = Paths.get(cfgRelPath).toRealPath(LinkOption.NOFOLLOW_LINKS).toString();

		// Load specification from config file
		MapReduceSpecification spec = new MapReduceSpecification(cfgAbsPath, "Grep");

		MapReduce mr = new MapReduce(spec, GrepMapper.class, GrepReducer.class);

		mr.execute(args);
	}

}
