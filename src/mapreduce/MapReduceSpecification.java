package mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Arrays;

/**
 * Reads in and parses a configuration file for a map reduce job
 * and stores the configuration details in an object is then passed
 * to the constructor for MapReduce. Info includes the path to the input file,
 * the number N of workers, and the directory where the outputs will be
 * placed.
 */
public class MapReduceSpecification {
	private int n; // Number of workers
	private Path input; // Input file path
	private Path output; // Output directory path
	private String name; // Name of map reduce job

	/**
	 * Constructor.
	 * 
	 * @param config_file - plain text file w/ each param on a different line
	 * @param job_name - the user's name for the job they are creating
	 * @throws IOException
	 */
	public MapReduceSpecification(String config_file, String job_name) throws IOException {		
		FileInputStream fileStream = new FileInputStream(config_file);
		InputStreamReader inputReader = new InputStreamReader(fileStream);
		BufferedReader reader = new BufferedReader(inputReader);
		
		// Format the job name
		if(job_name.isEmpty()) {
			System.out.println("Error: Job name must be nonempty.");
		}
		String[] name = job_name.toLowerCase().trim().split(" ");
		List<String> list = Arrays.asList(name);
		String result = String.join("-", list);
		this.name = result;
		
		// Extract details from the config file
		String line = null;
		try {
			line = reader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		while(line != null) {
			// Process the line
			line = line.trim();
			String[] pieces = line.split(":");
			if(pieces.length != 2) {
				System.err.println("Please make sure that each line in config file looks like <type> : <value>.");
				throw new IOException();
			}
			
			String component = pieces[0].trim().toLowerCase();
			String value = pieces[1].trim();
			if(component.equals("n")) {
				this.n = Integer.parseInt(value);
			}
			else if(component.equals("input") || component.equals("output")) {
				// Check if a valid path
				Path path = Paths.get(value).toRealPath(LinkOption.NOFOLLOW_LINKS);
				
				// Check that the output file directory is valid
				if(component.equals("output")) {
					String file_name = path.getFileName().toString();
					if(file_name.isEmpty()) {
						System.out.println("Error: Filebase for output files must end in name of directory.");
						throw new IOException();
					}
					
					path = path.resolve(this.name + "-" + this.n + "-output");
					
					File dir = new File(path.toString());
					if(!dir.mkdir()) {
						System.out.println("Error: Could not make output file directory.");
						throw new IOException();
					}
				}
				
				if(component.equals("input")) {
					this.input = path;
				}
				else {
					this.output = path;
				}
			}
			else {
				String err_msg = "Could not process the lefthand side of ':' in this line of config file: %s \n";
				System.out.printf(err_msg, line);
				throw new IOException();
			}
			
			line = reader.readLine();
		}
	}
	
	public int getN() {
		return this.n;
	}
	
	public Path getInput() {
		return this.input;
	}
	
	public Path getOutput() {
		return this.output;
	}
	
	public String getName() {
		return this.name;
	}


}
