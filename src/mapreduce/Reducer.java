package mapreduce;

import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import com.google.code.externalsorting.ExternalSort;
import static java.nio.file.StandardOpenOption.*;
import java.nio.channels.FileChannel;


public abstract class Reducer {
	private int numWorkers; 
	private int workerNumber;
	private BufferedWriter writer;
	private Path intermediateDir;
	private Path finalDir;
	private Path mergeSortFilePath;
	private Path finalFilePath;
	
	public abstract void reduce(ReduceInput input); // UDF
	
	public void merge_and_sort() throws IOException { //merging and sorting outputs of mappers by reducer worker number
		String glob = "glob:**/mapper-?-" + this.workerNumber; // fetch by reducer worker number
		final PathMatcher matcher = FileSystems.getDefault().getPathMatcher(glob); // instantiating PathMatcher
		Path mergePath = this.intermediateDir.resolve("merge-reducer-" + workerNumber); //merged file
		this.mergeSortFilePath = this.intermediateDir.resolve("sorted-reducer-" + workerNumber); // merged&sorted file
		
		// Open mapper files as channels and transfer to merge file channel
		try(FileChannel out=FileChannel.open(mergePath, CREATE, WRITE)) {
	        Files.walkFileTree(this.intermediateDir, new SimpleFileVisitor<Path>() {
	            @Override
	            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
	                if (matcher.matches(file)) {
	                    try(FileChannel in = FileChannel.open(file, READ)) {
	                    	for(long p=0, l = in.size(); p<l;)
	                    		p += in.transferTo(p, l-p, out);
	                    }
	                }
	                return FileVisitResult.CONTINUE;
	            }
	            @Override
	            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
	            	return FileVisitResult.CONTINUE;
	            }
	        });
		}
		
		// Using external sort library
		ExternalSort.mergeSortedFiles(ExternalSort.sortInBatch(
				new File(mergePath.toString())),
				new File(this.mergeSortFilePath.toString()));
	}
	
	public void execute() throws IOException {
		// setting few private variables uninstantitated from MRSpecification
		// TODO Maintain consistency. Call from MRSpec?
		this.setFinalFilePath();
		this.setMergeSortFilePath();
		
		// merge mapper files and sort
		merge_and_sort();
		// try and see if merge_sort file present
		FileInputStream fileStream = null;
		try {
			fileStream = new FileInputStream(this.mergeSortFilePath.toString());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		InputStreamReader inputReader = new InputStreamReader(fileStream);
		BufferedReader reader = new BufferedReader(inputReader); 
		ReduceInput reduceInput =  new ReduceInput(reader);
		// setting writers
		this.setWriter();
		// pass merge_sort_file as reduceInput to reduce fn
		this.reduce(reduceInput);
		reader.close();
		this.closeWriter();
	}
	
	// emit function as per MR interface
	// acts as a line builder for the reducer
	public void emit(String key, String value) throws IOException{
//		System.out.printf("Reducer emitting: %s:%s\n", key, value);
		String output = "(" + key + "%&%" + value + ")" + "\n";
		try {
			this.writer.write(output);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// setter for numWorkers
	public void setN(int n) {
		this.numWorkers = n;
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
}
