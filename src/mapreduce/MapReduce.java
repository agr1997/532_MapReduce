package mapreduce;

import java.lang.reflect.InvocationTargetException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;

// The actual class the initiates the map reduce job
public class MapReduce {
	private Class mapperClass;
	private Class reducerClass;
	private MapReduceSpecification spec;
	
	public MapReduce(MapReduceSpecification spec, Class mapperClass, Class reducerClass) {
		this.mapperClass = mapperClass;
		this.reducerClass = reducerClass;
		this.spec = spec;
	}
	
	public int execute() throws IOException {
		//this.mapper map = new this.mapper();
		Mapper mapper = null;

		
		// This is only an approximation: see https://stackoverflow.com/questions/12807797/java-get-available-memory
		long freeMemory = Runtime.getRuntime().freeMemory();
				
		// Set up the byte offsets for the map workers (commented out since apparently workers can do this)
		long fileSize = Files.size(spec.getInput());
		
		// Create dirs for intermediate files
		Path outputDir = spec.getOutput();
		Path intermediateDir = outputDir.resolve("intermediate");
		File dir = new File(intermediateDir.toString());
		if(!dir.mkdir()) {
			System.out.println("Error: Could not make intermediate file directory.");
			throw new IOException();
		}	
		
		// Set up our N mappers do their thing!
		Mapper[] mappers = new Mapper[spec.getN()];
		for(int i=0; i < spec.getN(); i++) {
			try {
				// "workers can perform both byte and row splitting parallel without 
				// any coordination with each other once they know the name and size of the input file"
				// (and I would add, they need to know their worker number)
				mappers[i] = (Mapper) this.mapperClass.getConstructor().newInstance();
				mappers[i].setN(spec.getN()); // Give mapper the # of workers
				mappers[i].setWorkerNumber(i); // Give mapper its worker number
				mappers[i].setInputSize(fileSize); // Give mapper the file size
				mappers[i].setIntermediate(intermediateDir);
				mappers[i].setInputFilePath(spec.getInput());
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
					| NoSuchMethodException | SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// Tell each mapper to execute its map job (will not need this in multiprocessing version)
		for(int i=0; i < spec.getN(); i++) {
			mappers[i].execute();
		}
		
		// TODO right now, map() receives a reader over the entire input file.
		// Need to either find a way to make a reader over just a particular byte range,
		// Note: Each worker should be able to find the starting point for its work independently.
		
		
		//Reducer
		Path finalDir = outputDir.resolve("final");
		File dir2 = new File(finalDir.toString());
		if(!dir2.mkdir()) {
			System.out.println("Error: Could not make final file directory.");
			throw new IOException();
		}	
		
		// Set up our N reducers to do their thing!
		Reducer[] reducers = new Reducer[spec.getN()];
		for(int i=0; i < spec.getN(); i++) {
			try {
				reducers[i] = (Reducer) this.reducerClass.getConstructor().newInstance();
				reducers[i].setN(spec.getN()); // Give reducer the # of workers
				reducers[i].setWorkerNumber(i); // Give reducer its worker number
				reducers[i].setIntermediate(intermediateDir);
				reducers[i].setFinal(finalDir);
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
					| NoSuchMethodException | SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// Tell each reducer to execute its map job (will not need this in multiprocessing version)
		for(int i=0; i < spec.getN(); i++) {
			reducers[i].execute();
		}
		
		return 0;
	}

}