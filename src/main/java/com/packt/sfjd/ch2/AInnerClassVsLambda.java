package com.packt.sfjd.ch2;

import java.io.File;
import java.io.FilenameFilter;

public class AInnerClassVsLambda {

	public static void main(String[] args) {
		
		File sourceDir= new File("/home/user");
		sourceDir.list(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				
				return name.endsWith("txt");
			}
		});
		
		
		sourceDir.list((dir,name)->name.endsWith("txt"));
		
		// Lexical scoping-wont work ---System.out.println(dir);
		
	}
	
}
