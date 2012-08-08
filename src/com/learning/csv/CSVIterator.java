/**
 * Copyright 2012, Wisdom Omuya.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.learning.csv;

// Java
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.NoSuchElementException;

/** 
 * CSVIterator.java
 * Purpose: A class for iterating through .csv files
 * 
 */

public class CSVIterator {
    private BufferedReader fileHandle;
    private String instance;
    private String filename;
    private String [] fields;
    private int current;
    
    public CSVIterator(String filename) throws IOException {
    	setInstanceVariables(filename);
    }

    private void setInstanceVariables(String filename) throws IOException {
    	this.filename = filename;
    	this.fileHandle = openFile(filename);
        this.current = 1;

    	this.fields =  fileHandle.readLine().split(",");
    	this.instance = fileHandle.readLine();
    }

    private BufferedReader openFile(String filename) throws IOException {
        return new BufferedReader(new FileReader(filename));
    }

    public String[] getFields() {
    	return this.fields;
    }

    private void readNextLine() throws IOException {
        this.instance = this.fileHandle.readLine();
    	setLineNumber();
    }

    private void setLineNumber() {
    	this.current += 1;
    }
    
    public boolean hasNext() {
        return (this.instance != null);
    }

    public int getLineNumber() {
        return this.current;
    }

    public String[] next() throws IOException {
        String[] row = this.instance.split(",");
        if (this.hasNext())
            this.readNextLine();
        return row;
    }
    
    public void close() throws IOException {
        fileHandle.close();
    }
}