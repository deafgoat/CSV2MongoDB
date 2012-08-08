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

// Mongo
import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

// Java
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.net.UnknownHostException;
import java.io.IOException;

/** 
 * CSVIterator.java
 * Purpose: This script parses CSV files and stores the entries in a mongodb collection.
 * 
 */

public class CSVStore {
	private CSVIterator iterator;
	private Mongo m;
	private DB db;
	private DBCollection coll;

    public CSVStore(String filename) throws IOException {
    	this.iterator = new CSVIterator(filename);
    	setDatabaseParameters();
    }

    private void setDatabaseParameters() {
    	try {
    		this.m = new Mongo( "localhost" , 27017 );
    	}
    	catch (UnknownHostException e) {
    		System.out.println("Could not connect to localhost");
	    	System.exit(1);
    	}
		this.db = m.getDB( "mydb" );
		this.coll = db.getCollection("mycoll");
    }

    public static void main(String argz[]) throws IOException {
    	if (argz.length != 1) {
    		System.out.print("Please enter csv file name (under csv/): ");
    		System.exit(1);
    	}
    	CSVStore ce = new CSVStore(argz[0]);
    	Map<String,ArrayList<String>> groupings = ce.determineGrouping();
		ce.insertObjects(groupings);
	}

	private HashMap<String,Integer> getHeaderMappings() {
		HashMap<String,Integer> mapping = new HashMap<String,Integer> ();
		String [] fields = iterator.getFields();
		for (int i = 0; i < fields.length; i++)
			mapping.put(fields[i].trim(), i);
		return mapping;
	}

    private void insertObjects(Map<String,ArrayList<String>> collectionGroupings) throws IOException {
    	HashMap<String,Integer> mappings = this.getHeaderMappings();
    	Double numVal;
    	String strVal;

        while (iterator.hasNext()) {
        	String[] record = iterator.next();
			BasicDBObject doc = new BasicDBObject();
			for (String key : collectionGroupings.keySet()) {
				ArrayList<String> fieldSet = collectionGroupings.get( key );
				if (fieldSet.size() == 1) {
					int field = mappings.get(fieldSet.get(0));
					if (field >= record.length) {
						System.out.println("Encountered blank line in file on line: " + this.iterator.getLineNumber());
			    		strVal = " ";
					}
					else
						strVal = record[field].trim();
					try {
						numVal = Double.parseDouble(strVal);
						doc.put(fieldSet.get(0).replace(" ","-"), numVal);
					} 
					catch (NumberFormatException nfe) {
						doc.put(fieldSet.get(0).replace(" ","-"), strVal);
					}
				}

				else {
					// Insert each sub document based on grouping

					BasicDBObject subDoc = new BasicDBObject();
					for (int i = 0; i < fieldSet.size(); i++) {
						int field = mappings.get(fieldSet.get(0));
						if (field >= record.length) {
				    		System.out.println("Encountered blank line in file: " + this.iterator.getLineNumber());
				    		strVal = " ";
						}
						else
							strVal = record[field].trim();
						try {
							numVal = Double.parseDouble(strVal);
							subDoc.put(fieldSet.get(i).replace(" ","-"), numVal);
						} 
						catch (NumberFormatException nfe) {
							subDoc.put(fieldSet.get(i).replace(" ","-"), strVal);
						}
					}
					doc.put(key, subDoc);
				}
			}
			coll.insert(doc);
		}
		this.iterator.close();
    }
    
    /** 
     * Choose grouping scheme.
     * 
     * @return Groupign scheme
     */
    private Map<String,ArrayList<String>> determineGrouping() {
    	String[] fields = iterator.getFields();
    	/* For starters, we'll check for commonalities in field names and 
    	 * use that as an aggregation basis. Naive, but serves the purpose.
    	 */
		Map<String,Integer> allWords = new HashMap<String,Integer>();
		Map<Integer,String> mappings = new HashMap<Integer,String>();

		// Fill word map
		for (String s : fields) {
			for (String n : s.split(" ")) {
				if (!n.isEmpty()) {
					if (allWords.get(n) != null) {
						int cur = allWords.get(n);
						allWords.put(n, cur+1);
					}
					else
						allWords.put(n, 1);
				}
			}
		}

		int size = fields.length;
		int mapIndex = 0;
		int index = 0;
		String curWord = "";

		// Perform grouping
		while (index < size) {
			String curStr[] = fields[index].split(" ");
			int len = curStr.length;
			int i = 0;
			while (i < len) {
				if (allWords.get(curStr[i]) != null)
					mappings.put(index, curStr[i]);
				i += 1;
			}
			index += 1;
		}

		// Create grouping echelon
		Map<String,ArrayList<String>> retMap = new HashMap<String,ArrayList<String>>();
		for (int i = 0; i < size; i++) {
			if (!retMap.containsKey(mappings.get(i)))
				retMap.put(mappings.get(i).trim(), new ArrayList<String> ());
			retMap.get(mappings.get(i).trim()).add(fields[i].trim());
		}
		return retMap;
    }
}