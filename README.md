# CSV2MongoDB
## Overview
This is a command-line utility to transform given csv files into a mongodb collection. It uses a simple heuristic to select grouping order.

## Instructions
* Compile with `ant compile`
* Run with `ant run -Dargs="time.csv"`

Sample files are provided in the csv/ folder. Be sure to include your csv file in the csv/ as well.

## Notes
Uses database 'mydb' and places csv in collection, 'mycoll'.