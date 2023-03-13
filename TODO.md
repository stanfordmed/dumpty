# Dumpty
**A utility for bulk migration of large on-premise databases to BigQuery** 

## TODO
- Deadline flag that causes the extract to fail if it has taken too long or going into the next day.
- Get how long a dump took using the time for the first file and the last... but that might not work with the rename.. 
- Write .json sidecar file, especially for empty tables 
- Cleanup exception handling:
  - Exclude exceptions from Spark shutting down
  - More detail on Spark exceptions from JDBC 
 