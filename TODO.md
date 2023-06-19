# Dumpty
**A utility for bulk migration of large on-premise databases to BigQuery** 

## TODO
- Deadline flag that causes the extract to abort if it has taken too long or going into the next day.
- .json sidecar file missing for empty tables
- Cleanup exception handling:
  - Exclude exceptions from Spark shutting down
  - More detail on Spark exceptions from JDBC 
 
## Would-be-awesome features:
- A benchmark mode which dumps a single table and adjusts threads / fetch row size to determine optimal settings
- Non-Spark extract for *very* tiny tables (eg. zc_ under 100 rows)
