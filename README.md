## DESCRIPTION

* A Serbian real estate processing pipeline. 
* The data is scraped from nekretine.rs, after that it gets uploaded to S3 with MinIo.
* Then it gets put into a Delta lake table using Spark.
* The data is going to be used for visualizations and machine learning projects.
