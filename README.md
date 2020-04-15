### Approach
This project's main logic is in python-code/main.py file. 

The program first read both `Queries.txt` and `Records.txt` and create data frames for both data sets. 
It then used a explode function on the array column of the records data frame. Then 
uses the queries data frame as the base to do a left join to the records data frame to define if the ids in queries exist in record data frame. An inner join could be sufficient, but I wanted to double check so I did a left join and then drop the null values afterward. 

* dedup
It use `explode` and `collect_set` function to achieve the deduplicate data goal. 

### Testing
`python3 python-code/test_main.py`

### Run the project
if you don't have python3 installed
`brew install python3`
`pip install -r requirements.txt`
`python3 python-code/main.py`
if you run into exceptions like PySpark cannot run with different minor versions 
make sure you add below path to `PYSPARK_PYTHON` variable
`which python3`
`export PYSPARK_PYTHON=<python3 path>`
>>>>>>> jing scribner project(in python) submission
