# Operation Instructions
For this exercise, I decided to use Spark's GraphX API. By treating IDs as
 vertices and records as edges, we can easily record associations between 
 unique IDs. This approach has the added benefit of being extensible as 
 requirements change, such as looking for more complex association properties
 or adding edge weights.

## Basic Usage
The project can be run using the default inputs by simply invoking the gradle 
run task, `./gradlew run`. Output directories will be written in the project 
base directory and will contain a single text file each.

## Running Tests
All tests can be invoked via the gradle test task, `./gradlew test`. 
Tests are at the unit level and cover the most of the application.

