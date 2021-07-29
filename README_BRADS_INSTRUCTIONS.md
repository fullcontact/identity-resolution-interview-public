# Instructions and Short Discussion from Brad

This is a _super_ brief discussion of how you can test and run my code for taking the `Records.txt` and `Queries.txt` files and transforming them into the `Output1.txt` and `Output2.txt` partitioned test files as requested in the original `README.md` at the root of this repo.

## Instructions

I...really didn't change _anything_ from the default behavior laid out by the assignment. The `Queries.txt` and `Records.txt` 'seed' files are in the same (root of repo) location as they were when I got the assignment, and my `main()` assumes we _always_ want to source from these two exact file names and paths.

To execute my program (again, as set up in the assignment scaffolding), you _should_ be able to simply invoke `./gradlew run` from e.g. a Terminal window in IntelliJ (which is where I was developing this).

I've added two very small `QueriesTest.txt` and `RecordsTest.txt` test files _also_ at the root of the repo. Once more, you should be able to run all the tests I wrote (in `test/scala/com/fullcontact/interview`) by invoking `./gradlew test` at the root of the repo.

## (Short!) Discussion

So the basic idea here is: if a client comes in asking for duped or deduped sets of graph neighbors for an array of IDs, that could take a _long_ time if we simply searched through _every_ `Records.txt` array for _every_ query ID.

What I thought'd be more fitting is if we 'reversed' the `Records.txt` mapping somewhat. For Output 1, we can simply explode the Records ID array while keeping original indices of which 'row' the IDs came from. An inner join between Query ID and ID within this flattened/exploded structure can then (fairly) quickly link us back (through another join/lookup on 'row number') to the partial list of neighbors to output from Records.

For Output 2, if we treat the groups/arrays of strings in Records as a `Set` that is _keyed_ to the IDs present in that set, we can `ReduceByKey` pretty effectively with logical OR (`|`) to get what we need.

## Acknowledgements

So I'm definitely not the strongest Scala/Spark programmer, but I had fun hacking this together! I _tried_ to do this in a way that avoided driver node collections whenever possible, but I may still be running more shuffles than I need to.

It would be fun to discuss how to optimize this in a distributed setting!
