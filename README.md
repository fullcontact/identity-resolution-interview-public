# Identity Resolution Coding Exercise

Welcome to the take-home coding exercise
for the FullContact Identity Resolution team hiring pipeline. 
This is intended to be a short, straightforward exercise
to help us understand your skill level
with a few of the technologies and tasks we most frequently work with.

##### Expectations:
* **Duration**: About 1 to 4 hours
* **Subjects**: Scala, Apache Spark
* **Resources**: Yourself, existing online documentation

##### Assignment Summary

* Read **Records.txt** and **Queries.txt**
* Utilize spark to process them to produce
    * **Output1**: IDs from **Queries.txt** with records they appear in, separated by a colon
    * **Output2**: IDs from **Queries.txt** with the deduplicated union of records they appear in, separated by a colon
* Include documentation on how to run your solution from a terminal at the root of the project
* Include a test suite

## The Scaffolding

This repository includes a basic gradle buildscript,
with dependencies on scala and spark already declared.

The buildscript also includes a scalatest-based test scaffold, invokable with `./gradlew test`.
Whether you use the provided scaffold or not,
you should include a test suite invokable with `./gradlew test`.

The `application` plugin has been applied to the project so that
`./gradlew run` will invoke the `main` method of `com.fullcontact.interview.RecordFinder`,
where you can write your solution.

You may modify the buildscript and codebase as you see fit,
(_e.g._, add dependencies, add classes, move/modify/duplicate `main` method, etc.).
However, we ask that you document how to run your submission
from a terminal session at the root of the project,
whether that is by `./gradlew run` or otherwise.

## The Scenario

In the course of our work,
we receive multiple datasets of observed, but incomplete, contact records.
_e.g._, we might have an input record of
`(bart@fullcontact.com, linkedin:lorangb, +2025551234)`,
indicating that those three pieces of contact information represent the same person.
If we later have a record of
`(bart@fullcontact.com, +2025557890)`
in addition to the first, we want to retrieve _both_ records
when searching for `bart@fullcontact.com`.

Your task is to replicate this behavior with
a small dataset and simplified matching logic.

You are provided two datasets of randomized IDs:

| Records.txt (~50k lines) |
|-------------------------------------------|
| `ZVREMGG HLLCCNX`                         |
| `BROOFJY UBXZQKD ATCZUTP`                 |
| `WONIWXW NPXNWKZ RHUJKEY QULQKGC LXZFLFQ` |
| `CFTGDGD QTBSKQW`                         |
| `MEKQMTV LOATEQG HEVOKKP YQLZEAY NPRMWRX` |
|                    ...                    |

| Queries.txt (~100k lines) |
|-------------|
|  `JKXBHJJ`  |
|  `TSZRMKP`  |
|  `HLLCCNX`  |
|  `MUXCODO`  |
|  `QIAXOON`  |
|     ...     |

The random identifiers stand in for pieces of contact information. Identifiers
are 7-character strings using the uppercase ASCII alphabet.

**Records.txt** is a file where each line is a space-separated list of IDs
indicating that those IDs were observed together in some dataset.

**Queries.txt** is a file where each line is an ID which we want to find all
associated records for.

**Given these two inputs**, create a submission
which utilizes spark in local mode to produce
two deliverable outputs:

| Output1.txt/part-00000                    |
|-------------------------------------------|
|` HLLCCNX:ZVREMGG HLLCCNX`                 |
|` LMLHENN:LMLHENN ZETNBFX`                 |
|` OOFDUJC:HAWCESV OOFDUJC MNVQEIN TJWEWHT` |
|` FSNMWAF:DJBEEPL FSNMWAF`                 |
|` FSNMWAF:FSNMWAF MLEAGKE`                 |
|                    ...                    |

| Output2.txt/part-00000                    |
|-------------------------------------------|
|` HLLCCNX:ZVREMGG HLLCCNX`                 |
|` LMLHENN:LMLHENN ZETNBFX`                 |
|` OOFDUJC:HAWCESV OOFDUJC MNVQEIN TJWEWHT` |
|` FSNMWAF:DJBEEPL FSNMWAF MLEAGKE`         |
|                    ...                    |

**Output1.txt** is a text file where each row is
an ID from **Queries.txt**,
followed by the `:` character,
followed by a row from **Records.txt** where the query ID appeared.
Query IDs _may_ appear multiple times on the left side of this output.

**Output2.txt** is a text file where each row is
an ID from **Queries.txt**,
followed by the `:` character,
followed by the deduplicated set of the union of
all IDs from rows from **Records.txt** where the query ID appeared.
Query IDs _must not_ appear multiple times on the left side of this output.

ID `FSNMWAF` demonstrates the difference between the two outputs in the samples above.


