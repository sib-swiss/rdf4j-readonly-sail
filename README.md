# A WriteOnce ReadMany compressing quad datastore

This quad store is ment for datasets that are static and have only a small number of different predicates and graph names.

# Building

```bash
mvn package
```

# Loading

Make a directory where your data will reside.

Make a tab delimeted file with two columns.
 * the first is the path and file name
 * the second is the IRI of the graph

```bash
file_tsv_loadfile_graph=${a_tsv_with_two_columns_file_to_load_and_graph}
directory=${directory_where_sail_data_will_be_stored}
java -jar target/sail-readonly-0.0.3-SNAPSHOT-uber.jar
```

# Setting up your machine

During loading a lot of files are opened concurrently.
Limits to the maximum number of open files should be quite high.
At least 2 times the number of predicates multiplied by the number of expected datatypes.
The memory overhead during building of the index is per predicate * cpu.
Which is very noticable during e.g. wikidata indexing where there are many more predicates
than the UniProtKB case.

# Querying

# Some technical snippets.

## Sorted lists for values

The store has dictionaries for values like the vast majority of quad
stores. Difference is one dictionary for each distinct datatype plus one
for iris. A nuance of these dictionaries are that they are based around
sorted lists compressed and memory mapped and all keys are therefore
just index position values. These keys are valid for comparison
operators e.g. key 1 value "a" key 2 value "b" and key comparison
(Long.compare) would match SPARQL value comparison.

## Partioned triple tables, with graph filters

The quad table however is highly partitioned.  e.g. one table per
* if the subject is bnode or iri
* the unique predicate
* if the object is bnode or iri or specific datatype.

e.g.

```turtle
_:1 :pred_0 <http://example.org/iri> .
<http://example.org/iri> :pred_0 3 .
<http://example.org/iri> :pred_0 "lala" .
```

Will be stored in 3 distinct tables. Allowing us to a completely avoid
storing the predicates and the type of subject or object. For now stored
in separate files e.g.

```
./pred_0/bnode/iris
./pred_0/iri/datatype_xsd_int
./pred_0/iri/datatype_xsd_string
```

Which graphs a triple is in is encoded in bitset (roaring for
compression) and there might be multiple graph bitsets per table.
All graphs must be identified by an IRI.

## Inverted indexes using bitsets
Many values can be stored complet
ely inline in such a representation
and we also do inversion of the table. e.g. very valuable for when there
is a small set of distinct objects. e.g. for a with boolean values

We do
```
true -> [:iri1, :iri2, :iri4]
false -> [:iri1, :iri4, :iri8]
```

instead of
```
:iri1 true
:iri1 false
:iri2 true
:iri4 true
:iri4 false
:iri7 false
```

As all iri's string values are addressable by a 63 bit long value
(positive only). We an turn this into two bitsets. Which give very large
compression ratios and speed afterwards. Reduction to 2% of the input
data for quite a large number of datasets is possible. (2/3rds of the
predicate value combinations in UniProtKB are compressible this way)

## Join optimization candidates

Considering all triples are stored in subject, object order (or that
order is cheap to generate) we can also do a MergeJoin per default for
all patterns where a "subject variable" is joined on. BitSet joins might
in some cases also be possible.

# D-entailment/non valid literals

This store always stores a value in the smallest possible representation. And literals that do not match the datatype decleration throw an exception. e.g. `"S"^^xsd:int` is not accepted.

## Open work

There is still a lot of work to be done to make it as fast as possible
and validate that it really works as it is supposed too.
* Strings using less than nine UTF-8 characters are also inline value
candidates but this is not wired up yet.
* FSST compression for the IRI dictionary instead of LZ4.
* Cleanup experiments
* Document more :(
* Reduce temporary file size requirements during compression stage (7TB
for UniProtKB)


## Early results

Early results are encouraging. With for UniProtKB release we need 610 GB
of diskspace. 197 GB for the "quads" the other 413GB for the values.
e.g. roughly 16 bit per triple! This is better than the raw rdf/xml
compressed with xz --best :)

Loading time (for UniProtKB 2022_04) is currently 59 hours on a 128 core
machine (first generation EPYC). With 24 hours in preparsing the rdf/xml
and merge sorting the triples. Another 10 hours in sorting all IRIs, and
25 for converting all values in the triple tables down into their long
identifiers.

In principle the first and last step are highly parallelize and the last
step might be much faster when moving from lz4 to fsst[1] compression
for IRIs and long strings.
