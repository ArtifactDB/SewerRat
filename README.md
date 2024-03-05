# Collecting random shit from a shared filesystem

[![Test and build](https://github.com/ArtifactDB/SewerRat/actions/workflows/build.yaml/badge.svg)](https://github.com/ArtifactDB/SewerRat/actions/workflows/build.yaml)
[![Publish version](https://github.com/ArtifactDB/SewerRat/actions/workflows/publish.yaml/badge.svg)](https://github.com/ArtifactDB/SewerRat/actions/workflows/publish.yaml)
[![Latest version](https://img.shields.io/github/v/tag/ArtifactDB/SewerRat?label=Version)](https://github.com/ArtifactDB/SewerRat/releases)

## Introduction

SewerRat retrieves user-supplied metadata from a shared filesystem and indexes them into a giant SQLite file.
This allows users to easily search for files of interest generated by other users, typically in high performance computing (HPC) clusters associated with the shared filesystem.
The aim is to promote discovery of analysis artifacts in an ergonomic manner -
we do not require uploads to an external service,
we do not impose schemas on the metadata format,
and we re-use the existing storage facilities on the HPC cluster.
SewerRat can be considered a much more relaxed version of the [Gobbler](https://github.com/ArtifactDB/gobbler) that federates the storage across users.

## Registering a directory

Any directory can be indexed as long as (i) the requesting user has write access to it and (ii) the account running the SewerRat service has read access to it.
To demonstrate, let's make a directory containing JSON-formatted metadata files.
Other files may be present, of course, but SewerRat only cares about the metadata.
Metadata files can be anywhere in this directory (including within subdirectories) and they can have any base name (here, `A.json` and `B.json`).

```shell
mkdir test 
echo '{ "title": "YAY", "description": "whee" }' > test/A.json
mkdir test/sub
echo '{ "authors": { "first": "Aaron", "last": "Lun" } }' > test/sub/A.json
echo '{ "foo": "bar", "gunk": [ "stuff", "blah" ] }' > test/sub/B.json
```

We define some small utility functions from [`scripts/functions.sh`](scripts/functions.sh) to perform the registration.
This is best placed in our `.bash_profile` so that we don't need to do this every time.
These functions are pretty simple and it should be trivial to define equivalents in any programming framework like R or Python -
see [below](#registration-in-more-detail) for more details.

```shell
export SEWER_RAT_URL=<INSERT URL HERE> # get this from your SewerRat admin.
source scripts/functions.sh
```

Once this is done, we can register our directory by calling the `registerSewerRat` function.
This requires a path to the directory along with a comma-separated list of file names to be indexed from that directory.

```shell
registerSewerRat test A.json,B.json
```

The registration process will walk recursively through the specified directory, indexing all files named `A.json` and `B.json`. 
We can then perform some complex searches on the contents of these files (see [below](#querying-the-index)).
There is no limit on the number of times that a directory can be registered, though every new registration will replace all previously-registered files from that directory.

SewerRat will periodically (by default, daily) update the index by examining all paths to metadata files in its registry.
If we modified one of our files, SewerRat will re-index that file; and if we deleted a file, SewerRat will remove it from the index.
This ensures that the information in the index reflects the organization on the filesystem.
Note that SewerRat will not index new files that were added to a directory after registration.
If we want these files in the index, we'll have to re-register the directory.

To remove files from the registry, we call the `deregisterSewerRat` function from [`scripts/functions.sh`](scripts/functions.sh).
This will remove all files in the registry that were previously registered from the specified directory.
Note that, if the directory no longer exists, the normalized absolute path should be supplied to `deregisterSewerRat`.
(Or we could just wait for the periodic updates to remove those paths automatically.)

```shell
deregisterSewerRat test
```

## Querying the index

We can query the SewerRat index to find files of interest based on the contents of the metadata, the user name of the file owner, the modification date, or any combination thereof.
This is done by making a POST request to the `/query` endpoint of the SewerRat API, where the request body contains the JSON-encoded search parameters:

```shell
curl -X POST -L ${SEWER_RAT_URL}/query \
    -H "Content-Type: application/json" \
    -d '{ "type": "text", "text": "Aaron" }' | jq
## {
##   "results": [
##     {
##       "path": "/Users/luna/Programming/GenomicsPlatform/SewerRat/scripts/test/sub/A.json",
##       "user": "luna",
##       "time": 1709320903,
##       "metadata": {
##         "authors": {
##           "first": "Aaron",
##           "last": "Lun"
##         }
##       }
##     }
##   ]
## }
```

The request body should be a "search clause", a JSON object with the `type` string property.
The nature of the search depends on the value of `type`:

- For `"text"`, SewerRat searches on the text (i.e., any string property) in the metadata file.
  The search clause should contain the following additional properties:
  - `text`, the search string.
    We use an adaptation of the [FTS5 Unicode61 tokenizer](https://www.sqlite.org/fts5.html#unicode61_tokenizer) to process all strings in the metadata files, 
    i.e., strings are split into tokens at any character that is not a Unicode letter/number or a dash.
    The same process is applied to the string in `text`.
    All tokens in `text` must match to a token in the metadata file in order for that file to be considered a match.
  - (optional) `field`, the name of the metadata property to be matched.
    Matches to tokens are only considered within the named property.
    Properties of nested objects can be specified via `.`-delimited names, e.g., `authors.first`.
    If `field` is not specified, matches are not restricted to any single property within a file.
  - (optional) `partial`, a boolean indicating whether to perform a partial match.
    If `true`, any SQL wildcards (`%` and `_`) in `text` will not be discarded during tokenization.
    Wildcard-containing tokens are then used for pattern matching to metadata-derived tokens.
    Defaults to `false`.
- For `"user"`, SewerRat searches on the user names of the file owners.
  The search clause should contain the `user` property, a string which contains the user name.
  A file is considered to be a match if the owning user is the same as that in `user`.
  Note that this only considered the most recent owner if the file was written by multiple people.
- For `"path"`, SewerRat searches on the path to each file. 
  The search clause should contain the `path` string property.
  A file is considered to be a match if its path contains `path` as a substring.
- For `"time"`, SewerRat searches on the latest modification time of each file.
  The search clause should contain the following additional properties:
  - `time`, an integer containing the Unix time.
    SewerRat searches for files that were modified before this time.
  - (optional) `after`, a boolean indicating whether to instead search for files that were created after `time`.
- For `"and"` and `"or"`, SewerRat searches on a combination of other filters.
  The search clause should contain the `children` property, which is an array of other search clauses.
  A file is only considered to be a match if it matches all (`"and"`) or any (`"or"`) of the individual clauses in `children`.
- For `"not"`, SewerRat negates the filter.
  The search clause should contain the `child` property, which contains the search clause to be negated.
  A file is only considered to be a match if it does not match the clause in `child`.

The API returns a request body that contains a JSON object with the following properties:

- `results`, an array of objects containing the matching metadata files, sorted by decreasing modification time.
   Each object has the following properties:
   - `path`, a string containing the path to the file.
   - `user`, the identity of the file owner.
   - `time`, the Unix time of most recent file modification.
   - `metadata`, the contents of the file.
- (optional) `next`, a string containing the endpoint to use for the next page of results.
  A request to this endpoint should use the exact same request body to correctly obtain the next page.
  If `next` is not present, callers may assume that all results have already been obtained.

Callers can control the number of results to return in each page by setting the `limit=` query parameter.
This should be a positive integer that is no greater than 100.

## Spinning up an instance

Clone this repository and build the binary.
This assumes that [Go version 1.20 or higher](https://go.dev/dl) is available.

```shell
git clone https://github.com/ArtifactDB/SewerRat
cd SewerRat
go build
```

And then execute the `SewerRat` binary to spin up an instance.
The `-db` flag specifies the location of the SQLite file (default to `index.sqlite3`)
and `-port` is the port we're listening to for requests (defaults to 8080).

```shell
./SewerRat -db DBPATH -port PORT
```

If a SQLite file at `DBPATH` already exists, it will be used directly, so a SewerRat instance can be easily restarted with the same database.

SewerRat will periodically (by default, daily) create a back-up of the index at `DBPATH.backup`.
This can be used to manually recover from problems with the SQLite database by copying the backup to `DBPATH` and restarting the SewerRat instance.

The [`html/`](html) subdirectory contains a minimal search page that queries a local SewerRat instance, also accessible from [GitHub Pages](https://artifactdb.github.com/SewerRat),
Developers can copy this page and change the `base_url` to point to their production instance.

## Registration in more detail

We previously glossed over the registration process by presenting users with the `registerSewerRat` function.
The process itself is slightly involved but it should still be simple enough to implement in any language.
First, we make a call to the `/register/start` endpoint with a request body that contains `path`, the absolute path to our directory that we want to register.

```shell
PWD=$(pwd)
curl -X POST -L ${SEWER_RAT_URL}/register/start \
    -H "Content-Type: application/json" \
    -d '{ "path": "'${PWD}'/test" }' | jq
## {
##   "code": ".sewer_HP0JOaQ14NBadaLGDPjOW712S2SIA_u-9yQH6AKbaQ8",
##   "status": "PENDING"
## }
```

On success, this returns a `PENDING` status with a verification code.
The caller is expected to verify that they have write access to the specified directory by creating the file inside the directory with the specified code.
Once this is done, we call the `/register/finish` endpoint with a request body that contains the same `path`.
The body may also contain `base`, an array of strings containing the names of the files to register within the directory -
if this is not provided, only files named `metadata.json` will be registered.

```shell
curl -X POST -L ${SEWER_RAT_URL}/register/finish \
    -H "Content-Type: application/json" \
    -d '{ "path": "'${PWD}'/test", "base": [ "A.json", "B.json" ] }' | jq
## {
##   "comments": [],
##   "status": "SUCCESS"
## }
```

On success, the files in the specified directory will be registered in the index.
Note that SewerRat will just skip problematic files, e.g., invalid JSON, insufficient permissions.
Any such problems are reported in the `comments` array rather than blocking the entire indexing process.

The deregistration process is identical if we replace the `/register/*` endpoints with `/deregister/*`.
The only exception is when the caller requests deregistration of a directory that does not exist.
In this case, `/deregister/start` may return a `SUCCESS` status instead of `PENDING`, after which `/deregister/finish` does not need to be called.

On error, the request body will contain an `ERROR` status with the `reason` string property containing the reason for the failure.
