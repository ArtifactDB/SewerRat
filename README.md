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

### Step-by-step

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

For convenience, we'll store the SewerRat API in an environment variable.

```shell
export SEWER_RAT_URL=<INSERT URL HERE> # get this from your SewerRat admin.
```

To start the registration process, we make a POST request to the `/register/start` endpoint.
This should have a JSON-encoded request body that contains the `path`, the absolute path to our directory that we want to register.

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
The caller is expected to verify that they have write access to the specified directory by creating a file with the same name as the verification code (i.e., `.sewer_XXX`) inside that directory.
Once this is done, we call the `/register/finish` endpoint with a request body that contains the same directory `path`.
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

On success, the files in the specified directory will be registered in the SQLite index.
We can then [search on the contents of these files](#querying-the-index) or [fetch the contents of any file](#fetching-file-contents) in the registered directory.
On error, the response usually has the `application-json` content type, where the body encodes a JSON object with an `ERROR` status and a `reason` string property explaining the reason for the failure.
Note that some error types (e.g., 404, 405) may instead return a `text/plain` content type with the reason directly in the response body.
In either case, the verification code file is no longer needed after a response is received and can be deleted from the directory to reduce clutter.

We provide some small utility functions from [`scripts/functions.sh`](scripts/functions.sh) to perform the registration from the command line.
The process should still be simple enough to implement equivalent functions in any language.

### Behind the scenes

Once verified in `/register/finish`, SewerRat will walk recursively through the specified directory.
It will identify all files with the specified `base` names (i.e., `A.json` and `B.json` in our example above), parsing them as JSON for indexing.
SewerRat will skip any problematic files that cannot be indexed due to, e.g., invalid JSON, insufficient permissions.
The causes of any failures are reported in the `comments` array in the HTTP response.

Symbolic links in the specified directory are treated differently depending on their target.
If the directory contains symbolic links to files, the contents of the target files can be indexed as long as the link has one of the `base` names.
All file information (e.g., modification time, owner) is taken from the link target, not the link itself;
SewerRat effectively treats the symbolic link as a proxy for the target file.
If the directory contains symbolic links to other directories, these will not be recursively traversed.

SewerRat will periodically update the index by inspecting all of its registered directories for new content.
If we added or modified a file with one of the registered names (e.g., `A.json`), SewerRat will (re-)index that file.
Similarly, if we deleted a file, SewerRat will remove it from the index.
This ensures that the information in the index reflects the directory contents on the filesystem.
Users can also manually update a directory by repeating the process above to re-index the directory's contents.

As an aside: updates and symbolic links can occasionally interact in strange ways.
Specifically, updates to the indexed information for symbolic links are based on the modification time of the link target.
One can imagine a pathological case where a symbolic link is changed to a different target with the same modification time as the previous target, which will not be captured by SewerRat.
Currently, this can only be resolved by deleting all affected symbolic links, re-registering the directory, and then restoring the links and re-registering again.

### Deregistering

To remove files from the index, we use the same procedure as above but replacing the `/register/*` endpoints with `/deregister/*`.
The only potential difference is when the caller requests deregistration of a directory that does not exist.
In this case, `/deregister/start` may return a `SUCCESS` status instead of `PENDING`, after which `/deregister/finish` does not need to be called.

## Querying the index

### Making the request

We can query the SewerRat index to find files of interest based on the contents of the metadata, the user name of the file owner, the modification date, or any combination thereof.
This is done by making a POST request to the `/query` endpoint of the SewerRat API, where the request body contains the JSON-encoded search parameters:

```shell
curl -X POST -L ${SEWER_RAT_URL}/query \
    -H "Content-Type: application/json" \
    -d '{ "type": "text", "text": "Aaron" }' | jq
## {
##   "results": [
##     {
##       "path": "/Users/luna/Programming/ArtifactDB/SewerRat/scripts/test/sub/A.json",
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

The request body should be a JSON-formatted "search clause", see [below](#defining-search-clauses) for details.
The response is a JSON object with the following properties:

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
This should be a positive integer, up to a maximum of 100.
Any value greater than 100 is ignored.

### Defining search clauses

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

### Human-readable syntax for text queries

For text searches, we support a more human-readable syntax for boolean operations in the query.
The search string below will look for all metadata documents that match `foo` or `bar` but not `whee`:

```
(foo OR bar) AND NOT whee
```

The `AND`, `OR` and `NOT` (note the all-caps!) are automatically translated to the corresponding search clauses.
This can be combined with parentheses to control precedence; otherwise, `AND` takes precedence over `OR`, and `NOT` takes precedence over both.
Note that any sequence of adjacent text terms are implicitly `AND`'d together, so the two expressions below are equivalent:

```
foo bar whee
foo AND bar AND whee
```

Users can prefix any sequence of text terms with the name of a metadata field, to only search for matches within that field of the metadata file.
For example:

```
(title: prostate cancer) AND (genome: GRCh38 OR genome: GRCm38)
```

Note that this does not extend to the `AND`, `OR` and `NOT` keywords,
e.g., `title:foo OR bar` will not limit the search for `bar` to the `title` field.

If a `%` wildcard is present in a search term, its local search clause is set to perform a partial search.

The human-friendly mode can be enabled by setting the `translate=true` query parameter in the request to the `/query` endpoint.
The structure of the request body is unchanged except that any `text` field is assumed to contain a search string and will be translated into the relevant search clause.

```shell
curl -X POST -L ${SEWER_RAT_URL}/query?translate=true \
    -H "Content-Type: application/json" \
    -d '{ "type": "text", "text": "Aaron OR stuff" }' | jq
## {
##   "results": [
##     {
##       "path": "/Users/luna/Programming/ArtifactDB/SewerRat/scripts/test/sub/B.json",
##       "user": "luna",
##       "time": 1711754321,
##       "metadata": {
##         "foo": "bar",
##         "gunk": [
##           "stuff",
##           "blah"
##         ]
##       }
##     },
##     {
##       "path": "/Users/luna/Programming/ArtifactDB/SewerRat/scripts/test/sub/A.json",
##       "user": "luna",
##       "time": 1711754321,
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

The [`html/`](html) subdirectory contains a minimal search page that queries a local SewerRat instance using this syntax.
Developers can copy this page and change the `base_url` to point to their production instance.

## Accessing registered directories

### Motivation

In general, users are expected to be operating on the same filesystem as the SewerRat API.
This makes it trivial to access the contents of directories registered with SewerRat, as we expect each registered directory to be world-readable.
For remote applications, the situation is more complicated as they are able to query the SewerRat index but cannot directly read from the filesystem.
This section describes some API endpoints that fill this gap for remote access.

### Listing directory contents 

We can list the contents of a directory by making a GET request to the `/list` endpoint of the SewerRat API,
where the URL-encoded path to the directory of interest is provided as a query parameter.

```shell
path=/Users/luna/Programming/ArtifactDB/SewerRat/scripts/test/
curl -L ${SEWER_RAT_URL}/list -G --data-urlencode "path=${path}" --data "recursive=true" | jq
## [
##   "A.json",
##   "hello.txt",
##   "sub/A.json",
##   "sub/B.json"
## ]
```

On success, the response contains a JSON-encoded array of strings, each of which is a relative path in the directory at `path`.
The `recursive=` parameter specifies whether a recursive listing should be performed.
If true, all paths refer to files; otherwise, the names of directories may be returned and will be suffixed with `/`.
All symbolic links are reported as files in the response. 
Symbolic links to directories will not be recursively traversed, even if `recursive=true`.

On error, the exact response may either be `text/plain` content containing the error message directly,
or `application/json` content encoding a JSON object with the `reason` for the error.
If the path does not exist in the index, a standard 404 error is returned.

### Fetching file contents

We can obtain the contents for a path inside any registered directory by making a GET request to the `/retrieve/file` endpoint of the SewerRat API,
where the URL-encoded path of interest is provided as a query parameter.
This is not limited to the registered metadata files - any file inside a registered directory can be extracted in this manner.

```shell
# Mocking up a non-metadata file.
echo "HELLO" > test/hello.txt

# Fetching it:
path=/Users/luna/Programming/ArtifactDB/SewerRat/scripts/test/hello.txt
curl -L ${SEWER_RAT_URL}/retrieve/file -G --data-urlencode "path=${path}"
## HELLO
```

On success, the contents of the target file are returned with a content type guessed from its name.
If `path` is a symbolic link to a file, the contents of the target file will be returned by this endpoint.
However, if a registered directory contains a symbolic link to a directory, the contents of the target directory cannot be retrieved if `path` needs to traverse that symbolic link.
This is consistent with the registration policy whereby symbolic links to directories are not recursively traversed during indexing.

On error, the exact response may either be `text/plain` content containing the error message directly,
or `application/json` content encoding a JSON object with the `reason` for the error.
If the path does not exist in the index, a standard 404 error is returned.

### Fetching metadata

For the special case of a metadata file, we can alternatively obtain its contents by making a GET request to the `/retrieve/metadata` endpoint of the SewerRat API,
where the URL-encoded path of interest is provided as a query parameter.

```shell
path=/Users/luna/Programming/ArtifactDB/SewerRat/scripts/test/A.json
curl -L ${SEWER_RAT_URL}/retrieve/metadata -G --data-urlencode "path=${path}" | jq
## {
##   "path": "/Users/luna/Programming/ArtifactDB/SewerRat/scripts/test/A.json",
##   "user": "luna",
##   "time": 1711754321,
##   "metadata": {
##     "title": "YAY",
##     "description": "whee"
##   }
## }
```

On success, this returns an object containing:

- `path`, a string containing the path to the file.
- `user`, the identity of the file owner.
- `time`, the Unix time of most recent file modification.
- `metadata`, the contents of the file.

If we do not actually need the metadata (e.g., we just want to check if the file exists),
we can skip it by setting the `metadata=false` URL query parameter in our request.

On error, the exact response may either be `text/plain` content containing the error message directly,
or `application/json` content encoding a JSON object with the `reason` for the error.
If the path does not exist in the index, a standard 404 error is returned.

## Administration

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

SewerRat will periodically create a back-up of the index at `DBPATH.backup`.
This can be used to manually recover from problems with the SQLite database by copying the backup to `DBPATH` and restarting the SewerRat instance.

Additional arguments can be passed to `./SewerRat` to control its behavior (check out `./SewerRat -h` for details):

- `-backup` controls the frequency of back-up creation.
  This defaults to 24 hours.
- `-update` controls the frequency of index updates.
  This defaults to 24 hours.
- `-session` specifies the lifetime of a registration sesssion 
  (i.e., the maximum time between starting and finishing the registration, see below).
  This defaults to 10 minutes.
- `-prefix` adds an extra prefix to all endpoints, e.g., to disambiguate between versions.
  For example, a prefix of `api/v2` would change the list endpoint to `/api/v2/list`.
  This defaults to an empty string, i.e., no prefix.

🚨🚨🚨 **IMPORTANT!** 🚨🚨🚨
It is assumed that SewerRat runs under a service account with no access to credentials or other sensitive information.
This is because users can, in their registered directories, craft symlinks to arbitrary locations that will be followed by SewerRat.
Any file path that can be accessed by the service account should be assumed to be public when the SewerRat API is active.
