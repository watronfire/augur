Setup

  $ pushd "$TESTDIR" > /dev/null
  $ export AUGUR="${AUGUR:-../../../../bin/augur}"

Testing metadata output for the curate command.
Running the `passthru` subcommand since it does not do any data transformations.

Create NDJSON file for testing.

  $ cat >$TMP/records.ndjson <<~~
  > {"strain": "sequence_A", "country": "USA", "date": "2020-10-01"}
  > {"strain": "sequence_B", "country": "USA", "date": "2020-10-02"}
  > {"strain": "sequence_C", "country": "USA", "date": "2020-10-03"}
  > ~~
Test metadata output TSV
  $ cat $TMP/records.ndjson \
  >   | ${AUGUR} curate passthru \
  >     --output-metadata $TMP/metadata.tsv
  $ cat $TMP/metadata.tsv
  strain\tcountry\tdate (esc)
  sequence_A\tUSA\t2020-10-01 (esc)
  sequence_B\tUSA\t2020-10-02 (esc)
  sequence_C\tUSA\t2020-10-03 (esc)
Test metadata output TSV to stdout
  $ cat $TMP/records.ndjson \
  >   | ${AUGUR} curate passthru \
  >     --output-metadata -
  strain\tcountry\tdate (esc)
  sequence_A\tUSA\t2020-10-01 (esc)
  sequence_B\tUSA\t2020-10-02 (esc)
  sequence_C\tUSA\t2020-10-03 (esc)
