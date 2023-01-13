Setup

  $ pushd "$TESTDIR" > /dev/null
  $ source _setup.sh

Metadata with ambiguous days on all strains should error when grouping by week.

  $ cat >$TMP/metadata.tsv <<~~
  > strain	date
  > SEQ1	2000-01-XX
  > SEQ2	2000-02-XX
  > SEQ3	2000-03-XX
  > SEQ4	2000-04-XX
  > ~~

  $ ${AUGUR} filter \
  >   --metadata $TMP/metadata.tsv \
  >   --group-by week \
  >   --sequences-per-group 1 \
  >   --subsample-seed 0 \
  >   --output-metadata $TMP/metadata-filtered.tsv \
  >   --output-log $TMP/filtered_log.tsv
  ERROR: All samples have been dropped! Check filter rules and metadata file format.
  4 strains were dropped during filtering
  \t4 were dropped during grouping due to ambiguous day information (esc)
  \t0 of these were dropped because of subsampling criteria (esc)
  [1]
  $ cat $TMP/filtered_log.tsv | grep "skip_group_by_with_ambiguous_day" | wc -l
  \s*4 (re)
  $ cat $TMP/metadata-filtered.tsv
  strain	date
  $ rm -f $TMP/filtered_log.tsv $TMP/metadata-filtered.tsv

Metadata with ambiguous months on all strains should error when grouping by month.

  $ cat >$TMP/metadata.tsv <<~~
  > strain	date
  > SEQ1	2000-XX-XX
  > SEQ2	2000-XX-XX
  > SEQ3	2000-XX-XX
  > SEQ4	2000-XX-XX
  > ~~

  $ ${AUGUR} filter \
  >   --metadata $TMP/metadata.tsv \
  >   --group-by month \
  >   --sequences-per-group 1 \
  >   --subsample-seed 0 \
  >   --output-metadata $TMP/metadata-filtered.tsv \
  >   --output-log $TMP/filtered_log.tsv
  ERROR: All samples have been dropped! Check filter rules and metadata file format.
  4 strains were dropped during filtering
  \t4 were dropped during grouping due to ambiguous month information (esc)
  \t0 of these were dropped because of subsampling criteria (esc)
  [1]
  $ cat $TMP/filtered_log.tsv | grep "skip_group_by_with_ambiguous_month" | wc -l
  \s*4 (re)
  $ cat $TMP/metadata-filtered.tsv
  strain	date
  $ rm -f $TMP/filtered_log.tsv $TMP/metadata-filtered.tsv

Metadata with ambiguous years on all strains should error when grouping by year.

  $ cat >$TMP/metadata.tsv <<~~
  > strain	date
  > SEQ1	XXXX-XX-XX
  > SEQ2	XXXX-XX-XX
  > SEQ3	XXXX-XX-XX
  > SEQ4	XXXX-XX-XX
  > ~~

  $ ${AUGUR} filter \
  >   --metadata $TMP/metadata.tsv \
  >   --group-by year \
  >   --sequences-per-group 1 \
  >   --subsample-seed 0 \
  >   --output-metadata $TMP/metadata-filtered.tsv \
  >   --output-log $TMP/filtered_log.tsv
  ERROR: All samples have been dropped! Check filter rules and metadata file format.
  4 strains were dropped during filtering
  \t4 were dropped during grouping due to ambiguous year information (esc)
  \t0 of these were dropped because of subsampling criteria (esc)
  [1]
  $ cat $TMP/filtered_log.tsv | grep "skip_group_by_with_ambiguous_year" | wc -l
  \s*4 (re)
  $ cat $TMP/metadata-filtered.tsv
  strain	date
  $ rm -f $TMP/filtered_log.tsv $TMP/metadata-filtered.tsv
