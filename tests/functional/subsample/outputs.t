Setup

  $ AUGUR="${AUGUR:-$TESTDIR/../../../bin/augur}"

Write data files.

  $ cat >metadata.tsv <<~~
  > strain	date	region
  > SEQ1	2021-01-01	A
  > SEQ2	2021-01-02	A
  > SEQ3	2021-01-01	B
  > SEQ4	2021-01-02	B
  > SEQ5	2021-02-02	B
  > ~~

  $ cat >sequences.fasta <<~~
  > >SEQ1
  > AAA
  > >SEQ2
  > CCC
  > >SEQ3
  > TTT
  > >SEQ4
  > GGG
  > >SEQ5
  > AAC
  > ~~

Subsampling configuration:

  $ cat >config.yaml <<~~
  > samples:
  >   focal:
  >     query: region=='A'
  >     max_sequences: 1
  >   context:
  >     query: region=='B'
  >     max_sequences: 2
  > ~~

Apply subsampling.

FIXME: Use better regex to ignore temp file paths but still show other relevant info.

  $ ${AUGUR} subsample \
  >  --metadata metadata.tsv \
  >  --sequences sequences.fasta \
  >  --config config.yaml \
  >  --output-metadata subsampled-metadata.tsv \
  >  --output-sequences subsampled-sequences.fasta \
  >  --random-seed 0
  4 strains were dropped during filtering
  	3 were filtered out by the query: "region=='A'"
  	1 was dropped because of subsampling criteria
  1 strain passed all filters
  3 strains were dropped during filtering
  	2 were filtered out by the query: "region=='B'"
  	1 was dropped because of subsampling criteria
  2 strains passed all filters
  2 strains were dropped during filtering
  	5 were dropped by `--exclude-all`
  .* (re)
  .* (re)
  3 strains passed all filters
  Sampling for 'focal' (no dependencies)
  	metadata: metadata.tsv
  .* (re)
  	query: region=='A'
  	max_sequences: 1
  
  augur filter .* (re)
  
  .* (re)
  \tmetadata: metadata.tsv (esc)
  .* (re)
  \tquery: region=='B' (esc)
  \tmax_sequences: 2 (esc)
  
  augur filter .* (re)
  
  Sampling for 'output' (depends on focal, context)
  \tmetadata: metadata.tsv (esc)
  \tsequences: sequences.fasta (esc)
  \toutput_metadata: subsampled-metadata.tsv (esc)
  .* (re)
  \texclude_all: True (esc)
  .* (re)
  
  augur filter .* (re)
  

  $ cat subsampled-metadata.tsv
  strain	date	region
  SEQ1	2021-01-01	A
  SEQ3	2021-01-01	B
  SEQ4	2021-01-02	B

  $ cat subsampled-sequences.fasta
  >SEQ1
  AAA
  >SEQ3
  TTT
  >SEQ4
  GGG
