Setup

  $ export AUGUR="${AUGUR:-$TESTDIR/../../../../bin/augur}"

Try building a tree with IQ-TREE.

  $ ${AUGUR} tree \
  >  --alignment "$TESTDIR/../data/aligned.fasta" \
  >  --method iqtree \
  >  --output tree_raw.nwk \
  >  --nthreads 1 > /dev/null
