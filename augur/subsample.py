"""
Subsample a dataset according to a provided config.

This augur functionality is _alpha_ and may change at any time.
It does not conform to the semver release standards used by Augur.
"""

from typing import List, Optional
from .errors import AugurError
from os import path
import subprocess
import tempfile

INCOMPLETE = 'incomplete'
COMPLETE = 'complete'

def register_parser(parent_subparsers):
    parser = parent_subparsers.add_parser("subsample", help=__doc__)
    parser.add_argument('--config', required=True, metavar="YAML", help="subsampling config file") # TODO: allow a string representation
    parser.add_argument('--metadata', required=True, metavar="TSV", help="sequence metadata")
    parser.add_argument('--sequences', required=True, metavar="FASTA", help="sequences in FASTA format") # TODO XXX VCF ?
    parser.add_argument('--output-metadata', required=True, metavar="TSV", help="output metadata")
    parser.add_argument('--output-sequences', required=True, metavar="FASTA", help="output sequences in FASTA format") # TODO XXX VCF ?

    optionals = parser.add_argument_group(
        title="Optional arguments",
    )
    optionals.add_argument('--dry-run', action="store_true")
    optionals.add_argument('--metadata-id-columns', metavar="NAME", nargs="+",
        help="names of possible metadata columns containing identifier information, ordered by priority. Only one ID column will be inferred.")
    optionals.add_argument('--subsample-seed', type=int, metavar="N",
        help="random number generator seed to allow reproducible subsampling (with same input data).")
    optionals.add_argument('--reference', metavar="FASTA", help="needed for priority calculation (but it shouldn't be!)")

    return parser

def parse_config(filename):
    import yaml
    with open(filename) as fh:
        try:
            data = yaml.safe_load(fh)
        except yaml.YAMLError as e:
            print(e)
            raise AugurError(f"Error parsing subsampling scheme {filename}")
    # TODO XXX - write a schema and validate against this
    if 'samples' not in data:
        raise AugurError('Config must define a "samples" key')
    return data

# TODO: Move these classes closer to code for augur filter?
PandasQuery = str
Date = str
File = str
EqualityFilterQuery = str


class Filter():

    # This is not optional but marking it as such for easier implementation.
    metadata: Optional[File]
    sequences: Optional[File]
    metadata_id_columns: Optional[List[str]]

    output_metadata: Optional[File]
    output_sequences: Optional[File]
    output_strains: Optional[File]

    query: Optional[PandasQuery]
    min_date: Optional[Date]
    max_date: Optional[Date]
    exclude: Optional[List[File]]
    exclude_where: Optional[List[EqualityFilterQuery]]
    exclude_all: Optional[bool]
    include: Optional[List[File]]
    include_where: Optional[List[EqualityFilterQuery]]

    subsample_seed: Optional[int]
    subsample_max_sequences: Optional[int]

    def __init__(self, name, depends_on=None):
        self.name = name

        if depends_on is None:
            depends_on = []
        self.depends_on = depends_on

        self.status = INCOMPLETE

        # Initialize instance attributes.
        for option in self.__annotations__.keys():
            self.__setattr__(option, None)

    def add_options(self, **kwargs):
        for option, value in kwargs.items():
            if option not in self.__annotations__:
                raise AugurError(f'Option {option!r} not allowed.')
            # TODO: Check types
            self.__setattr__(option, value)

    def args(self):
        args = ['augur', 'filter']

        if self.metadata is not None:
            args.extend(['--metadata', self.metadata])

        if self.sequences is not None:
            args.extend(['--sequences', self.sequences])

        if self.metadata_id_columns is not None:
            args.append('--metadata-id-columns')
            args.extend(self.metadata_id_columns)


        if self.output_metadata is not None:
            args.extend(['--output-metadata', self.output_metadata])

        if self.output_sequences is not None:
            args.extend(['--output-sequences', self.output_sequences])

        if self.output_strains is not None:
            args.extend(['--output-strains', self.output_strains])


        if self.query is not None:
            args.extend(['--query', self.query])

        if self.min_date is not None:
            args.extend(['--min-date', self.min_date])

        if self.exclude_all is not None and self.exclude_all:
            args.append('--exclude-all')

        if self.include is not None:
            args.append('--include')
            args.extend(self.include)

        # FIXME: Add other options


        if self.subsample_max_sequences is not None:
            args.extend(['--subsample-max-sequences', self.subsample_max_sequences])

        if self.subsample_seed is not None:
            args.extend(['--subsample-seed', self.subsample_seed])

        return args

    def exec(self, dry_run=False):
        """
        Instead of running an `augur filter` command in a subprocess like we do
        here, a nicer way would be to refactor `augur filter` to expose
        functions which can be called here so that we can get a list of returned
        strains. Doing so would provide a HUGE speedup if we could load the
        sequences+metadata into memory a single time and then use that in-memory
        data for all filtering calls which subsampling performs. This is
        analogous to having an in-memory database running in a separate process
        we can query - not as fast, but much easier implementation. Using
        subprocess does make parallelisation trivial, but the above speed up
        would be preferable.
        """
        deps = ("depends on " + ", ".join(self.depends_on)) if len(self.depends_on) else "(no dependencies)"
        print(f"RUNNING augur filter with name {self.name!r} {deps}")
        for option in self.__annotations__:
            if self.__getattribute__(option):
                print(f'\t{option}: {self.__getattribute__(option)}')
        print()

        if not dry_run:
            try:
                subprocess.run([str(arg) for arg in self.args()])
            except subprocess.CalledProcessError as e:
                raise AugurError(e)

        self.status = COMPLETE



def generate_calls(config, args, tmpdir):
    """
    Produce an (unordered) dictionary of calls to be made to accomplish the
    desired subsampling. Each call is either (i) a use of augur filter or (ii) a
    proximity calculation The names given to calls are config-defined, but there
    is guaranteed to be one call with the name "output".

    The separation between this function and the Filter (etc) classes is not
    quite right, but it's a WIP.
    """
    calls = {}

    # Add intermediate samples.
    for sample_name, sample_config in config['samples'].items():
        ## TODO XXX
        ## I designed this to have a 'include' parameter whereby the starting meta/seqs for this filter call could
        ## be the (joined) output of previous samples. To be implemented.
        ## Similarly, the "exclude" YAML parameter is also not implemented

        if 'include' in sample_config:
            raise AugurError("'include' subsampling functionality not yet implemented")

        call = Filter(sample_name)
        call.add_options(
            metadata=args.metadata,
            output_strains=path.join(tmpdir, f'{sample_name}.samples.txt'),
            # This works when YAML config keys are the same name as the
            # corresponding option class attribute.
            **sample_config['filter'],
        )

        if args.subsample_seed is not None:
            call.add_options(
                subsample_seed=args.subsample_seed,
            )

        # Add sequences only if sequence filters are used.
        if ('min_length' in sample_config['filter'] or
            'non_nucleotide' in sample_config['filter']):
            call.add_options(
                sequences=args.sequences,
            )

        calls[sample_name] = call

    # Combine intermediate samples.
    output_call = Filter('output', config['samples'].keys())
    output_call.add_options(
        metadata=args.metadata,
        sequences=args.sequences,
        exclude_all=True,
        include=[path.join(tmpdir, f"{sample_name}.samples.txt") for sample_name in config['samples']],
        output_metadata=args.output_metadata,
        output_sequences=args.output_sequences,
    )
    calls['output'] = output_call
    
    # TODO XXX prune any calls which are not themselves used in 'output' or as a dependency of another call

    return calls


def get_runnable_call(calls):
    """
    Return a call (i.e. a filter / proximity command) which can be run, either
    because it has no dependencies or because all it's dependencies have been
    computed
    """
    for name, call in calls.items():
        if call.status==COMPLETE:
            continue
        if len(call.depends_on)==0:
            return name
        if all([calls[name].status==COMPLETE for name in call.depends_on]):
            return name
    return None

def loop(calls, dry_run):
    """
    Execute the required calls in an approprate order (i.e. taking into account
    necessary dependencies). There are plenty of ways to do this, such as making
    a proper graph, using  parallelisation, etc etc This is a nice simple
    solution however.
    """
    while name:=get_runnable_call(calls):
        calls[name].exec(dry_run)

def run(args):
    config = parse_config(args.config)
    with tempfile.TemporaryDirectory() as tmpdir:
        calls = generate_calls(config, args, tmpdir)
        loop(calls, args.dry_run)
