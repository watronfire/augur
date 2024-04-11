"""
Subsample a dataset according to a provided config.

This augur functionality is _alpha_ and may change at any time.
It does not conform to the semver release standards used by Augur.
"""

from typing import Any, List, Optional
from .errors import AugurError
from os import path
import subprocess
import tempfile
import yaml

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
    optionals.add_argument('--random-seed', type=int, metavar="N",
        help="random number generator seed to allow reproducible subsampling (with same input data).")
    optionals.add_argument('--reference', metavar="FASTA", help="needed for priority calculation (but it shouldn't be!)")
    optionals.add_argument('--include', type=str, nargs="+", help="file(s) with list of strains to include regardless of priorities, subsampling, or absence of an entry in --sequences.")

    return parser


# TODO: Move these classes closer to code for augur filter?
PandasQuery = str
Date = str
File = str
EqualityFilterQuery = str


class Sample:
    query: Optional[PandasQuery]
    group_by: Optional[List[str]]
    min_date: Optional[Date]
    max_date: Optional[Date]
    exclude: Optional[List[File]]
    exclude_where: Optional[List[EqualityFilterQuery]]
    exclude_all: Optional[bool]
    include: Optional[List[File]]
    include_where: Optional[List[EqualityFilterQuery]]
    min_length: Optional[str]
    non_nucleotide: Optional[bool]

    weight: Optional[int]
    max_sequences: Optional[int]
    disable_probabilistic_sampling: Optional[bool]
    random_seed: Optional[int]

    priorities: Optional[Any]

    def __init__(self, name: str):
        # TODO: figure out where to put name. currently comes from config but is used for depends_on
        self.name = name

        # Initialize instance attributes.
        for option in self.__annotations__.keys():
            self.__setattr__(option, None)

    def add_options(self, **kwargs):
        for option, value in kwargs.items():
            if option not in self.__annotations__:
                raise AugurError(f'Option {option!r} not allowed.')
            # TODO: Check types
            self.__setattr__(option, value)


class FilterCall:

    # This is not optional but marking it as such for easier implementation.
    metadata: Optional[File]
    sequences: Optional[File]
    metadata_id_columns: Optional[List[str]]

    output_metadata: Optional[File]
    output_sequences: Optional[File]
    output_strains: Optional[File]

    sample: Sample

    def __init__(self, name, depends_on=None):
        self.name = name

        # Initialize instance attributes.
        for option in self.__annotations__.keys():
            self.__setattr__(option, None)

        if depends_on is None:
            depends_on = []
        self.depends_on = depends_on

        self.status = INCOMPLETE

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


        if self.sample.group_by is not None:
            args.append('--group-by')
            args.extend(self.sample.group_by)

        if self.sample.query is not None:
            args.extend(['--query', self.sample.query])

        if self.sample.min_date is not None:
            args.extend(['--min-date', self.sample.min_date])

        if self.sample.exclude_all is not None and self.sample.exclude_all:
            args.append('--exclude-all')

        if self.sample.include is not None:
            args.append('--include')
            args.extend(self.sample.include)

        # FIXME: Add other options


        if self.sample.weight is not None:
            args.extend(['--subsample-max-sequences', self.sample.max_sequences])

        if self.sample.disable_probabilistic_sampling:
            args.append('--no-probabilistic-sampling')

        if self.sample.random_seed is not None:
            args.extend(['--subsample-seed', self.sample.random_seed])


        if self.sample.priorities is not None:
            # TODO: implement proximity-based sampling
            pass

        return args

    def exec(self, dry_run=False):
        """
        Instead of running an `augur filter` command in a subprocess like we do
        here, a nicer way would be to refactor `augur filter` to expose
        functions which can be called here so that we can get a list of returned
        strains. Doing so would provide a HUGE speedup if we could index the
        sequences+metadata on disk a single time and then use that for all
        filtering calls which subsampling performs. This is analogous to having
        an in-memory database running in a separate process we can query - not
        as fast, but much easier implementation. Using subprocess does make
        parallelisation trivial, but the above speed up would be preferable.
        """
        deps = (f"depends on {', '.join(self.depends_on)}") if self.depends_on else "no dependencies"
        print(f"Sampling for {self.name!r} ({deps})")
        for option in self.sample.__annotations__:
            if self.sample.__getattribute__(option):
                print(f'\t{option}: {self.sample.__getattribute__(option)}')
        print()
        print(' '.join(str(arg) for arg in self.args()))
        print()

        if not dry_run:
            try:
                subprocess.run([str(arg) for arg in self.args()])
            except subprocess.CalledProcessError as e:
                raise AugurError(e)

        self.status = COMPLETE


class Config:
    size: int
    samples: Optional[List[Sample]]

    def __init__(self):
        # Initialize instance attributes.
        for option in self.__annotations__.keys():
            self.__setattr__(option, None)

        self.samples = []

    def load_yaml(self, filename):
        with open(filename) as fh:
            try:
                data = yaml.safe_load(fh)
            except yaml.YAMLError as e:
                print(e)
                raise AugurError(f"Error parsing subsampling scheme {filename}")
        # TODO XXX - write a schema and validate against this
        if 'samples' not in data:
            raise AugurError('Config must define a "samples" key')

        self.size = data['size']
        for sample_name, sample_dict in data['samples'].items():
            sample = Sample(sample_name)
            sample.add_options(**sample_dict)
            self.samples.append(sample)

    def add(self, new_sample: Sample):
        if any(new_sample.name == sample.name for sample in self.samples):
            raise Exception(f"ERROR: A sample with the name {new_sample.name} already exists.")

        self.samples.append(new_sample)

    def compute_max_sequences(self):
        total_weights = sum(sample.weight for sample in self.samples)

        for sample in self.samples:
            sample.max_sequences = int(self.size * (sample.weight / total_weights))

    def get_filter_calls(self, args, tmpdir):
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
        for sample in self.samples:

            call = FilterCall(sample.name)
            call.add_options(
                metadata=args.metadata,
                output_strains=path.join(tmpdir, f'{sample.name}.samples.txt'),
            )

            # Add sequences only if sequence filters are used.
            if ( sample.__getattribute__('min_length') or
                sample.__getattribute__('non_nucleotide')):
                call.add_options(
                    sequences=args.sequences,
                )

            # This works when YAML config keys are the same name as the
            # corresponding option class attribute.    
            call.sample = sample

            if args.random_seed is not None:
                call.sample.add_options(
                    random_seed=args.random_seed,
                )

            calls[sample.name] = call

        # Combine intermediate samples.
        include = [path.join(tmpdir, f"{sample.name}.samples.txt") for sample in self.samples]
        # TODO: Also read include from config file? I suspect workflows will need it
        # to be defined in workflow-level config for both filter and subsample step,
        # so maybe best to only allow from command line for now.
        if args.include:
            include.extend(args.include)

        output_call = FilterCall('output', [sample.name for sample in self.samples])
        output_call.add_options(
            metadata=args.metadata,
            sequences=args.sequences,
            output_metadata=args.output_metadata,
            output_sequences=args.output_sequences,
        )
        # FIXME: the separate object exposed here is unnecessary complexity
        output_call.sample = Sample('output')
        output_call.sample.add_options(
            exclude_all=True,
            include=include,
        )
        calls['output'] = output_call
        
        # TODO XXX prune any calls which are not themselves used in 'output' or as a dependency of another call

        # TODO XXX top-level includes

        return calls
    
    def run(self, args):
        self.compute_max_sequences()
        with tempfile.TemporaryDirectory() as tmpdir:
            calls = self.get_filter_calls(args, tmpdir)
            loop(calls, args.dry_run)


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
        # FIXME: exit early if one fails
        calls[name].exec(dry_run)

def run(args):
    config = Config()
    config.load_yaml(args.config)
    config.run(args)
