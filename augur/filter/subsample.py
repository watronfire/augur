import heapq
import itertools
import sys
import uuid
import numpy as np
import pandas as pd
from typing import Collection

from augur.dates import get_iso_year_week
from augur.errors import AugurError
from . import GROUP_BY_GENERATED_COLUMNS


def get_groups_for_subsampling(strains, metadata, group_by=None):
    """Return a list of groups for each given strain based on the corresponding
    metadata and group by column.

    Parameters
    ----------
    strains : list
        A list of strains to get groups for.
    metadata : pandas.DataFrame
        Metadata to inspect for the given strains.
    group_by : list
        A list of metadata (or calculated) columns to group records by.

    Returns
    -------
    dict :
        A mapping of strain names to tuples corresponding to the values of the strain's group.
    list :
        A list of dictionaries with strains that were skipped from grouping and the reason why (see also: `apply_filters` output).


    >>> strains = ["strain1", "strain2"]
    >>> metadata = pd.DataFrame([{"strain": "strain1", "date": "2020-01-01", "region": "Africa"}, {"strain": "strain2", "date": "2020-02-01", "region": "Europe"}]).set_index("strain")
    >>> group_by = ["region"]
    >>> group_by_strain, skipped_strains = get_groups_for_subsampling(strains, metadata, group_by)
    >>> group_by_strain
    {'strain1': ('Africa',), 'strain2': ('Europe',)}
    >>> skipped_strains
    []

    If we group by year or month, these groups are calculated from the date
    string.

    >>> group_by = ["year", "month"]
    >>> group_by_strain, skipped_strains = get_groups_for_subsampling(strains, metadata, group_by)
    >>> group_by_strain
    {'strain1': (2020, (2020, 1)), 'strain2': (2020, (2020, 2))}

    If we omit the grouping columns, the result will group by a dummy column.

    >>> group_by_strain, skipped_strains = get_groups_for_subsampling(strains, metadata)
    >>> group_by_strain
    {'strain1': ('_dummy',), 'strain2': ('_dummy',)}

    If we try to group by columns that don't exist, we get an error.

    >>> group_by = ["missing_column"]
    >>> get_groups_for_subsampling(strains, metadata, group_by)
    Traceback (most recent call last):
      ...
    augur.errors.AugurError: The specified group-by categories (['missing_column']) were not found.

    If we try to group by some columns that exist and some that don't, we allow
    grouping to continue and print a warning message to stderr.

    >>> group_by = ["year", "month", "missing_column"]
    >>> group_by_strain, skipped_strains = get_groups_for_subsampling(strains, metadata, group_by)
    >>> group_by_strain
    {'strain1': (2020, (2020, 1), 'unknown'), 'strain2': (2020, (2020, 2), 'unknown')}

    If we group by year month and some records don't have that information in
    their date fields, we should skip those records from the group output and
    track which records were skipped for which reasons.

    >>> metadata = pd.DataFrame([{"strain": "strain1", "date": "", "region": "Africa"}, {"strain": "strain2", "date": "2020-02-01", "region": "Europe"}]).set_index("strain")
    >>> group_by_strain, skipped_strains = get_groups_for_subsampling(strains, metadata, ["year"])
    >>> group_by_strain
    {'strain2': (2020,)}
    >>> skipped_strains
    [{'strain': 'strain1', 'filter': 'skip_group_by_with_ambiguous_year', 'kwargs': ''}]

    Similarly, if we group by month, we should skip records that don't have
    month information in their date fields.

    >>> metadata = pd.DataFrame([{"strain": "strain1", "date": "2020", "region": "Africa"}, {"strain": "strain2", "date": "2020-02-01", "region": "Europe"}]).set_index("strain")
    >>> group_by_strain, skipped_strains = get_groups_for_subsampling(strains, metadata, ["month"])
    >>> group_by_strain
    {'strain2': ((2020, 2),)}
    >>> skipped_strains
    [{'strain': 'strain1', 'filter': 'skip_group_by_with_ambiguous_month', 'kwargs': ''}]

    """
    metadata = metadata.loc[list(strains)]
    group_by_strain = {}
    skipped_strains = []

    if metadata.empty:
        return group_by_strain, skipped_strains

    if not group_by or group_by == ('_dummy',):
        group_by_strain = {strain: ('_dummy',) for strain in strains}
        return group_by_strain, skipped_strains

    group_by_set = set(group_by)
    generated_columns_requested = GROUP_BY_GENERATED_COLUMNS & group_by_set

    # If we could not find any requested categories, we cannot complete subsampling.
    if 'date' not in metadata and group_by_set <= GROUP_BY_GENERATED_COLUMNS:
        raise AugurError(f"The specified group-by categories ({group_by}) were not found. Note that using any of {sorted(GROUP_BY_GENERATED_COLUMNS)} requires a column called 'date'.")
    if not group_by_set & (set(metadata.columns) | GROUP_BY_GENERATED_COLUMNS):
        raise AugurError(f"The specified group-by categories ({group_by}) were not found.")

    # Warn/error based on other columns grouped with 'week'.
    if 'week' in group_by_set:
        if 'year' in group_by_set:
            print(f"WARNING: 'year' grouping will be ignored since 'week' includes ISO year.", file=sys.stderr)
            group_by.remove('year')
            group_by_set.remove('year')
            generated_columns_requested.remove('year')
        if 'month' in group_by_set:
            raise AugurError("'month' and 'week' grouping cannot be used together.")

    if generated_columns_requested:

        for col in sorted(generated_columns_requested):
            if col in metadata.columns:
                print(f"WARNING: `--group-by {col}` uses a generated {col} value from the 'date' column. The custom '{col}' column in the metadata is ignored for grouping purposes.", file=sys.stderr)
                metadata.drop(col, axis=1, inplace=True)

        if 'date' not in metadata:
            # Set generated columns to 'unknown'.
            print(f"WARNING: A 'date' column could not be found to group-by {sorted(generated_columns_requested)}.", file=sys.stderr)
            print(f"Filtering by group may behave differently than expected!", file=sys.stderr)
            df_dates = pd.DataFrame({col: 'unknown' for col in GROUP_BY_GENERATED_COLUMNS}, index=metadata.index)
            metadata = pd.concat([metadata, df_dates], axis=1)
        else:
            # Create a DataFrame with year/month/day columns as nullable ints.
            # These columns are prefixed to note temporary usage. They are used
            # to generate other columns, and will be discarded at the end.
            temp_prefix = str(uuid.uuid4())
            temp_date_cols = [f'{temp_prefix}year', f'{temp_prefix}month', f'{temp_prefix}day']
            df_dates = metadata['date'].str.split('-', n=2, expand=True)
            df_dates = df_dates.set_axis(temp_date_cols[:len(df_dates.columns)], axis=1)
            missing_date_cols = set(temp_date_cols) - set(df_dates.columns)
            for col in missing_date_cols:
                df_dates[col] = pd.NA
            for col in temp_date_cols:
                df_dates[col] = pd.to_numeric(df_dates[col], errors='coerce').astype(pd.Int64Dtype())

            # Extend metadata with generated date columns
            # Drop the 'date' column since it should not be used for grouping.
            metadata = pd.concat([metadata.drop('date', axis=1), df_dates], axis=1)

            ambiguous_date_strains = list(_get_ambiguous_date_skipped_strains(
                metadata,
                temp_prefix,
                generated_columns_requested
            ))
            metadata.drop([record['strain'] for record in ambiguous_date_strains], inplace=True)
            skipped_strains.extend(ambiguous_date_strains)

            # Check again if metadata is empty after dropping ambiguous dates.
            if metadata.empty:
                return group_by_strain, skipped_strains

            # Generate columns.
            if 'year' in generated_columns_requested:
                metadata['year'] = metadata[f'{temp_prefix}year']
            if 'month' in generated_columns_requested:
                metadata['month'] = list(zip(
                    metadata[f'{temp_prefix}year'],
                    metadata[f'{temp_prefix}month']
                ))
            if 'week' in generated_columns_requested:
                # Note that week = (year, week) from the date.isocalendar().
                # Do not combine the raw year with the ISO week number alone,
                # since raw year ≠ ISO year.
                metadata['week'] = metadata.apply(lambda row: get_iso_year_week(
                    row[f'{temp_prefix}year'],
                    row[f'{temp_prefix}month'],
                    row[f'{temp_prefix}day']
                    ), axis=1
                )

            # Drop the internally used columns.
            for col in temp_date_cols:
                metadata.drop(col, axis=1, inplace=True)

    unknown_groups = group_by_set - set(metadata.columns)
    if unknown_groups:
        print(f"WARNING: Some of the specified group-by categories couldn't be found: {', '.join(unknown_groups)}", file=sys.stderr)
        print("Filtering by group may behave differently than expected!", file=sys.stderr)
        for group in unknown_groups:
            metadata[group] = 'unknown'

    # Finally, determine groups.
    group_by_strain = dict(zip(metadata.index, metadata[group_by].apply(tuple, axis=1)))
    return group_by_strain, skipped_strains


def _get_ambiguous_date_skipped_strains(
        metadata, temp_prefix, generated_columns_requested):
    """Get strains skipped due to date ambiguity.

    Each value is a dictionary with keys:
    - `strain`: strain name
    - `filter`: filter reason. Used for the final report output.
    - `kwargs`: Empty string since filter reason does not represent a function.
    """
    # Don't yield the same strain twice.
    already_skipped_strains = set()

    if generated_columns_requested:
        # Skip ambiguous years.
        df_skip = metadata[metadata[f'{temp_prefix}year'].isnull()]
        for strain in df_skip.index:
            if strain not in already_skipped_strains:
                yield {
                    "strain": strain,
                    "filter": "skip_group_by_with_ambiguous_year",
                    "kwargs": "",
                }
        already_skipped_strains.update(df_skip.index)
    if 'month' in generated_columns_requested or 'week' in generated_columns_requested:
        # Skip ambiguous months.
        df_skip = metadata[metadata[f'{temp_prefix}month'].isnull()]
        for strain in df_skip.index:
            if strain not in already_skipped_strains:
                yield {
                    "strain": strain,
                    "filter": "skip_group_by_with_ambiguous_month",
                    "kwargs": "",
                }
        already_skipped_strains.update(df_skip.index)
    if 'week' in generated_columns_requested:
        # Skip ambiguous days.
        df_skip = metadata[metadata[f'{temp_prefix}day'].isnull()]
        for strain in df_skip.index:
            if strain not in already_skipped_strains:
                yield {
                    "strain": strain,
                    "filter": "skip_group_by_with_ambiguous_day",
                    "kwargs": "",
                }
        # TODO: uncomment if another filter reason is ever added.
        # already_skipped_strains.update(df_skip.index)


class PriorityQueue:
    """A priority queue implementation that automatically replaces lower priority
    items in the heap with incoming higher priority items.

    Add a single record to a heap with a maximum of 2 records.

    >>> queue = PriorityQueue(max_size=2)
    >>> queue.add({"strain": "strain1"}, 0.5)
    1

    Add another record with a higher priority. The queue should be at its maximum
    size.

    >>> queue.add({"strain": "strain2"}, 1.0)
    2
    >>> queue.heap
    [(0.5, 0, {'strain': 'strain1'}), (1.0, 1, {'strain': 'strain2'})]
    >>> list(queue.get_items())
    [{'strain': 'strain1'}, {'strain': 'strain2'}]

    Add a higher priority record that causes the queue to exceed its maximum
    size. The resulting queue should contain the two highest priority records
    after the lowest priority record is removed.

    >>> queue.add({"strain": "strain3"}, 2.0)
    2
    >>> list(queue.get_items())
    [{'strain': 'strain2'}, {'strain': 'strain3'}]

    Add a record with the same priority as another record, forcing the duplicate
    to be resolved by removing the oldest entry.

    >>> queue.add({"strain": "strain4"}, 1.0)
    2
    >>> list(queue.get_items())
    [{'strain': 'strain4'}, {'strain': 'strain3'}]

    """
    def __init__(self, max_size):
        """Create a fixed size heap (priority queue)

        """
        self.max_size = max_size
        self.heap = []
        self.counter = itertools.count()

    def add(self, item, priority):
        """Add an item to the queue with a given priority.

        If adding the item causes the queue to exceed its maximum size, replace
        the lowest priority item with the given item. The queue stores items
        with an additional heap id value (a count) to resolve ties between items
        with equal priority (favoring the most recently added item).

        """
        heap_id = next(self.counter)

        if len(self.heap) >= self.max_size:
            heapq.heappushpop(self.heap, (priority, heap_id, item))
        else:
            heapq.heappush(self.heap, (priority, heap_id, item))

        return len(self.heap)

    def get_items(self):
        """Return each item in the queue in order.

        Yields
        ------
        Any
            Item stored in the queue.

        """
        for priority, heap_id, item in self.heap:
            yield item


def create_queues_by_group(groups, max_size, max_attempts=100, random_seed=None):
    """Create a dictionary of priority queues per group for the given maximum size.

    When the maximum size is fractional, probabilistically sample the maximum
    size from a Poisson distribution. Make at least the given number of maximum
    attempts to create queues for which the sum of their maximum sizes is
    greater than zero.

    Create queues for two groups with a fixed maximum size.

    >>> groups = ("2015", "2016")
    >>> queues = create_queues_by_group(groups, 2)
    >>> sum(queue.max_size for queue in queues.values())
    4

    Create queues for two groups with a fractional maximum size. Their total max
    size should still be an integer value greater than zero.

    >>> seed = 314159
    >>> queues = create_queues_by_group(groups, 0.1, random_seed=seed)
    >>> int(sum(queue.max_size for queue in queues.values())) > 0
    True

    A subsequent run of this function with the same groups and random seed
    should produce the same queues and queue sizes.

    >>> more_queues = create_queues_by_group(groups, 0.1, random_seed=seed)
    >>> [queue.max_size for queue in queues.values()] == [queue.max_size for queue in more_queues.values()]
    True

    """
    queues_by_group = {}
    total_max_size = 0
    attempts = 0

    if max_size < 1.0:
        random_generator = np.random.default_rng(random_seed)

    # For small fractional maximum sizes, it is possible to randomly select
    # maximum queue sizes that all equal zero. When this happens, filtering
    # fails unexpectedly. We make multiple attempts to create queues with
    # maximum sizes greater than zero for at least one queue.
    while total_max_size == 0 and attempts < max_attempts:
        for group in sorted(groups):
            if max_size < 1.0:
                queue_max_size = random_generator.poisson(max_size)
            else:
                queue_max_size = max_size

            queues_by_group[group] = PriorityQueue(queue_max_size)

        total_max_size = sum(queue.max_size for queue in queues_by_group.values())
        attempts += 1

    return queues_by_group


def calculate_sequences_per_group(target_max_value, counts_per_group, allow_probabilistic=True):
    """Calculate the number of sequences per group for a given maximum number of
    sequences to be returned and the number of sequences in each requested
    group. Optionally, allow the result to be probabilistic such that the mean
    result of a Poisson process achieves the calculated sequences per group for
    the given maximum.

    Parameters
    ----------
    target_max_value : int
        Maximum number of sequences to return by subsampling at some calculated
        number of sequences per group for the given counts per group.
    counts_per_group : list[int]
        A list with the number of sequences in each requested group.
    allow_probabilistic : bool
        Whether to allow probabilistic subsampling when the number of groups
        exceeds the requested maximum.

    Raises
    ------
    TooManyGroupsError :
        When there are more groups than sequences per group and probabilistic
        subsampling is not allowed.

    Returns
    -------
    int or float :
        Number of sequences per group.
    bool :
        Whether probabilistic subsampling was used.

    """
    probabilistic_used = False

    try:
        sequences_per_group = _calculate_sequences_per_group(
            target_max_value,
            counts_per_group,
        )
    except TooManyGroupsError as error:
        if allow_probabilistic:
            print(f"WARNING: {error}", file=sys.stderr)
            sequences_per_group = _calculate_fractional_sequences_per_group(
                target_max_value,
                counts_per_group,
            )
            probabilistic_used = True
        else:
            raise error

    return sequences_per_group, probabilistic_used


class TooManyGroupsError(ValueError):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return str(self.msg)


def _calculate_total_sequences(
        hypothetical_spg: float, sequence_lengths: Collection[int],
) -> float:
    # calculate how many sequences we'd keep given a hypothetical spg.
    return sum(
        min(hypothetical_spg, sequence_length)
        for sequence_length in sequence_lengths
    )


def _calculate_sequences_per_group(
        target_max_value: int,
        sequence_lengths: Collection[int]
) -> int:
    """This is partially inspired by
    https://github.com/python/cpython/blob/3.8/Lib/bisect.py

    This should return the spg such that we don't exceed the requested
    number of samples.

    Parameters
    ----------
    target_max_value : int
        the total number of sequences allowed across all groups
    sequence_lengths : Collection[int]
        the number of sequences in each group

    Returns
    -------
    int
        maximum number of sequences allowed per group to meet the required maximum total
        sequences allowed

    >>> _calculate_sequences_per_group(4, [4, 2])
    2
    >>> _calculate_sequences_per_group(2, [4, 2])
    1
    >>> _calculate_sequences_per_group(1, [4, 2])
    Traceback (most recent call last):
        ...
    augur.filter.subsample.TooManyGroupsError: Asked to provide at most 1 sequences, but there are 2 groups.
    """

    if len(sequence_lengths) > target_max_value:
        # we have more groups than sequences we are allowed, which is an
        # error.

        raise TooManyGroupsError(
            "Asked to provide at most {} sequences, but there are {} "
            "groups.".format(target_max_value, len(sequence_lengths)))

    lo = 1
    hi = target_max_value

    while hi - lo > 2:
        mid = (hi + lo) // 2
        if _calculate_total_sequences(mid, sequence_lengths) <= target_max_value:
            lo = mid
        else:
            hi = mid

    if _calculate_total_sequences(hi, sequence_lengths) <= target_max_value:
        return int(hi)
    else:
        return int(lo)


def _calculate_fractional_sequences_per_group(
        target_max_value: int,
        sequence_lengths: Collection[int]
) -> float:
    """Returns the fractional sequences per group for the given list of group
    sequences such that the total doesn't exceed the requested number of
    samples.

    Parameters
    ----------
    target_max_value : int
        the total number of sequences allowed across all groups
    sequence_lengths : Collection[int]
        the number of sequences in each group

    Returns
    -------
    float
        fractional maximum number of sequences allowed per group to meet the
        required maximum total sequences allowed

    >>> np.around(_calculate_fractional_sequences_per_group(4, [4, 2]), 4)
    1.9375
    >>> np.around(_calculate_fractional_sequences_per_group(2, [4, 2]), 4)
    0.9688

    Unlike the integer-based version of this function, the fractional version
    can accept a maximum number of sequences that exceeds the number of groups.
    In this case, the function returns a fraction that can be used downstream,
    for example with Poisson sampling.

    >>> np.around(_calculate_fractional_sequences_per_group(1, [4, 2]), 4)
    0.4844
    """
    lo = 1e-5
    hi = target_max_value

    while (hi / lo) > 1.1:
        mid = (lo + hi) / 2
        if _calculate_total_sequences(mid, sequence_lengths) <= target_max_value:
            lo = mid
        else:
            hi = mid

    return (lo + hi) / 2
