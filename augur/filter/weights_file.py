import pandas as pd
from textwrap import dedent
from typing import List, Optional, Union
from augur.errors import AugurError


WEIGHTS_COLUMN = 'weight'
COLUMN_VALUE_FOR_DEFAULT_WEIGHT = 'default'


class InvalidWeightsFile(AugurError):
    def __init__(self, file, error_message):
        super().__init__(f"Bad weights file {file!r}.\n{error_message}")


# FIXME: actually use this and figure out how to make the _df attribute mote
# caller-friendly and not read the file multiple times
class WeightsFile:
    """Represents a weights file."""

    path: str
    """Path to the file on disk."""

    _df: pd.DataFrame

    weighted_columns: List[str]

    default_weight: Optional[Union[int, float]]

    def __init__(self, path: str, read_dataframe=False):
        """
        Parameters
        ----------
        path
            Path of the weights file.
        read_dataframe
            Indicate whether to read the file into the dataframe.
        """
        self.path = path

        if read_dataframe:
            self._df = self.read_dataframe()

        self.weighted_columns = self._get_weighted_columns()

        self.default_weight = self._get_default_weight()

    def read_dataframe(self):
        weights = pd.read_csv(self.path, delimiter='\t', comment='#')

        if not pd.api.types.is_numeric_dtype(weights[WEIGHTS_COLUMN]):
            non_numeric_weight_lines = [index + 2 for index in weights[~weights[WEIGHTS_COLUMN].str.isnumeric()].index.tolist()]
            raise InvalidWeightsFile(self.path, dedent(f"""\
                Found non-numeric weights on the following lines: {non_numeric_weight_lines}
                {WEIGHTS_COLUMN!r} column must be numeric."""))

        if any(weights[WEIGHTS_COLUMN] < 0):
            negative_weight_lines = [index + 2 for index in weights[weights[WEIGHTS_COLUMN] < 0].index.tolist()]
            raise InvalidWeightsFile(self.path, dedent(f"""\
                Found negative weights on the following lines: {negative_weight_lines}
                {WEIGHTS_COLUMN!r} column must be non-negative."""))

        return weights

    def _get_weighted_columns(self):
        with open(self.path) as f:
            has_rows = False
            for row in f:
                has_rows = True
                if row.startswith('#'):
                    continue
                columns = row.rstrip().split('\t')
                break

        if not has_rows:
            raise InvalidWeightsFile(self.path, "File is empty.")

        columns.remove(WEIGHTS_COLUMN)

        return columns

    def _get_default_weight(self):
        default_weight_values = self._df[(self._df[self.weighted_columns] == COLUMN_VALUE_FOR_DEFAULT_WEIGHT).all(axis=1)][WEIGHTS_COLUMN].unique()

        if len(default_weight_values) > 1:
            raise InvalidWeightsFile(self.path, f"Multiple default weights were specified: {', '.join(repr(weight) for weight in default_weight_values)}. Only one default weight entry can be accepted.")
        if len(default_weight_values) == 1:
            return default_weight_values[0]

