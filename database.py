import numpy as np
import pandas as pd
import os
from typing import Dict, Any, List, Mapping


class Database:
    def __init__(self, database_path: os.PathLike) -> None:
        self.database_path = database_path
        self.database = pd.read_pickle(self.database_path)
        self.tmp_database = pd.DataFrame(columns=self.database.columns)

    @classmethod
    def from_scratch(cls, database_path: os.PathLike, columns: List[str]) -> "Database":
        pd.DataFrame(columns=columns).to_pickle(database_path)
        return cls(database_path)

    def add_entry(self, index: int, entry_dict: Dict[str, Any]) -> None:
        self.tmp_database.loc[index] = entry_dict

    def find_entries(self, **kwargs: Mapping[str, Any]) -> pd.DataFrame:
        return self.database[self._get_mask(**kwargs)]

    def modify(self, to_modify: Dict[str, Any], **kwargs: Mapping[str, Any]) -> None:
        column = list(to_modify.keys())[0]
        value = list(to_modify.values())[0]
        self.database.loc[self._get_mask(**kwargs), column] = value

    def save(self) -> None:
        for index in self.tmp_database.index:
            self.database.loc[index] = self.tmp_database.loc[index]
        self.tmp_database.drop(self.tmp_database.index, inplace=True)
        self.database.to_pickle(self.database_path)

    def _get_mask(self, **kwargs: Mapping[str, Any]) -> pd.Series:
        masks = [self.database[key] == value for key, value in kwargs.items()]
        return np.logical_and.reduce(masks)


if __name__ == "__main__":
    ...
