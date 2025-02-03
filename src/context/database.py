from pathlib import Path
import numpy as np
import pandas as pd


class Database:
    def __init__(self, database_path: Path) -> None:
        self.database_path: Path = database_path
        self.database: pd.DataFrame = pd.read_pickle(self.database_path)
        self.tmp_database: pd.DataFrame = pd.DataFrame(columns=self.database.columns)

    @classmethod
    def from_scratch(cls, database_path: Path, columns: list[str]) -> "Database":
        pd.DataFrame(columns=columns).to_pickle(database_path)
        return cls(database_path)

    def add_entry(self, index: int, entry_dict: dict[str, str]) -> None:
        try:
            entry_df = pd.DataFrame([entry_dict])
            entry_df.index = [index]
            self.tmp_database = pd.concat([self.tmp_database, entry_df], axis=0)
        except Exception as e:
            print(f"Error adding entry: {e}")
            raise
        self.save()

    def find_entries(self, query: dict[str, str]) -> pd.DataFrame:
        return self.database[self._get_mask(query)]

    def modify_entry(self, to_modify: tuple[str, str], query: dict[str, str]) -> None:
        column = to_modify[0]
        value = to_modify[1]
        self.database.loc[self._get_mask(query), column] = value

    def save(self) -> None:
        for index in self.tmp_database.index:
            self.database.loc[index] = self.tmp_database.loc[index]
        self.tmp_database.drop(self.tmp_database.index, inplace=True)
        self.database.to_pickle(self.database_path)

    def _get_mask(self, query: dict[str, str]) -> pd.Series:
        masks = [self.database[key] == value for key, value in query.items()]
        return np.logical_and.reduce(masks)
