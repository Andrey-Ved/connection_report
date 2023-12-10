import pandas as pd
from os import environ


environ["MODE"] = "TEST"

pd.set_option('display.max_columns', 10)
pd.set_option('display.width', 380)
pd.set_option('display.max_rows', 30)

print(
    f'\n'
    f'init root-conftest'  # noqa
)
