import typer
import sqlite3
import pandas as pd
from tabulate import tabulate
import settings

def main():
    con = sqlite3.connect("map-updates.db")
    df = pd.read_sql('SELECT * FROM queries', con)
    df['date'] = pd.to_datetime(df['date'], unit='s')
    print(tabulate(df, headers='keys'))

if __name__ == "__main__":
    typer.run(main)