import pandas as pd
import numpy as np
import typer
import os
import sqlite3
from asyncio import run as aiorun
import settings
from nacsos_data.db.schemas import Import
from db_utils import get_engine_async_preping
from sqlalchemy import select

def main():
    '''
    Generate INCLUSION predictions for documents we don't yet have predictions for
    '''
    async def _main():
        # Get the latest update

        con = sqlite3.connect("map-updates.db")
        cur = con.cursor()
        db_engine_async = get_engine_async_preping(conf_file=settings.config_path)

        
        res = cur.execute('SELECT version from queries WHERE updated=1 ORDER BY date DESC LIMIT 1')
        n_version = res.fetchone()
        if n_version is None:
            print('no import')
            #return
            
        import_name = f'OpenAlex import {n_version}'
        print(settings.PROJECT_ID)        
        async with db_engine_async.session() as session:
            stmt = select(Import).where(Import.project_id==settings.PROJECT_ID)
            result = await session.execute(stmt)
            print([x.name for x in result.scalars().all()])
            
        

        
        try:
            df = pd.read_feather('data/inclusion_predictions.feather')
        except FileNotFoundError as e:
            # Generate predictions for all 
            pass
            
    aiorun(_main())

if __name__ == "__main__":
    typer.run(main)