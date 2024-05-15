import typer
from typing_extensions import Annotated
import sqlite3
import requests  
import string
from datetime import datetime
from typing import TYPE_CHECKING, Callable, Generator
import uuid
#from nacsos_pipelines.nacsos_lib.academic.import import import_openalex
#from nacsos_pipelines.nacsos_lib.twitter.twitter_api import search_twitter
from nacsos_data.db.connection import _get_settings
from nacsos_data.db import get_engine_async, get_engine
from db_utils import get_engine_async_preping
from nacsos_data.db.crud.imports import get_or_create_import, read_item_count_for_import
from nacsos_data.util.academic.openalex import generate_items_from_openalex
from nacsos_data.util.academic.importer import import_academic_items
from nacsos_data.models.items.academic import AcademicItemModel
import logging
import asyncio
import sys
from asyncio import run as aiorun

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))


PROJECT_ID = '7bdc33a8-55d4-4670-8989-df5d0b304ad6'
USER_ID = 'b4c20ee5-e415-4ac8-8e9d-77770311e38c'

def setup_db(cur):
    '''Setup database to store queries'''
    #cur.execute("DROP TABLE IF EXISTS queries;")
    cur.execute("""CREATE TABLE IF NOT EXISTS queries(
        query_path text, 
        date real,
        n_records integer,
        n_dedup integer,
        updated integer,
        version integer,
        import_id text
    ) 
    """)

def oa_n_results(q):
    '''Returns the number of results returned by a query'''
    data = {
        'q': q,
        'df': 'title_abstract',
        'rows': 0,
    }

    url = 'http://srv-mcc-apsis-rechner:8983/solr/openalex/select'
    n_found = requests.post(url, data=data).json()['response']['numFound']
    return n_found

def generate_import(project_id, user_id):
    '''Creates a new import object'''
    pass

async def read_oa(import_id, q, logger, PROJECT_ID, db_engine_async):
    logger.info('Read oa items and add to the import')
    url = 'http://0.0.0.0:8983/solr/openalex'
    def _read_openalex() -> Generator[AcademicItemModel, None, None]:
        for itm in generate_items_from_openalex(
                query=q,
                openalex_endpoint=url,
                def_type='lucene',
                field='title_abstract',
                op='AND',
                batch_size=10000,
                log=logger
        ):
            itm.item_id = uuid.uuid4()
            yield itm
    
    async with db_engine_async.session() as session:
        await import_academic_items(
            session=session,
            project_id=PROJECT_ID,
            new_items=_read_openalex,
            import_name=None,
            description=None,
            user_id=None,
            import_id=import_id,
            vectoriser=None,
            max_slop=0.05,
            batch_size=5000,
            dry_run=False,
            trust_new_authors=False,
            trust_new_keywords=False,
            log=logger
        )

def main(query_path: str, force_update: Annotated[bool, typer.Option("--force-update")] = False):
    '''
    Run query on openalex, check the number of results against previous query runs, and create a new import in NACSOS2 if desired.

    If --force-update is used, a new import will be created without asking as long as the number of results is different to the last update
    '''
    async def _main():
        logging.basicConfig(filename='logs/update_query.log', level=logging.INFO)
        logger.info(f"Hello {query_path}")
        con = sqlite3.connect("map-updates.db")
        cur = con.cursor()
        now = datetime.now()
    
        
        engine = get_engine(conf_file=settings.config_path)
        #db_engine_async = get_engine_async(conf_file=config_path)
        db_engine_async = get_engine_async_preping(conf_file=config_path)
    
        ## Set up DB
        setup_db(cur)
    
        # Read query
        with open(query_path,'r') as f:
            q = f.read()
    
        # Get number of oa results
        n_found = oa_n_results(q)
    
        # Get number of results in the last update
        res = cur.execute('SELECT n_records, date, version from queries WHERE updated=1 ORDER BY date DESC LIMIT 1')
        n_db = res.fetchone()
        if n_db is not None:
            date_update = datetime.fromtimestamp(n_db[1])
            n_version = n_db[2]
            n_db = n_db[0]
        else:
            date_update = None
            n_version = 0
    
        # Do we need to update?
        msg = f'the last update ({date_update}) contained {n_db}, while the latest search found {n_found}'
        if n_db is None or n_db != n_found:
            if force_update:
                update = True
            else:
                update = typer.confirm(f'{msg}. Would you like to carry out an update?')
        else:
            logger.info(f'{msg}. Not carrying out an update')
            update = False
            
        if update:
            n_version += 1
            ## Create new import
            import_name = f'OpenAlex import {n_version}: created on {now.strftime("%Y-%m-%d")}'
            logger.info(f'Creating new import: {import_name}')
            async with db_engine_async.session() as session:
                nacsos_import = await get_or_create_import(
                    session=session,
                    project_id=PROJECT_ID,
                    import_name=import_name,
                    user_id=USER_ID,
                    description=import_name
                )
                import_id = nacsos_import.import_id
                logger.info(f'Import ID: {import_id}')
                
            ## Add oa items to import
            await read_oa(import_id, q, logger, PROJECT_ID, db_engine_async)

            # How many deduplicated?
            n_dedup = await read_item_count_for_import(import_id, db_engine_async)
            logger.info(f'Found {n_dedup} records after deduplication')
    
            # Update query versions table
            data = (query_path, round(now.timestamp()),n_found, n_dedup, True, n_version, str(import_id))
            cur.execute(f'INSERT into queries VALUES (?,?,?,?,?,?,?)',data)
        else:
            data = (query_path, round(now.timestamp()), n_found, None, False, n_version, None)
            cur.execute(f'INSERT into queries VALUES (?,?,?,?,?,?,?)',data)
            
        con.commit()
    aiorun(_main())


if __name__ == "__main__":
    typer.run(main)