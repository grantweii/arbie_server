#!/usr/bin/env python3
import importlib
from db import DB

if __name__ == '__main__':
    # change script to run here
    script = importlib.import_module('migrations.7-pull-historical-data')
    
    try:
        db = DB()
        script.run(db)
    except Exception as err:
        raise err
    finally:
        if db.connection is not None:
            db.connection.close()