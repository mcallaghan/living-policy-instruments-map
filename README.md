## Living map of climate policy instruments

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install --nodeps git+https://gitlab.pik-potsdam.de/mcc-apsis/nacsos/nacsos-data.git@v0.14.6
```

Steps to animate the map
- Run the query. If update necessary and desired, create new import
- Run models on any new imports, creating new data outputs
  
```
nohup python run_query.py data/query.txt --force-update
```