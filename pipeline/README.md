
# running the modelling pipelines

etl must be done first

### config files

Specify at least the following parameters in the config file (use `config_dev` to run a smaller grid with less time splits a fewer features):
- base_filepath
- logging.filename
- db_config.schema_name

Other parameters can be adjusted if needed.

### running the pipeline

Run the pipeline using the following command:
```
python src/pipeline/main.py -c <config_filename> -e_desc '<descriptio of the experiment>'
```

or alternatively:

```
python src/pipeline/main.py -c <config_filename> -e_id <experiment_id>
```
to continue an already started experiment.


