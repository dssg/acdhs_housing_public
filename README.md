# Reducing Entry Into Homelessness through ML-Informed Rental Assistance

Repository for a project to prioritize individuals currently facing eviction for rental assistance based on their predicted risk of future homelessness. This is an ongoing collaboration between Carnegie Mellon University (CMU) and the Allegheny County Department of Human Services (ACDHS). This work began as a project for the Data Science for Social Good Summer Fellowship 2022 at CMU in Pittsburgh (the final code from the DSSG fellowship is available [here](https://github.com/dssg/acdhs_housing_public/releases/tag/dssg2022)).

## Data

We use data from ACDHS' rich data warehouse that includes individual-level information on previous evictions, homeless spells, interactions with mental, behavioral, and physical health services, address changes, demographic information, as well as enrollment in a variety of other ACDHS and state programs. We refresh the data from the warehouse on a weekly basis. This data is not publicly available; researchers interested in collaborating with ACDHS should contact them directly. 

## Formulation of the Modeling Problem

Using the historical data described above, we trained a series of models that predict an individual's risk of experiencing homelessness within 12 months of the prediction, among all those with a recent eviction filing. Using these predictions, every week we provide ACDHS with a list of individuals who are at highest risk of experiencing homelessness in the near future, among those currently facing eviction.

### Cohort 

The cohort is individuals in Allegheny County
1. who have had an eviction filing or a disposition in the four months prior to the prediction date
2. are not currently homeless
   
We define "currently homeless" individuals to be anyone who has interacted with homelessness services (as defined below) in the four months prior to the prediction date.

### Outcome Label 

Individuals belong to the class of interest (`label=1`) if they interact with homelessness services at least once within 12 months of the prediction date. The relevant homelessness services include emergency shelters, street outreach programs, transitional housing, and other re-housing programs. For each weekly list, we predict the top `k` individuals (`k` is based on intervention capacity) in terms of their risk of interacting with homelessness services within 12 months.

### Alternate formulations

We use the above cohort and label definitions in our experiments, but alternate formulations we have explored are documented in [`pipeline\configs\README.md`](pipeline/configs/README.md).

## Methodology
1. Define cohort based on problem formulation
2. Define outcome label based on formulation
4. Define training and validation sets over time 
5. Define and generate predictors 
6. Train Models on each training set and score all individuals in the corresponding validation set
7. Evaluate all models for each validation time according to each metric (PPV at top k)
8. Select "Best" model based on results over time
9. Explore the high performing models to understand who they rank high, how they compare to the cohort, and important predictors
10. Check and/or correct for bias issues

## Repository Structure

To build our predictive models, we use [Triage](https://github.com/dssg/triage), an open-sourced ML pipeline tool built and maintained by our team. Triage enables the modeling parameters to be set in a YAML configuration file, and the full configuration is defined in [this YAML config file](pipeline/configs/base_config.yaml) (feature definitions are defined separately).

* [Features](pipeline/configs/feature_groups/)
* [Models](pipeline/configs/base_config.yaml#L33)
* [Label and cohort definition](pipeline/configs/labels/facing_eviction_homelessness.sql)

## Triage 
We are using [Triage](https://github.com/dssg/triage) to build and select models. Some background and tutorials:

* [Tutorial on Google Colab](https://colab.research.google.com/github/dssg/triage/blob/master/example/colab/colab_triage.ipynb): a quick tutorial hosted on Google Colab (no setup necessary) for users who are completely new to Triage
* [Dirty Duck Tutorial](https://dssg.github.io/triage/dirtyduck/): a more in-depth walk-through of Triage's functionality and concepts using sample data
* [QuickStart Guide](https://dssg.github.io/triage/quickstart/): try Triage on your own project and data
* [Suggested workflow](https://dssg.github.io/triage/triage_project_workflow/)
* [Understanding the configuration file](https://dssg.github.io/triage/experiments/experiment-config/#experiment-configuration)

## How to run

### Requirements

- Triage is installed and the data is in a PostgreSQL database
- Place the experiment config YAML in `pipeline/configs` folder
- The database credentials and fed through environmental variables. The required environmental variables: 
    - PGUSER (username for the db)
    - PGHOST (database server)
    - PGDATABASE (database name)
    - PGROLE (shared database role, if any)
    - PGPORT (relevant port of the database server)
    - PGPASSWORD (password to authenticate access to the db server)
    - PYTHONPATH (should be the path to the base folder)
- These values could be set on the `.bashrc` or `.bash_profile` files in the users home folder, or could be loaded through a `.env` file (see `.env_sample`) placed in the base folder. That file needs to contain the above fields (format: `field=value`).
- If a .env file is used, prior to running the pipeline, can load the variables using the following command

```
 export $(xargs < .env)
```

### Running the pipeline

The pipeline is executed using the `run.py` script, which takes a config file (filename only, not full path; the file should be located in `pipeline/configs`) as a parameter. The following command can be used to execute the pipeline for the specified config. 

```
python run.py -c <config_filename> -n <number of jobs> 
```

Other tips:
- You can change the number of jobs across which you would like Triage to parallelize the process. If not provided, this runs a single threaded experiment (integer). 
- A bash script placed in the base folder that loads the environmental variables and runs the pipeline could make the process easier.
