## Setting up Triage Pipelines 

We use YAML configurations files to set up the ML training and inference pipelines using Triage. The following configurations are used: 

- `base_config.yaml`: Main configuration file used for triage experiments. 

- `train_models_for_trial.yaml` - Configuration file used to periodically retrain the selected ML model during the trial. 


The `labels` folder contains the `SQL` scripts used to capture the modeling cohort (defining who is included in our modeling universe) and the label (incidence of the outcome of interest). The primary cohort and label of interest is presented in the file `facing_eviction_homelessness.yaml`. The other files capture different modeling formulations (e.g., facing eviction first time homelessness) we experimented with. 


The `feature_groups` folder contains the definitions of all predictros we build using Triage. Features are organized in collections called "feature groups" and each feature group has its own YAML file stored in this folder. 

