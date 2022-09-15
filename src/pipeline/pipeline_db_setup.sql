-- experiment metadata
create table if not exists {config.db_config.schema_name}.{config.experiment_config.experiment_table_name}
(
    experiment_id           serial primary key,
    exp_desc                varchar,
    experiment_start_time   timestamp with time zone default now(),
    exp_config              json
);

-- matrix metadata
create table if not exists {config.db_config.schema_name}.{config.matrix_config.table_name}
(
    experiment_id   int not null references {config.db_config.schema_name}.{config.experiment_config.experiment_table_name},
    matrix_type     varchar,
    matrix_date     timestamp,
    cohort_hash     varchar,
    feature_hash    varchar,
    label_hash      varchar,
    matrix_hash     varchar
);

-- model groups
create table if not exists {config.db_config.schema_name}.{config.modeling_config.model_group_table_name}
(
    model_group_id      serial primary key,
    experiment_id       int not null references {config.db_config.schema_name}.{config.experiment_config.experiment_table_name},
    model_name          varchar
);

-- model metadata
create table if not exists {config.db_config.schema_name}.{config.modeling_config.model_table_name}
(
    model_id            serial primary key,
    experiment_id       int not null references {config.db_config.schema_name}.{config.experiment_config.experiment_table_name},
    model_group_id      int not null references {config.db_config.schema_name}.{config.modeling_config.model_group_table_name},
    model_name          varchar,
    train_end_date      timestamp,
    train_matrix_hash   varchar
);

-- model params
create table if not exists {config.db_config.schema_name}.{config.modeling_config.model_params_table_name}
(
    experiment_id       int not null references {config.db_config.schema_name}.{config.experiment_config.experiment_table_name},
    model_id            int not null references {config.db_config.schema_name}.{config.modeling_config.model_table_name},
    model_group_id      int not null references {config.db_config.schema_name}.{config.modeling_config.model_group_table_name},
    model_name          varchar,
    model_param_name    varchar,
    model_param_value   varchar
);

-- model predictions
create table if not exists {config.db_config.schema_name}.{config.modeling_config.predictions_table_name}
(
    client_hash         varchar,
    as_of_date          timestamp,
    homelessness_label  int,
    score               double precision,
    model_id            int not null references {config.db_config.schema_name}.{config.modeling_config.model_table_name}
);

-- model label
create table if not exists {config.db_config.schema_name}.{config.label_config.table_name}
(
    client_hash         varchar,
    as_of_date          timestamp,
    homelessness_label  int
);

-- model results
create table if not exists {config.db_config.schema_name}.{config.evaluation.metrics_table_name}
(
    top_n               int,
    threshold           double precision,
    acceptance_rate     double precision,
    recall              double precision,
    fpr                 double precision,
    fnr                 double precision,
    tnr                 double precision,
    "precision"         double precision,
    fdr                 double precision,
    forate              double precision,
    npv                 double precision,
    model_id            double precision,
    validation_date     timestamp
);

commit;