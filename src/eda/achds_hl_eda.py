from utils import get_db_conn
import pandas as pd
import numpy as np
import plotly.io as pio
import plotly.express as px
import os
import sys
import kaleido
import matplotlib.pyplot as plt
from pywaffle import Waffle

# load functions
sys.path.append("../")

# plotly parameters
pio.templates.default = "plotly_white"

layout_update_dict = dict(
    font={"size": 20},
    title={"font": {"size": 24}},
)

# generate plots folder if doesnt exist
if not os.path.exists("plots"):
    os.mkdir("plots")

# connect to database
db_conn = get_db_conn()

# specify parameters for sql query
DATE_ANALYSIS = "2019-01-01"
MONTHS_COHORT = 120
MONTHS_CURRENT_HL = 12
MONTHS_FUTURE_HL = 12
MONTHS_EVICT_RANGE = 24
HL_PROGRAM_KEY = "(263)"
TABLE_NAME = f"acdhs_{MONTHS_COHORT}months"

# should existing table be dropped?
drop_model_table = False

# load sql query and add parameters
with open("src/eda/acdhs_hl_pipe.sql", "r") as f:
    sql_query = f.read()

sql_query = sql_query.format(
    TABLE_NAME=TABLE_NAME,
    DATE_ANALYSIS=DATE_ANALYSIS,
    MONTHS_COHORT=MONTHS_COHORT,
    MONTHS_CURRENT_HL=MONTHS_CURRENT_HL,
    MONTHS_FUTURE_HL=MONTHS_FUTURE_HL,
    MONTHS_EVICT_RANGE=MONTHS_EVICT_RANGE,
    HL_PROGRAM_KEY=HL_PROGRAM_KEY,
)

# checks if table should be dropped
if drop_model_table:
    db_conn.execute(f"drop table if exists modelling.{TABLE_NAME};")
    db_conn.execute("commit")

# gen table
db_conn.execute(sql_query, db_conn)
db_conn.execute("commit")

# open table
sql_query = f"select * from modelling.{TABLE_NAME}"
df = pd.read_sql(sql_query, db_conn)

# inspect data
df.head()

# gen table
df["past_hl"].value_counts(normalize=True)
df["current_hl"].value_counts(normalize=True)
df["future_hl"].value_counts(normalize=True)

# gen figures
df2 = df
df2["Future Homelessness"] = df["future_hl"].replace([0, 1], ["No", "Yes"])
df2["Race"] = df2["race_grp"].replace(
    ["Black/African American"], "African American")
df2["Sex"] = df2["legal_sex"].replace(
    ["1~Male", "2~Female"], ["Male", "Female"])
df2["Prev. homeless"] = df2["past_hl"].replace([0, 1], ["No", "Yes"])
df2["Curr. homeless"] = df2["current_hl"].replace([0, 1], ["No", "Yes"])
df2["Eviction"] = df2["had_eviction"].replace([0, 1], ["No", "Yes"])


# Bar plots
for v in ["Race", "Sex", "Prev. homeless", "Curr. homeless", "Eviction"]:

    print(v)

    temp = df2.groupby(["Future Homelessness", v]).size().reset_index(name="n")

    temp["per"] = (
        df.groupby(["Future Homelessness", v])
        .size()
        .groupby(level=0)
        .apply(lambda x: 100 * x / float(x.sum()))
        .values
    )

    temp = temp.melt(id_vars=["Future Homelessness", "n", "per"])

    fig = px.bar(
        temp,
        x="Future Homelessness",
        y="per",
        color="value",
        facet_col_wrap=2,
        labels=dict(value=v, per="Percentage (%)", variable=""),
    )

    fig.write_image(f"plots/bar_{v}.png", engine="kaleido")

# Violin plots
df2 = df
df2["future_hl"] = df2["future_hl"].replace([0, 1], ["No", "Yes"])

# Violin plot by program months
fig = px.violin(
    df2,
    x="future_hl",
    y="nr_programs_not_housing",
    color="past_hl",
    points=False,
    box=True,
    title="Homeless individuals spend longer time in programs",
    labels=dict(
        nr_programs_not_housing="Program-months (#)",
        future_hl="Future homelessness",
        past_hl="Prev. homeless",
    ),
)

fig.update_layout(layout_update_dict, width=1000)

fig.write_image("plots/hl_program_violin.png", engine="kaleido")

# Violin plot by distinct programs
fig = px.violin(
    df2,
    x="future_hl",
    y="nr_distinct_programs_not_housing",
    color="past_hl",
    points=False,
    box=True,
    title="Homeless individuals participate in more distinct programs",
    labels=dict(
        nr_distinct_programs_not_housing="Programs (#)",
        future_hl="Future homelessness",
        past_hl="Prev. homeless",
    ),
)

fig.update_layout(layout_update_dict, width=1000)

fig.write_image("plots/hl_distinct_program_violin.png", engine="kaleido")


# Waffle plot
data = {"Never homeless": 98.5, "Homeless at any point": 1.5}

fig = plt.figure(
    FigureClass=Waffle,
    rows=5,
    values=data,
    colors=("blue", "red"),
    legend={
        "loc": "lower left",
        "bbox_to_anchor": (0, -0.4),
        "ncol": len(data),
        "framealpha": 0,
    },
)
fig.savefig("plots/waffle.png", dpi=300, transparent=False)
