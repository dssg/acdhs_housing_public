# This script generates the Pittsburgh city map with dots highlighting evictions and homelessness spells.
# This map was used fot the data fest presentation.

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go

# load data
df = pd.read_csv(
    "/mnt/data/projects/acdhs-housing/geocoding_data/evictions_2018_with_hl_label.csv")

np.random.seed(42)

# jitter coordinates
sigma = 0.001
df['lat']
df['lat_jitter'] = df['lat'].apply(
    lambda x: float(np.random.normal(x, sigma, 1)))
df['lon_jitter'] = df['lon'].apply(
    lambda x: float(np.random.normal(x, sigma, 1)))
df['marker_size'] = [x+5 if x == 0 else x+8 for x in df.homelessness_label]

fig = px.scatter_mapbox(
    df,
    lat="lat_jitter",
    lon="lon_jitter",
    opacity=0.4,
    size='marker_size',
    size_max=7.8,
    color_discrete_sequence=["white"])


fig2 = px.scatter_mapbox(
    df[df['homelessness_label'] == 1],
    lat="lat_jitter",
    lon="lon_jitter",
    opacity=1,
    size='marker_size',
    size_max=7.8,
    color_discrete_sequence=["#F9B900"],
)

fig.add_trace(fig2.data[0])


fig.update_layout(
    margin={"r": 0, "t": 0, "l": 0, "b": 0},
    mapbox=dict(
        #        accesstoken="[put access token here]",
        #        style='mapbox://styles/arunf/cl6v8goxr000k15rpiiktuyeq',
        zoom=12,
    ),
)

fig.update_layout(
    autosize=False,
    width=900,
    height=900,
    coloraxis_showscale=False,
    showlegend=False)

fig.write_image(
    '/mnt/data/projects/acdhs-housing/geocoding_data/eviction_map.png')
