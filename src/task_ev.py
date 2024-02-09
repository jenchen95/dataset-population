import polars as pl

# Read the data
iso_r10 = (
    pl.scan_csv("../data/data_man/country_iso_r10.csv")
    .drop_nulls(subset=['r10_ar6'])
)
pop_medium = (
    pl.scan_csv("../data/data_raw/WPP2022_Demographic_Indicators_Medium.csv")
    .join(iso_r10, left_on='ISO3_code', right_on='iso', how='inner')
    .rename({'ISO3_code': 'iso', 'Time': 'year', 'TPopulation1Jan': 'population', 'r10_ar6': 'region'})
    .select(['iso', 'region', 'year', 'population'])
    .group_by(['region','year'])
    .agg(pl.sum('population'))
    .with_columns(unit=pl.lit('thousands'))
    .sort('region','year')
)

# Export the data
pop_medium.sink_csv('../data/data_task/pop_medium_r10.csv')