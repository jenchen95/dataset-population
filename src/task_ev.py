import polars as pl
import polars.selectors as cs

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

gdp_history = (
    pl.read_excel(source='../data/data_raw/gdp-2010-constant.xlsx', sheet_name='Data')
    .lazy()
    .join(iso_r10, left_on='Country Code', right_on='iso', how='left')
    .rename({'Indicator Name':'unit', 'r10_ar6':'region'})
    .drop_nulls(subset=['region'])
    .select(pl.col('region','unit'), cs.float())
    .group_by(
        ['region','unit']
    )
    .agg(pl.all().sum())
    .melt(id_vars=['region','unit'], variable_name='year', value_name='gdp')
    .cast({'year':pl.Int64})
    .select(pl.col('region','year','gdp','unit'))
)

gdp_per_cap_history = (
    gdp_history.join(pop_medium, on=['region','year'], suffix='_pop')
    .drop_nulls(subset=['gdp','population'])
    .with_columns(gdp_per_cap=pl.col('gdp') / pl.col('population') / 1000)
    .select(pl.col('region','year','gdp_per_cap','unit','gdp','population','unit_pop'))
)

# Export the data
pop_medium.sink_csv('../data/data_task/pop_medium_r10.csv')
gdp_per_cap_history.sink_csv('../data/data_task/gdp_per_cap_r10.csv')