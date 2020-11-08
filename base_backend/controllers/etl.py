import pandas as pd
import numpy as np
from time import strptime
from sklearn.utils import shuffle
import h2o
from h2o.automl import H2OAutoML


def csv_to_pandas(csv):
    return pd.read_csv(csv)


async def data_cleaning(df, df_country_codes, df_country_coor):
    # Remove columns with > 70% missing data
    df = remove_columns_percentage_missing(df, 70)
    # Replace na with 0
    df = df.replace(np.nan, 0)
    print(df.head())
    # Date (asumimos que si no está year-mes está date)
    if {'Year', 'Month'}.issubset(df.columns):
        pass
    else:
        df['Year'] = pd.DatetimeIndex(df['Date']).year
        df['Month'] = pd.DatetimeIndex(df['Date']).month
        df['Day'] = pd.DatetimeIndex(df['Date']).day

    # Si pais en código-> nombre
    if len(df['Country_origin'][0]) == 2:
        df_country_codes = df_country_codes.rename(columns={df_country_codes.columns[1]: 'Country_origin'})
        df = pd.merge(df, df_country_codes, how='left', on='Country_origin')
        df = df.drop(columns=['Country_origin']).rename(columns={'Country': 'Country_origin'})

    if 'Country_dest' in df.columns:
        if len(df['Country_dest'][0]) == 2:
            df_country_codes = df_country_codes.rename(columns={df_country_codes.columns[1]: 'Country_dest'})
            df = pd.merge(df, temp, on='Country_dest', how='left')
            df = df.drop(columns=['Country_dest']).rename(columns={'Country': 'Country_dest'})

    # Obtain latitude from country or country from latitude if any is missing
    # Country origin
    if {'Country_origin', 'Latitude_origin', 'Longitude_origin'}.issubset(df.columns):
        pass
    else:
        df_country_coor = df_country_coor.rename(
            columns={'Country': 'Country_origin', 'latitude': 'Latitude_origin', 'longitude': 'Longitude_origin'})
        if 'Country_origin' in df:
            df = pd.merge(df, df_country_coor, on=['Country_origin'], how='left')
        elif {'Latitude_origin', 'Longitude_origin'}.issubset(df.columns):
            df = pd.merge(df, df_country_coor, on=['Latitude_origin', 'Longitude_origin'], how='left')

    # Country dest
    if {'Country_dest', 'Latitude_dest', 'Longitude_dest'}.issubset(df.columns):
        pass
    else:
        df_country_coor = df_country_coor.rename(
            columns={'Country': 'Country_dest', 'latitude': 'Latitude_dest', 'longitude': 'Longitude_dest'})
        if 'Country_dest' in df:
            df = pd.merge(df, df_country_coor, on=['Country_dest'], how='left')
        elif {'Latitude_dest', 'Longitude_dest'}.issubset(df.columns):
            df = pd.merge(df, df_country_coor, on=['Latitude_dest', 'Longitude_dest'], how='left')

    # Categorical columns to lowercase
    df_col_types = pd.DataFrame(df.dtypes).reset_index()
    df_col_types = df_col_types.rename(columns={'index': 'col', df_col_types.columns[1]: 'type'})
    cat_colums = list(df_col_types.query("type == 'object' ")['col'])
    for cat_colum in cat_colums:
        df[cat_colum] = df[cat_colum].str.lower()

    # Add Seasons
    spring = range(3, 5)
    summer = range(6, 8)
    fall = range(9, 11)

    season = []
    for month in df['Month']:
        if month in spring:
            season.append("spring")
        elif month in summer:
            season.append("summer")
        elif month in fall:
            season.append("fall")
        else:
            season.append("winter")
    df['Season'] = season

    return df.to_json()


def remove_colum_spaces(df):
    old_colnames = df.columns
    new_colnames = []
    for col in old_colnames:
        new_name = col.replace(" ", "_")
        new_colnames.append(new_name)
    df = df.rename(columns=dict(zip(old_colnames, new_colnames)))
    return df


def remove_columns_percentage_missing(df, percentaje):
    percent_missing_df = df.isnull().sum() * 100 / len(df)
    df_missing = pd.DataFrame({'column_name': df.columns, 'percent_missing': percent_missing_df})
    columns_to_remove = list(df_missing[df_missing['percent_missing'] > percentaje]['column_name'])
    df = df.drop(columns=columns_to_remove)
    return df


def load_model_predict(col_to_predict, test_data):
    if col_to_predict == 'animal':
        model_path = "mymodel_animal"
    elif col_to_predict == "item":
        model_path = "mymodel_item"
    elif col_to_predict == "destino":
        model_path = "mymodel_dest"

    h2o_test = h2o.H2OFrame(test_data)
    model = h2o.load_model(model_path)
    predictions = model.predict(h2o_test)
    return predictions.columns
