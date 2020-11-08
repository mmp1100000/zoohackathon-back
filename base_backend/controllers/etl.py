import pandas as pd
import numpy as np
from time import strptime
from sklearn.utils import shuffle
import h2o
from h2o.automl import H2OAutoML
import networkx as nx

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


def create_graph_from_df():

    # Creating unique nodes
    items = data[["Item", "Count"]].drop_duplicates().reset_index().rename(columns={'index': 'item_id'})
    infraction = data[
        ["Category", "Order_in_Trade_Route", "Outcome", "Year", "Month"]].drop_duplicates().reset_index().rename(
        columns={'index': 'infraction_id'})
    source = data[["Source_Type"]].drop_duplicates().reset_index().rename(columns={'index': 'source_id'})
    species = data[
        ["Scientific_Name", "Kingdom", "Phylum", "Class", "Order", "Family", "Genus", "Species", "Common_Name",
         "Role"]].drop_duplicates().reset_index().rename(columns={'index': 'species_id'})
    countries_dest = data[["Country_dest", "Region_dest", "City_dest", "Latitude_dest",
                           "Longitude_dest"]].drop_duplicates().reset_index().rename(
        columns={'index': 'countries_dest_id'})
    countries_origin = data[
        ["Country_origin", "Latitude_origin", "Longitude_origin"]].drop_duplicates().reset_index().rename(
        columns={'index': 'countries_origin_id'})

    # Create data with IDs
    ided_data = countries_origin.merge(countries_dest.merge(species.merge(source.merge(
        infraction.merge(items.merge(data, on=["Item", "Count"]),
                         on=["Category", "Order_in_Trade_Route", "Outcome", "Year", "Month"]), on=["Source_Type"]),
                                                                          on=["Scientific_Name", "Kingdom", "Phylum",
                                                                              "Class", "Order", "Family", "Genus",
                                                                              "Species", "Common_Name", "Role"]),
                                                            on=["Country_dest", "Region_dest", "City_dest",
                                                                "Latitude_dest", "Longitude_dest"]),
                                       on=["Country_origin", "Latitude_origin", "Longitude_origin"])

    # Creating relationships
    relation_items_infraction = ided_data[["item_id", "infraction_id"]].groupby(["item_id", "infraction_id"],
                                                                                as_index=False).size()
    relation_infraction_source = ided_data[["infraction_id", "source_id"]].groupby(["infraction_id", "source_id"],
                                                                                   as_index=False).size()
    relation_infraction_species = ided_data[["infraction_id", "species_id"]].groupby(["infraction_id", "species_id"],
                                                                                     as_index=False).size()
    relation_infraction_country_origin = ided_data[["infraction_id", "countries_origin_id"]].groupby(
        ["infraction_id", "countries_origin_id"], as_index=False).size()
    relation_infraction_country_dest = ided_data[["infraction_id", "countries_dest_id", "Date"]].groupby(
        ["infraction_id", "countries_dest_id"], as_index=False).size()

    # Create graph

    G = nx.Graph()

    # Adding nodes

    for row in items.itertuples():
        G.add_node(row.item_id,
                   Item=row.Item,
                   Count=row.Count,
                   label="Item")
    for row in infraction.itertuples():
        G.add_node(row.infraction_id,
                   Category=row.Category,
                   Order_in_Trade_Route=row.Order_in_Trade_Route,
                   Outcome=row.Outcome,
                   Year=row.Year,
                   Month=row.Month,
                   label="Infraction")
    for row in source.itertuples():
        G.add_node(row.source_id,
                   Source=row.Source_Type,
                   label="Source")
    for row in species.itertuples():
        G.add_node(row.species_id,
                   Scientific_Name=row.Scientific_Name,
                   Kingdom=row.Kingdom,
                   Phylum=row.Phylum,
                   Class=row.Class,
                   Order=row.Order,
                   Family=row.Family,
                   Genus=row.Genus,
                   Species=row.Species,
                   Common_Name=row.Common_Name,
                   Role=row.Role,
                   label="Species")
    for row in countries_origin.itertuples():
        G.add_node(row.countries_origin_id,
                   Country_origin=row.Country_origin,
                   Latitude_origin=row.Latitude_origin,
                   Longitude_origin=row.Longitude_origin,
                   Label="Country Origin")
    for row in countries_dest.itertuples():
        G.add_node(row.countries_dest_id,
                   Country_dest=row.Country_dest,
                   Region_dest=row.Region_dest,
                   City_dest=row.City_dest,
                   Latitude_dest=row.Latitude_dest,
                   Longitude_dest=row.Longitude_dest,
                   Label="Country destination")

    # Adding edges
    G.add_weighted_edges_from([(item.item_id, item.infraction_id, item.size) for item in relation_items_infraction.itertuples()])
    G.add_weighted_edges_from([(item.infraction_id, item.source_id, item.size) for item in relation_infraction_source.itertuples()])
    G.add_weighted_edges_from([(item.infraction_id, item.species_id, item.size) for item in relation_infraction_species.itertuples()])
    G.add_weighted_edges_from([(item.infraction_id, item.countries_origin_id, item.size) for item in relation_infraction_country_origin.itertuples()])
    G.add_weighted_edges_from([(item.infraction_id, item.countries_dest_id, item.size) for item in relation_infraction_country_dest.itertuples()])

    # Create metrics
    clustering = nx.algorithms.clustering(G)
    nx.set_node_attributes(G, clustering, "community")
    community_louvain = community_louvain.best_partition(G)
    nx.set_node_attributes(G, community_louvain, "community_louvain")
    jaccard_coefficient = nx.jaccard_coefficient(G)
    nx.set_node_attributes(G, jaccard_coefficient, "jaccard_coefficient")
    degree_centrality = nx.degree_centrality(G)
    nx.set_node_attributes(G, degree_centrality, "degree_centrality")
    pageRank = nx.algorithms.pagerank(G)
    nx.set_node_attributes(G, pageRank, "pageRank")
    harmonic_function = nx.algorithms.node_classification.harmonic_function(G)
    nx.set_node_attributes(G, harmonic_function, "harmonic_function")
    greedy_community = nx.algorithms.community.greedy_modularity_communities(G)
    nx.set_node_attributes(G, greedy_community, "greedy_community")

    return G