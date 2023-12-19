import pandas as pd
import geopandas as gpd
import duckdb
import os
from shapely import wkt
from shapely.errors import ShapelyError

def get_table_names(conn):
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';"
    result = conn.execute(query).fetchall()
    return [table[0] for table in result]

def read_table_as_gdf(conn, table_name):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)

    # Convert the 'Geometry' column from WKT to a shapely object, handling errors
    if 'Geometry' in df.columns:
        def convert_geometry(row):
            try:
                return wkt.loads(row) if row else None
            except ShapelyError:
                return None

        df['Geometry'] = df['Geometry'].apply(convert_geometry)
        gdf = gpd.GeoDataFrame(df, geometry='Geometry')
    else:
        gdf = gpd.GeoDataFrame(df)

    return gdf

def export_table_to_geopackage(gdf, output_file):
    gdf.to_file(output_file, driver='GPKG')
    print(f"Exported to {output_file}")

def export_database_to_geopackage(database_path, output_directory):
    with duckdb.connect(database=database_path) as conn:
        table_names = get_table_names(conn)
        for table_name in table_names:
            gdf = read_table_as_gdf(conn, table_name)
            output_file = os.path.join(output_directory, f'{table_name}.gpkg')
            export_table_to_geopackage(gdf, output_file)

def main():
    database_path = 'Storage\\canadaserviceareas.duckdb'
    output_directory = 'Storage'

    export_database_to_geopackage(database_path, output_directory)

if __name__ == "__main__":
    main()
    