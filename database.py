import duckdb
import geopandas as gpd
import os
import glob
import tempfile
import zipfile


def load_and_join_tiers(base_directory, num_tiers):
    tier_gdfs = []
    zip_file_path = os.path.join(base_directory, 'CanadaServiceAreasTAB.zip')

    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        for tier in range(1, num_tiers + 1):
            tier_pattern = os.path.join(temp_dir, f'Tier{tier}_Niveau{tier}_*.tab')
            matching_files = glob.glob(tier_pattern)

            if not matching_files:
                print(f"No files found for pattern: {tier_pattern}")
                continue

            tier_file = matching_files[0]
            tier_gdf = gpd.read_file(tier_file)

            if tier > 1:
                tier_gdf = gpd.sjoin(tier_gdf, tier_gdfs[-1][['Service_Area_Zone_de_service', 'geometry']], 
                                     how="left", predicate='intersects')
                tier_gdf.rename(columns={'Service_Area_Zone_de_service_left': 'Service_Area_Zone_de_service',
                                         'Service_Area_Zone_de_service_right': f'Tier{tier - 1}_Service_Area_Zone_de_service'},
                                inplace=True)
            tier_gdfs.append(tier_gdf)
    return tier_gdfs

def table_exists(conn, table_name):
    # Query the DuckDB catalog to check if the table exists
    query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
    result = conn.execute(query).fetchall()
    return result[0][0] > 0

def create_and_populate_tables(conn, tier_gdfs):
    for tier, gdf in enumerate(tier_gdfs, start=1):
        table_name = f'Tier{tier}_Areas'
        schema = ', '.join([f"{col} VARCHAR" for col in gdf.columns if col != 'geometry'] + ['Geometry STRING'])
        if not table_exists(conn, table_name):
            conn.execute(f"CREATE TABLE {table_name} ({schema})")

        # Batch insert data
        rows_to_insert = []
        for index, row in gdf.iterrows():
            row_data = [str(row[col]) if col != 'geometry' else row['geometry'].wkt for col in gdf.columns]
            rows_to_insert.append(tuple(row_data))

        insert_query = f'INSERT INTO {table_name} VALUES ({", ".join(["?"] * len(gdf.columns))})'
        conn.executemany(insert_query, rows_to_insert)



def main():
    base_directory = 'Storage'
    database_path = os.path.join(base_directory, 'canadaserviceareas.duckdb')  # Change the database path
    num_tiers = 5  # Change this to the number of tiers you have
    # Load and join tiers
    tier_gdfs = load_and_join_tiers(base_directory, num_tiers)
    # Connect to the existing DuckDB database
    conn = duckdb.connect(database=database_path, read_only=False)
    try:
        # Create and populate tables
        create_and_populate_tables(conn, tier_gdfs)
        # Get some statistics
        num_tiers_processed = len(tier_gdfs)
        table_names = [f'Tier{tier}_Areas' for tier in range(1, num_tiers_processed + 1)]
        # Print statistics and sample database structure
        print("Data loading and table creation completed successfully.")
        print(f"Number of tiers processed: {num_tiers_processed}")
        print(f"Table names: {', '.join(table_names)}")

        if table_names:
            # Provide the structure of the first table
            sample_table_name = table_names[0]
            result = conn.execute(f"SELECT * FROM {sample_table_name} LIMIT 0")  # Limit 0 to get only the structure
            columns = result.description

            print(f"Structure of {sample_table_name} (columns):")
            for column_info in columns:
                column_name = column_info[0]
                column_data_type = column_info[1]
                print(f"{column_name}: {column_data_type}")

    finally:
        # Close the database connection
        conn.close()

if __name__ == "__main__":
    main()