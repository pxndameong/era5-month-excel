import os
import pandas as pd
import xarray as xr

# YOUR FOLDER PATH NETCDF
folder_single_level = r"\MONTHLY_DATA\SINGLELEVEL\\"
folder_pressure_level = r"\MONTHLY_DATA\PRESSURE\\"
output_folder = r"\HASIL EXCEL ERA5\\"

years_per_batch = 3
def check_missing_values(data, label, year=None, month=None):
    """
    Check for NaN/missing values in the data and print detailed information.
    """
    missing_info = data.isna().sum()
    total_missing = missing_info.sum()

    if total_missing > 0:
        print(f"Warning: Missing values detected in {label}.")
        if year and month:
            print(f"Year: {year}, Month: {month}")
        print("Details of missing values:")
        print(missing_info[missing_info > 0])
    else:
        print(f"No missing values in {label}.")


def process_netcdf_by_year(folder_path, is_pressure_level, years):
    all_data = []
    for file in os.listdir(folder_path):
        if file.endswith(".nc"):
            file_path = os.path.join(folder_path, file)
            ds = xr.open_dataset(file_path)

            ds['valid_time'] = pd.to_datetime(ds['valid_time'].values)
            ds = ds.sel(valid_time=ds['valid_time'].dt.year.isin(years))
            ds = ds.drop_vars(["expver", "number"], errors="ignore")
            df = ds.to_dataframe().reset_index()

            if is_pressure_level and "pressure_level" in df.columns:
                pressure_levels = df['pressure_level'].unique()
                for level in pressure_levels:
                    level_df = df[df['pressure_level'] == level].copy()
                    for var in ds.data_vars:
                        level_df.rename(columns={var: f"{var}_{int(level)}"}, inplace=True)
                    all_data.append(level_df.drop(columns=["pressure_level"]))
            else:
                all_data.append(df)

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()

def process_by_year_batches(start_year, end_year, years_per_batch):
    for batch_start in range(start_year, end_year + 1, years_per_batch):
        batch_end = min(batch_start + years_per_batch - 1, end_year)
        years = list(range(batch_start, batch_end + 1))
        print(f"Processing years: {years}")

        df_single_level = process_netcdf_by_year(folder_single_level, is_pressure_level=False, years=years)
        check_missing_values(df_single_level, "Single Level Data")

        df_pressure_level = process_netcdf_by_year(folder_pressure_level, is_pressure_level=True, years=years)
        check_missing_values(df_pressure_level, "Pressure Level Data")

        combined_df = pd.merge(df_single_level, df_pressure_level, on=["latitude", "longitude", "valid_time"],
                               how="outer")
        combined_df.rename(columns={"latitude": "lat", "longitude": "lon"}, inplace=True)
        if "viwvd" in combined_df.columns:
            combined_df["vimfc"] = combined_df["viwvd"] * -1
            combined_df.drop(columns=["viwvd"], inplace=True)

        combined_df['valid_time'] = pd.to_datetime(combined_df['valid_time'])
        combined_df['year'] = combined_df['valid_time'].dt.year
        combined_df['month'] = combined_df['valid_time'].dt.month

        grouped = combined_df.groupby(['year', 'month'])
        for (year, month), data in grouped:
            data = data.drop(columns=['valid_time', 'year', 'month'])
            data = data.groupby(['lat', 'lon']).mean().reset_index() 

            check_missing_values(data, f"Processed Data for {year}-{month:02d}", year, month)
            output_file = os.path.join(output_folder, f"era5jawa_{year}_{month:02d}.xlsx")

            # Save to Excel
            data.to_excel(output_file, index=False)
            print(f"Saved: {output_file}")

# Jalankan pemrosesan dalam batch
process_by_year_batches(1985, 2024, years_per_batch)
