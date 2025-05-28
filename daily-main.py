import os
import pandas as pd
import xarray as xr

# Path folder (sesuaikan dengan kebutuhan)
folder_single_level = r"\SINGLE\\"
folder_pressure_level = r"\PRESSURE\\"
output_folder = r"\HASIL\\"

# Jumlah tahun yang diproses per batch
years_per_batch = 5

def check_missing_values(data, label):
    """
    Memeriksa nilai NaN/missing data dan menampilkan informasi detail
    """
    missing_info = data.isna().sum()
    total_missing = missing_info.sum()

    if total_missing > 0:
        print(f"Peringatan: Terdapat missing values pada {label}")
        print("Detail missing values:")
        print(missing_info[missing_info > 0])
    else:
        print(f"Tidak ada missing values pada {label}")

def process_netcdf_by_year(folder_path, is_pressure_level, years):
    """
    Memproses file NetCDF untuk tahun-tahun tertentu
    """
    all_data = []
    for file in os.listdir(folder_path):
        if file.endswith(".nc"):
            file_path = os.path.join(folder_path, file)
            ds = xr.open_dataset(file_path)

            # Konversi waktu ke datetime
            ds['valid_time'] = pd.to_datetime(ds['valid_time'].values)

            # Filter data berdasarkan tahun
            ds = ds.sel(valid_time=ds['valid_time'].dt.year.isin(years))

            # Hapus variabel tidak perlu
            ds = ds.drop_vars(["expver", "number"], errors="ignore")

            # Konversi ke DataFrame
            df = ds.to_dataframe().reset_index()

            # Handle pressure level
            if is_pressure_level and "pressure_level" in df.columns:
                pressure_levels = df['pressure_level'].unique()
                for level in pressure_levels:
                    level_df = df[df['pressure_level'] == level].copy()
                    for var in ds.data_vars:
                        level_df.rename(columns={var: f"{var}_{int(level)}"}, inplace=True)
                    all_data.append(level_df.drop(columns=["pressure_level"]))
            else:
                all_data.append(df)

    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

def process_by_year_batches(start_year, end_year, years_per_batch):
    for batch_start in range(start_year, end_year + 1, years_per_batch):
        batch_end = min(batch_start + years_per_batch - 1, end_year)
        years = list(range(batch_start, batch_end + 1))
        print(f"Memproses tahun: {years}")

        # Proses data single level
        df_single = process_netcdf_by_year(folder_single_level, False, years)
        check_missing_values(df_single, "Data Single Level")

        # Proses data pressure level
        df_pressure = process_netcdf_by_year(folder_pressure_level, True, years)
        check_missing_values(df_pressure, "Data Pressure Level")

        # Gabungkan dataset
        combined_df = pd.merge(
            df_single, df_pressure,
            on=["latitude", "longitude", "valid_time"],
            how="outer"
        )

        # Penyesuaian kolom
        combined_df.rename(columns={
            "latitude": "lat",
            "longitude": "lon"
        }, inplace=True)

        # Penyesuaian variabel viwvd
        if "vimdf" in combined_df.columns:
            combined_df["vimfc"] = combined_df["vimdf"] * -1
            combined_df.drop(columns=["vimdf"], inplace=True)

        if "tp" in combined_df.columns:
            combined_df["tp_sum"] = combined_df["tp"] * 1000
            combined_df.drop(columns=["tp"], inplace=True)

        # Ekstrak komponen waktu
        combined_df['valid_time'] = pd.to_datetime(combined_df['valid_time'])
        combined_df['year'] = combined_df['valid_time'].dt.year
        combined_df['month'] = combined_df['valid_time'].dt.month
        combined_df['day'] = combined_df['valid_time'].dt.day  # Tambahan kolom hari

        # Kelompokkan berdasarkan tanggal
        grouped = combined_df.groupby(['year', 'month', 'day'])

        # Simpan per hari
        for (year, month, day), data in grouped:
            # Hapus kolom waktu dan rata-rata per lokasi
            clean_data = data.drop(columns=['valid_time', 'year', 'month', 'day'])
            averaged_data = clean_data.groupby(['lat', 'lon']).mean().reset_index()

            # Cek missing values
            check_missing_values(averaged_data, f"Data Harian {year}-{month:02d}-{day:02d}")

            # Simpan ke Excel
            output_file = os.path.join(
                output_folder,
                f"processed_era5jawa_{year}_{month:02d}_{day:02d}.xlsx"  # Format nama file harian
            )
            averaged_data.to_excel(output_file, index=False)
            print(f"File tersimpan: {output_file}")

# Jalankan proses
process_by_year_batches(1997, 1997, years_per_batch)
