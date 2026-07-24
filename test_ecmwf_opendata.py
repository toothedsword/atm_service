"""
Test script for ecmwf-opendata package.
Downloads ECMWF open data and processes it into grid format.
"""

import numpy as np
import xarray as xr
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors


def test_ecmwf_download_and_process():
    """Test downloading ECMWF data and converting to grid format."""
    try:
        from ecmwf.opendata import Client
    except ImportError:
        print("Installing ecmwf-opendata...")
        import subprocess
        subprocess.check_call(["pip", "install", "ecmwf-opendata"])
        from ecmwf.opendata import Client

    # Install cfgrib for reading GRIB files
    try:
        import cfgrib
    except ImportError:
        print("Installing cfgrib for GRIB format support...")
        import subprocess
        subprocess.check_call(["pip", "install", "cfgrib"])

    # Initialize ECMWF client
    client = Client()

    # Download latest available OPEN-IFS forecast data
    # Using correct API parameters for ecmwf-opendata
    request = {
        "source": "ifs",
        "date": -1,  # Latest available date
        "time": 0,
        "levtype": "sfc",
        "param": "2t",  # 2m temperature
        "step": 0
    }

    print(f"Downloading ECMWF data...")
    print(f"  Using IFS forecast data (latest available)")

    output_path = Path("ecmwf_data.grib")
    target = str(output_path)

    # Download data
    client.retrieve(request, target)
    print(f"✓ Data downloaded to {output_path}")

    # Load GRIB format as xarray Dataset for grid processing
    ds = xr.open_dataset(output_path, engine='cfgrib')
    print(f"✓ Dataset loaded successfully (GRIB format)")
    print(f"  Dimensions: {dict(ds.dims)}")
    print(f"  Variables: {list(ds.data_vars)}")

    # Access grid data (GRIB format uses different variable names)
    if 't2m' in ds.data_vars:
        var_name = 't2m'
    elif '2t' in ds.data_vars:
        var_name = '2t'
    else:
        var_name = list(ds.data_vars)[0]

    grid_data = ds[var_name].values

    print(f"✓ Grid data shape: {grid_data.shape}")
    print(f"  Data type: {grid_data.dtype}")
    print(f"  Value range: [{np.nanmin(grid_data):.2f}, {np.nanmax(grid_data):.2f}]")

    # Basic statistics
    print(f"\nGrid statistics:")
    print(f"  Mean: {np.nanmean(grid_data):.2f}")
    print(f"  Std:  {np.nanstd(grid_data):.2f}")

    # Access coordinates
    print(f"\nCoordinates:")
    for coord_name, coord_data in ds.coords.items():
        print(f"  {coord_name}: shape={coord_data.shape}")

    # Example: slice and process a region
    if 'latitude' in ds.coords and 'longitude' in ds.coords:
        # Get a regional subset (e.g., China region)
        ds_subset = ds.sel(
            latitude=slice(50, 15),  # North to South
            longitude=slice(75, 135)  # West to East
        )
        subset_grid = ds_subset[list(ds_subset.data_vars)[0]].values
        print(f"\nRegional subset (China region):")
        print(f"  Shape: {subset_grid.shape}")
        print(f"  Mean: {np.nanmean(subset_grid):.2f}")

    # Save processed grid as numpy binary for fast loading
    np.save("grid_data.npy", grid_data)
    print(f"\n✓ Grid data saved to grid_data.npy")

    return ds, grid_data


def plot_grid_data(ds, variable_name=None, title=None):
    """Visualize grid data with geographic coordinates."""
    if variable_name is None:
        variable_name = list(ds.data_vars)[0]

    data_var = ds[variable_name]

    # Create figure with multiple subplots
    fig = plt.figure(figsize=(16, 10))

    # Full global view
    ax1 = plt.subplot(2, 2, 1)
    if len(data_var.shape) == 3:
        plot_data = data_var.values[0]  # Take first time step
    else:
        plot_data = data_var.values

    im1 = ax1.imshow(plot_data, cmap='RdYlBu_r', aspect='auto')
    ax1.set_title(f'{variable_name} - Global Grid')
    ax1.set_xlabel('Longitude index')
    ax1.set_ylabel('Latitude index')
    plt.colorbar(im1, ax=ax1, label='Value')

    # Geographical plot if coordinates available
    ax2 = plt.subplot(2, 2, 2)
    if 'latitude' in ds.coords and 'longitude' in ds.coords:
        lats = ds.coords['latitude'].values
        lons = ds.coords['longitude'].values

        # Handle different coordinate orders
        if len(lats) > 1:
            im2 = ax2.contourf(lons, lats, plot_data, levels=20, cmap='RdYlBu_r')
        else:
            im2 = ax2.imshow(plot_data, extent=[lons[0], lons[-1], lats[0], lats[-1]],
                            cmap='RdYlBu_r', origin='upper', aspect='auto')

        ax2.set_title(f'{variable_name} - Geographic View')
        ax2.set_xlabel('Longitude (°E)')
        ax2.set_ylabel('Latitude (°N)')
        ax2.grid(True, alpha=0.3)
        plt.colorbar(im2, ax=ax2, label='Value')

    # Histogram distribution
    ax3 = plt.subplot(2, 2, 3)
    valid_data = plot_data[~np.isnan(plot_data)]
    ax3.hist(valid_data, bins=50, edgecolor='black', alpha=0.7, color='steelblue')
    ax3.set_title(f'{variable_name} - Value Distribution')
    ax3.set_xlabel('Value')
    ax3.set_ylabel('Frequency')
    ax3.grid(True, alpha=0.3)

    # Regional zoom (if coordinates available)
    ax4 = plt.subplot(2, 2, 4)
    if 'latitude' in ds.coords and 'longitude' in ds.coords:
        try:
            # China region
            ds_region = ds.sel(
                latitude=slice(55, 10),
                longitude=slice(70, 140)
            )
            region_data = ds_region[variable_name].values
            if len(region_data.shape) == 3:
                region_data = region_data[0]

            lats_region = ds_region.coords['latitude'].values
            lons_region = ds_region.coords['longitude'].values

            im4 = ax4.contourf(lons_region, lats_region, region_data,
                              levels=15, cmap='RdYlBu_r')
            ax4.set_title(f'{variable_name} - China Region')
            ax4.set_xlabel('Longitude (°E)')
            ax4.set_ylabel('Latitude (°N)')
            ax4.grid(True, alpha=0.3)
            plt.colorbar(im4, ax=ax4, label='Value')
        except Exception as e:
            ax4.text(0.5, 0.5, f'Region plot failed: {str(e)[:50]}',
                    ha='center', va='center', transform=ax4.transAxes)

    main_title = title or f'ECMWF Data Visualization - {variable_name}'
    fig.suptitle(main_title, fontsize=14, fontweight='bold')
    plt.tight_layout()

    return fig


def test_simple_forecast_download():
    """Test downloading forecast data with multiple parameters."""
    try:
        from ecmwf.opendata import Client
    except ImportError:
        print("Installing ecmwf-opendata...")
        import subprocess
        subprocess.check_call(["pip", "install", "ecmwf-opendata"])
        from ecmwf.opendata import Client

    try:
        import cfgrib
    except ImportError:
        import subprocess
        subprocess.check_call(["pip", "install", "cfgrib"])

    client = Client()

    # Download forecast data with multiple parameters
    request = {
        "source": "ifs",
        "date": -1,
        "time": 0,
        "step": [0, 3, 6],
        "levtype": "sfc",
        "param": ["2t", "10u", "10v"],  # 2m temp, 10m U and V wind components
    }

    print(f"Downloading forecast data with multiple steps...")
    output_path = Path("forecast_data.grib")

    try:
        client.retrieve(request, str(output_path))
        ds = xr.open_dataset(output_path, engine='cfgrib')
        print(f"✓ Forecast data downloaded")
        print(f"  Dimensions: {dict(ds.dims)}")
        print(f"  Variables: {list(ds.data_vars)}")
        return ds
    except Exception as e:
        print(f"⚠ Could not download forecast: {e}")
        return None


if __name__ == "__main__":
    print("=" * 60)
    print("ECMWF OpenData Test - Download & Grid Processing")
    print("=" * 60)

    try:
        print("\n[Test 1] Historical reanalysis data download:")
        print("-" * 60)
        ds, grid = test_ecmwf_download_and_process()

        # Visualize the downloaded data
        print("\n[Visualization] Generating plots...")
        fig = plot_grid_data(ds, title="ECMWF Historical Data Visualization")
        output_plot = Path("ecmwf_visualization.png")
        fig.savefig(output_plot, dpi=100, bbox_inches='tight')
        print(f"✓ Visualization saved to {output_plot}")
        plt.close(fig)

        print("\n[Test 2] Forecast data download:")
        print("-" * 60)
        ds_forecast = test_simple_forecast_download()

        if ds_forecast is not None:
            print("\n[Visualization] Generating forecast plots...")
            fig_forecast = plot_grid_data(ds_forecast, title="ECMWF Forecast Data Visualization")
            output_plot_forecast = Path("ecmwf_forecast_visualization.png")
            fig_forecast.savefig(output_plot_forecast, dpi=100, bbox_inches='tight')
            print(f"✓ Forecast visualization saved to {output_plot_forecast}")
            plt.close(fig_forecast)

        print("\n" + "=" * 60)
        print("✓ All tests completed successfully!")
        print(f"✓ Output files:")
        print(f"  - Grid data: grid_data.npy")
        print(f"  - GRIB data: ecmwf_data.grib")
        print(f"  - Visualization: ecmwf_visualization.png")
        if Path("forecast_data.grib").exists():
            print(f"  - Forecast data: forecast_data.grib")
            print(f"  - Forecast visualization: ecmwf_forecast_visualization.png")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
