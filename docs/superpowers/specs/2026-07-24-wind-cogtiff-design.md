# Wind Speed & Direction COG TIFF Generation - Design Spec

**Date:** 2026-07-24  
**Feature:** Generate multi-channel COG TIFF files from WRF U10/V10 wind component data

## Overview

Create a new API endpoint `/api/wind-cogtiff` that accepts uploaded U (east-west) and V (north-south) wind component ZIP files, calculates wind speed and direction, and outputs multi-channel Cloud-Optimized GeoTIFF files (one file per time step per level, with 2 bands: wind speed + wind direction).

## Requirements

### Input
- Two ZIP files uploaded via multipart/form-data:
  - `u_file`: U wind component (e.g., WRF_REAL_..._u10_single_00000.zip)
  - `v_file`: V wind component (e.g., WRF_REAL_..._v10_single_00000.zip)
- Each ZIP contains binary data file (data.bin or xxx.dat format)
- Same structure: 4D array (times, levels, y, x) with JSON metadata header

### Processing
1. **Read both ZIPs** in parallel:
   - Extract 4D arrays for u and v components
   - Validate metadata consistency (times, levels, grid dimensions must match)

2. **Calculate wind properties** for each (time, level) combination:
   - **Wind Speed:** `sqrt(u² + v²)` → store directly as int16 (0-255 m/s, no scaling)
   - **Wind Direction:** meteorological convention (wind coming FROM direction)
     - Formula: `atan2(-u, -v) * 180/π` → 0-360° → store as int16
     - 0° = North wind, 90° = East wind, 180° = South wind, 270° = West wind

3. **Interpolate to target grid:**
   - Output bounds (hardcoded): longitude 105-111°E, latitude 28-33°N
   - Resolution: 0.002° (same as interpolate_zip_worker.py)
   - Result: 3000×2500 pixels per output file

4. **Generate multi-channel COG TIFF:**
   - Band 1: Wind Speed (int16, 0-255 m/s, no scaling)
   - Band 2: Wind Direction (int16, 0-360°)
   - Geospatial metadata: EPSG:4326, proper bounds transform
   - Compression: deflate
   - Cloud-optimized: tiled (512×512), with pyramids (gdaladdo)
   - File naming: `wind_{time}_{level}.tif`

5. **Output delivery:**
   - All TIFF files packaged into ZIP archive
   - Return as application/zip download

### Non-functional Requirements
- **Parallelization:** 2 ThreadPoolExecutor workers (same as fixed OOM bug)
- **Memory:** Peak ~1.2GB (2 workers × ~500MB interpolation arrays)
- **Error handling:** Partial failure mode (if one time/level fails, others continue)
- **Logging:** Detailed timing and progress per file

## Architecture

### Files to Create/Modify

**New file:** `wind_cogtiff_worker.py`
- Standalone CLI worker script (like `interpolate_zip_worker.py`)
- Accepts: `--u-input`, `--v-input`, `--output` arguments
- Reuses: `read_zip_data()` from `interpolate_zip_worker.py` (copy or import)
- New function: `save_wind_cogtiff(u_2d, v_2d, lon, lat, time_str, level_str, output_path)`

**Modify:** `run.py`
- Add `/api/wind-cogtiff` endpoint (POST, multipart/form-data)
- Upload validation: check both files are ZIP and not empty
- Worker dispatch: call `wind_cogtiff_worker.py` subprocess
- Response: return output ZIP with all TIFF files

### Data Flow

```
User upload (u_file, v_file)
    ↓
/api/wind-cogtiff endpoint (run.py)
    ↓
run_worker() subprocess → wind_cogtiff_worker.py
    ↓
read_zip_data(u_file) → u_4d, metadata
read_zip_data(v_file) → v_4d, metadata
    ↓
ThreadPoolExecutor (2 workers):
  for each (time, level):
    extract u_2d, v_2d slices
    calculate wind_speed, wind_dir
    interpolate to 105-111°E, 28-33°N grid
    save_wind_cogtiff() → multi-channel TIFF
    ↓
create output ZIP with all TIFF files
    ↓
return response to user
```

## Implementation Details

### Wind Calculation Formula

Replicate existing `_profile_uv_to_wind()` from run.py (line 1074):

```python
import math

def uv_to_wind(u, v):
    """u, v components → (wind_speed, wind_direction_deg)"""
    spd = math.sqrt(u * u + v * v)
    if spd < 1e-8:
        return 0.0, 0.0
    # atan2(-u, -v) gives meteorological direction (from which wind comes)
    direction = math.degrees(math.atan2(-u, -v)) % 360
    return spd, direction
```

### Interpolation & TIFF Output

**Target grid (hardcoded):**
- Bounds: lon_min=105.0, lon_max=111.0, lat_min=28.0, lat_max=33.0
- Resolution: 0.002°
- Grid size: 3000 × 2500 pixels

**Data type mapping:**
- Wind speed: stored as int16 (0-255 m/s, no scaling, direct truncation)
  - Formula: `wind_speed_int16 = int(wind_speed)`
- Wind direction: 0-360° → int16
- Handle NaN/missing values: nodata value = -9999

**Multi-channel rasterio output:**
```python
with rasterio.open(
    output_path, 'w',
    driver='GTiff',
    height=height, width=width,
    count=2,  # 2 bands
    dtype=np.int16,  # Both bands: int16
    crs='EPSG:4326',
    transform=transform,
    compress='deflate',
    tiled=True, blockxsize=512, blockysize=512
) as dst:
    dst.write(wind_speed_array.astype(np.int16), 1)   # Band 1: wind speed (0-255)
    dst.write(wind_dir_array.astype(np.int16), 2)     # Band 2: wind direction (0-360)
```

### File Naming Convention

- Output TIFF: `wind_{time}_{level}.tif`
  - time: from timeList in metadata (e.g., 1, 2, 3, ...)
  - level: from levelList in metadata (e.g., 0, 1, 2, ...)
  - Example: `wind_1_0.tif`, `wind_2_0.tif`

## Error Handling

1. **Input validation:**
   - Both files must be valid ZIP archives
   - Both must contain readable data (data.bin or .dat)
   - Metadata must match: same times, levels, grid dimensions

2. **Processing:**
   - If one time/level fails to process, log error and continue (partial success mode)
   - If ALL time/levels fail, return 500 error
   - If output ZIP generation fails, return 500 error

3. **Cleanup:**
   - Temporary directories cleaned up after response sent (delayed cleanup in background thread)

## Testing Strategy

### Unit Tests (for wind calculation)
- `test_uv_to_wind()`: verify formulas against known values
- `test_wind_direction_bounds()`: ensure 0-360° wrapping
- `test_wind_speed_scale()`: verify uint scaling

### Integration Tests
- Load actual u10/v10 ZIPs from `/home/leon/src/atm_service/tmp/data/`
- Generate output TIFF for single time/level
- Verify TIFF: 2 bands, correct bounds, correct nodata

### Manual Testing
- Upload u10 + v10 ZIPs via `/api/wind-cogtiff`
- Download output ZIP, inspect TIFF files with GDAL tools
- Verify visual: wind direction vs u/v components makes sense

## Constraints & Notes

1. **Fixed output bounds:** 105-111°E, 28-33°N (hardcoded, not configurable)
2. **Parallel workers:** Fixed at 2 (based on 8-core system, balances memory)
3. **Data types:** uint8/16 for wind speed, int16 for wind direction (user confirmed)
4. **Meteorological convention:** Wind direction = direction wind comes FROM (not goes TO)
5. **Reuse existing code:** Copy `read_zip_data()` or import from `interpolate_zip_worker.py`

## Success Criteria

✅ API endpoint `/api/wind-cogtiff` created and functional  
✅ Accepts two ZIP files (u, v components)  
✅ Generates multi-channel TIFF (wind speed + direction)  
✅ All time/level combinations processed  
✅ Output fits within 105-111°E, 28-33°N bounds  
✅ Output ZIP can be downloaded and TIFFs are valid COG format  
✅ No OOM errors (parallel memory management working)
