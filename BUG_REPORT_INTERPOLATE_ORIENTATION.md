# Bug: North-South Data Orientation in interpolate_zip_worker.py

## Problem

The `interpolate_and_save_tif()` function in `interpolate_zip_worker.py` writes GeoTIFF arrays **without flipping rows**, causing output files to be **vertically mirrored** relative to their declared geospatial coordinates.

### Impact
- GeoTIFFs from `/api/interpolate-zip-to-cogtiff` have inverted north-south orientation
- When viewed in GIS software (QGIS, etc.), the data appears upside-down
- Wind/precipitation patterns appear in wrong geographic locations
- Manual georeferencing or map overlay comparisons will show misalignment

### Root Cause
Line ~342 in `interpolate_zip_worker.py`:
```python
dst.write(interpolated_int16, 1)
```

This writes the array as-is, but the `from_bounds()` transform declares row 0 = north edge (lat_max = 33°). The meshgrid creates row 0 = lat_min (south). The two don't match.

## Solution

**Change the rasterio write statement (line ~342):**

```python
# OLD (incorrect):
dst.write(interpolated_int16, 1)

# NEW (correct):
dst.write(interpolated_int16[::-1, :], 1)
```

This flips the rows vertically so:
- Output row 0 = south (lat_min = 25.4°) ✓
- Output last row = north (lat_max = 34.4°) ✓
- Matches `from_bounds()` north-up declaration ✓

## Testing

After applying the fix:

1. **Create synthetic test** with north-south gradient:
   - North half (lat > 30.5): interpolated_data = 100
   - South half (lat ≤ 30.5): interpolated_data = 0

2. **Run interpolation** and verify with `gdalinfo`:
   ```bash
   gdalinfo output.tif | grep -A2 "Upper Left\|Lower Right"
   ```
   Should show:
   - Upper Left (NW corner) = ~33° latitude (north)
   - Lower Right (SE corner) = ~25° latitude (south)

3. **Read output TIFF** and confirm:
   - North pixels (row 0) have value 100 ✓
   - South pixels (last row) have value 0 ✓

## Reference

This fix is already correctly implemented in the new `wind_cogtiff_worker.py` (commit 2aac350):
```python
# Line ~440 in wind_cogtiff_worker.py
dst.write(wind_speed_int16[::-1, :], 1)   # Band 1 (flipped north-up)
dst.write(wind_dir_int16[::-1, :], 2)     # Band 2 (flipped north-up)
```

## Priority

**Important** — Affects data integrity of all existing `/api/interpolate-zip-to-cogtiff` outputs. Should be fixed before any dependent systems rely on georeferencing accuracy.

## Related

- Wind COG TIFF implementation (PR/commit: wind_cogtiff_worker.py) — demonstrates correct approach
- Design spec `2026-07-24-wind-cogtiff-design.md` — same georeferencing requirements, now correctly implemented
