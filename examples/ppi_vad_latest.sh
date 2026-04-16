curl -X POST http://localhost:5001/api/ppi_vad_latest \
  -H "Content-Type: application/json" \
  -d '{
    "files": [
      "/home/leon/src/atm-radar/input/20260415/level0/Z_RADR_I_WU7-01_0912_P_CDWL_MCXG_DATA.csv",
      "/home/leon/src/atm-radar/input/20260415/level0/Z_RADR_I_WU7-01_0913_P_CDWL_MCXG_DATA.csv"
    ]
  }' \
  -o ppi_vad_latest.zip
