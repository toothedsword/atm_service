curl -X POST http://localhost:5001/api/ppi_latest \
  -F "files=@/home/leon/src/atm-radar/input/20260415/level0/Z_RADR_I_WU7-01_0912_P_CDWL_MCXG_DATA.csv" \
  -F "files=@/home/leon/src/atm-radar/input/20260415/level0/Z_RADR_I_WU7-01_0913_P_CDWL_MCXG_DATA.csv" \
  -F "files=@/home/leon/src/atm-radar/input/20260415/level0/Z_RADR_I_WU7-01_0914_P_CDWL_MCXG_DATA.csv" \
  -o ppi_latest.zip
