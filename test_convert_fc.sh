curl -X POST http://localhost:5001/api/convert-txt \
  -F "file=@/home/leon/welink/dld/temp_json_ce03a0cdc322412084c01f22c8672c773216305649321716444.json" \
  -F "minLat=40.0" \
  -F "maxLat=41.0" \
  -F "minLon=115.5" \
  -F "maxLon=116.8" \
  -o /tmp/output.zip


