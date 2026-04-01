curl -X POST http://localhost:5001/api/convert-surface \
  -F "file=@/home/leon/welink/dld/vs.json" \
  -F "minLat=40.0" \
  -F "maxLat=41.0" \
  -F "minLon=115.5" \
  -F "maxLon=116.8" \
  -o /tmp/output.zip
