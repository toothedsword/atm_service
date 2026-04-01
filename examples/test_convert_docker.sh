curl -X POST http://172.17.0.2:5001/api/convert \
  -F "file=@/home/leon/Downloads/vsfc.json" \
  -F "minLat=40.0" \
  -F "maxLat=41.0" \
  -F "minLon=115.5" \
  -F "maxLon=116.8" \
  -o /tmp/output.zip
