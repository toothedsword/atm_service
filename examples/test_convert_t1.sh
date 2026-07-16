curl -X POST http://172.17.0.7:5001/api/convert-time \
  -F "file=@/home/leon/welink/dld/temp_json_e9416b53f9ba4575845911ab4c96d7724184710517740325585.json" \
  -F "minLat=40.0" \
  -F "maxLat=41.0" \
  -F "minLon=115.5" \
  -F "maxLon=116.8" \
  -o /tmp/output.zip


