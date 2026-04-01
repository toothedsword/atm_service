curl -X POST http://localhost:5001/api/convert-height \
  -F "file=@/home/leon/welink/dld/temp_json_c973f5c8814e48d5b518cbabca93887f3352566676311610612.json" \
  -F "minLat=40.0" \
  -F "maxLat=41.0" \
  -F "minLon=115.5" \
  -F "maxLon=116.8" \
  -o /tmp/output.zip
