curl -X POST http://localhost:5001/api/slice \
  -H "Content-Type: application/json" \
  -d @slice_config_example.json \
  -o slice_plot.png
