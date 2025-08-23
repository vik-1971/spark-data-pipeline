```powershell
docker run -it --rm `
  -v "${PWD}:/work" `
  --entrypoint="" `
  apache/spark `
  /opt/spark/bin/spark-submit `
  /work/src/etl_pipeline.py