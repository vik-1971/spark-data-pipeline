@echo off
echo.
echo Запуск ETL-пайплайна на PySpark...
echo ======================================
echo.
docker run -it --rm ^
  -v "%cd%:/work" ^
  --entrypoint="" ^
  apache/spark ^
  /opt/spark/bin/spark-submit ^
  /work/src/etl_pipeline.py ^
  /work/data/data.csv ^
  /work/output

echo.
echo ETL завершён.
echo Результаты в папке output.
echo.
pause