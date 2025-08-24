# Spark Data Pipeline Example
![Python](https://img.shields.io/badge/Python-3.x-blue)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5-red)
![Docker](https://img.shields.io/badge/Docker-Supported-blue)
![PySpark](https://img.shields.io/badge/PySpark-Ready-green)
## 📊 Ноутбук
![Jupyter Notebook](https://img.shields.io/badge/Jupyter_Notebook-F37626?logo=jupyter&logoColor=white)

Простой ETL-пайплайн на PySpark с визуализацией.

## Этапы
1. Чтение CSV
2. Очистка данных
3. Агрегация: средняя зарплата по отделам
4. Визуализация

## Как запустить

### 1. ETL (PowerShell)
```powershell
docker run -it --rm `
  -v "${PWD}:/work" `
  --entrypoint="" `
  apache/spark `
  /opt/spark/bin/spark-submit `
  /work/src/etl_pipeline.py

