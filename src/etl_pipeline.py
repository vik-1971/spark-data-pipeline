# etl_pipeline.py
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, current_date, months_between, round as spark_round

# Настройка логгера
import logging
# Убедимся, что путь к логам — в смонтированной директории
log_file = "/work/logs/app.log"

# Попробуем создать папку, если её нет (в контейнере)
import os
os.makedirs("/work/logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)  # чтобы в Docker-логах было нормально
    ]
)


logger = logging.getLogger(__name__)

def main():
    # Получаем аргументы
    input_path = sys.argv[1] if len(sys.argv) > 1 else "data/data.csv"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "output"

    logger.info(f"Запуск ETL-пайплайна")
    logger.info(f"Вход: {input_path}")
    logger.info(f"Выход: {output_path}")

    # Инициализация Spark
    spark = SparkSession.builder \
        .appName("ImprovedETL") \
        .master("local[*]") \
        .getOrCreate()

    try:
        # 1. Чтение данных
        logger.info("Чтение CSV...")
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        logger.info(f"Прочитано {df.count()} строк")

        # 2. Очистка
        logger.info("Очистка данных...")
        clean_df = (
            df
            .filter(col("name").isNotNull() & (col("name") != ""))
            .filter(col("salary").isNotNull() & (col("salary") > 0))
            .withColumn("join_date", col("join_date").cast("date"))
            .filter(col("join_date").isNotNull())
        )
        logger.info(f"Осталось {clean_df.count()} строк после очистки")

        # 3. Добавим стаж (в годах)
        logger.info("Расчёт стажа...")
        with_tenure = clean_df.withColumn(
            "tenure_years",
            spark_round(months_between(current_date(), col("join_date")) / 12, 2)
        )

        # 4. Агрегация: средняя зарплата и стаж по отделу
        logger.info("Агрегация данных...")
        result = (
            with_tenure
            .groupBy("department")
            .agg(
                avg("salary").alias("avg_salary"),
                avg("tenure_years").alias("avg_tenure"),
                count("*").alias("employee_count")
            )
            .orderBy("department")
        )

        # 5. Сохранение по слоям
        logger.info("Сохранение результатов...")

        df.write.mode("overwrite").parquet(f"{output_path}/raw")
        clean_df.write.mode("overwrite").parquet(f"{output_path}/clean")
        result.write.mode("overwrite").parquet(f"{output_path}/aggregated")

        logger.info(f"ETL завершён. Результаты в {output_path}/aggregated")

        # Покажем результат
        print("\n" + "="*50)
        print("ИТОГОВАЯ СТАТИСТИКА ПО ОТДЕЛАМ")
        print("="*50)
        result.show(truncate=False)

    except Exception as e:
        logger.error(f"Ошибка в пайплайне: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()