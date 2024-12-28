from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, max

def main():
    # Spark-Session erstellen
    spark = SparkSession.builder \
        .appName("Tankstellen Analyse - Neue Fragen") \
        .config("spark.executor.heartbeatInterval", "20s") \
        .config("spark.network.timeout", "300s") \
        .config("spark.ui.port", "4050") \
        .getOrCreate()

    # Daten laden
    preise = spark.read.csv("Aufgabe 1 - OnPremise-Engines/data/prices.csv", header=True, inferSchema=True)
    stationen = spark.read.csv("Aufgabe 1 - OnPremise-Engines/data/stations.csv", header=True, inferSchema=True)

    # 1. Tankstelle mit den meisten Preisänderungen an einem Tag
    preis_aenderungen = preise.groupBy("date", "station_uuid").agg(
        count("dieselchange").alias("anzahl_aenderungen")
    )
    max_aenderungen = preis_aenderungen.orderBy(col("anzahl_aenderungen").desc()).first()
    beste_tankstelle = stationen.filter(col("uuid") == max_aenderungen["station_uuid"]).first()

    print(f"Die Tankstelle mit den meisten Preisänderungen ist {beste_tankstelle['name']} am {max_aenderungen['date']} mit {max_aenderungen['anzahl_aenderungen']} Änderungen.")
    preis_aenderungen.write.mode("overwrite").csv("output/preis_aenderungen", header=True)

    # 2. Durchschnittliche Preisänderung pro Tankstelle (mit Tankstellennamen)
    durchschnittliche_aenderung = preise.groupBy("station_uuid").agg(
        avg("dieselchange").alias("durchschnitt_aenderung")
    )
    durchschnittliche_aenderung = durchschnittliche_aenderung.join(stationen, col("station_uuid") == col("uuid"))
    max_station = durchschnittliche_aenderung.orderBy(col("durchschnitt_aenderung").desc()).first()

    print(f"Die Tankstelle mit der höchsten durchschnittlichen Preisänderung ist {max_station['name']} mit einem Durchschnitt von {max_station['durchschnitt_aenderung']:.2f}.")
    durchschnittliche_aenderung.write.mode("overwrite").csv("output/durchschnittliche_aenderung", header=True)

    # 3. Regionale Analyse: Höchste Durchschnittspreise pro Stadt
    preise_mit_stationen = preise.join(stationen, preise["station_uuid"] == stationen["uuid"])
    durchschnittspreise_stadt = preise_mit_stationen.groupBy("city").agg(
        avg("diesel").alias("durchschnittspreis")
    )
    teuerste_stadt = durchschnittspreise_stadt.orderBy(col("durchschnittspreis").desc()).first()

    print(f"Die Stadt mit den höchsten durchschnittlichen Dieselpreisen ist {teuerste_stadt['city']} mit einem Durchschnitt von {teuerste_stadt['durchschnittspreis']:.2f} Euro.")
    durchschnittspreise_stadt.write.mode("overwrite").csv("output/durchschnittspreise_stadt", header=True)

    # Spark-Session beenden
    spark.stop()

if __name__ == "__main__":
    main()
