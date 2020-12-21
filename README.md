# ETL-SCD-prototype
Prototypische ETL-pipeline zur Demonstration der SCD-Typen 1, 2 und 3 nach Kimball, implementiert in Python

Zum Testen das Jupyter-Notebook aufrufen!

Die Ordner haben folgende Inhalte:
./DatabaseMeta.py:          Python-Klasse DatabaseMeta
./ETL.py:                   Python-Klasse ETLPipeline
./SCD_Projekt.ipynb:        Jupyter-Notebook zum Testen der Software
./TestDataGenerator.py:     Python-Klasse TestDataGenerator
./dim_data/:                Ausgangsdaten der Dimensionen für die Erstbefüllung der Dimensionen
    ./dim_data/XXX.csv:                 Datensätze für die Dimension XXX
./dim_data_drops/:          Inhalt der Dimensionstabellen (ausgegeben durch dump_dim_data() in Jupyter-NB)
./test_data/:               All weiteren Testdaten
    ./test_data/XXX_neu.csv:            Neue Datensätze für die Dimension XXX
    ./test_data/XXX_changes.csv:        Änderungsdatensätze für die Dimension XXX
    ./test_data/XXX_original.csv:       Ausgangsdaten in der Dimension XXX für TestDataGenerator
    ./test_data/personal_mapping.csv:   Mapping-Tabelle für die neuen Personalcodes  
    ./test_data/fact_data/:             Durch TestDataGenerator erzeugte Faktendaten
