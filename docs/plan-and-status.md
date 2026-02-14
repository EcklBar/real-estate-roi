# נדלניסט — תוכנית וסטטוס

מסמך אחד שמראה **לפי התוכנית** מה הושלם ומה נשאר. מתבסס על `architecture-summary.md` והמבנה הקיים.

---

## 1. תשתית (Docker + שירותים)

| פריט | סטטוס | הערות |
|------|--------|--------|
| docker-compose: MinIO + init buckets | ✅ | nadlanist-raw, nadlanist-clean |
| PostgreSQL + PostGIS (warehouse) | ✅ | פורט 5433, init.sql רץ בהפעלה |
| Kafka + Zookeeper + Kafdrop | ✅ | פורט 9092, Kafdrop 9003 |
| Spark Master + Worker | ✅ | פורט 8081 |
| Airflow (DB, init, webserver, scheduler) | ✅ | build מ-./airflow, env ל-MinIO/Warehouse/Kafka/Dirobot |
| Dashboard (Streamlit) | ✅ | build מ-./dashboard, פורט 8501 |
| ELK (Elasticsearch + Kibana) | ✅ | profile `elk`, לא עולה כברירת מחדל |

---

## 2. סכמת נתונים (Warehouse)

| פריט | סטטוס | הערות |
|------|--------|--------|
| init.sql — dim_location, dim_property, dim_time, dim_macro_economic | ✅ | |
| init.sql — fact_transactions, fact_market_indices, fact_roi_analysis | ✅ | |
| init.sql — stg_nadlan_raw | ✅ | |
| init.sql — indexes (B-tree, GIST) | ✅ | |

---

## 3. צינור Batch (ממשלה / דירובוט)

| פריט | סטטוס | הערות |
|------|--------|--------|
| Extract: gov_api — cities-summary, ערים דינמיות | ✅ | get_city_names_from_summary() |
| Extract: gov_api — שכונות, timeseries, רחובות (streets-summary) | ✅ | כל הרחובות לכל עיר |
| Extract: שמירה ל-MinIO (nadlanist-raw) | ✅ | קבצי JSON |
| ETL: PySpark — קורא מ-MinIO, טרנספורמציות | ✅ | processing/etl_transactions.py |
| ETL: Load ל-PostgreSQL (Star Schema) | ✅ | JDBC, dim_*, fact_transactions |
| DAG: fetch_dirobot_data >> run_etl (שבועי) | ✅ | dag_batch_pipeline.py |

---

## 4. צינור Streaming (יד2 + מדלן)

| פריט | סטטוס | הערות |
|------|--------|--------|
| Producer: Yad2 — גריפה, שליחה ל-Kafka topic `new_listings` | ✅ | yad2_producer.py |
| Producer: Madlan — גריפה, parsing (מחיר/חדרים/עיר), שליחה ל-Kafka | ✅ | madlan_producer.py, תיקוני parsing |
| DAG: Yad2 streaming (תדירות) | ✅ | dag_yad2_streaming.py |
| DAG: Madlan streaming (תדירות) | ✅ | dag_madlan_streaming.py |
| Consumer: קורא מ-Kafka, מעבד ומדפיס | ✅ | listings_consumer.py |
| Consumer: שמירה ל-DB או MinIO | ❌ | כרגע רק print — **נשאר** |

---

## 5. דשבורד

| פריט | סטטוס | הערות |
|------|--------|--------|
| Streamlit app, חיבור ל-PostgreSQL | ✅ | dashboard/app.py |
| מסננים: עיר (מ-fact_transactions), טווח תאריכים (dim_time) | ✅ | |
| KPIs: Total Deals, Avg/Min/Max Price | ✅ | |
| גרף התפלגות מחירים | ✅ | |
| טבלת עסקאות | ✅ | |
| גרף ממוצע מחיר לאורך זמן | ✅ | |
| הצגת מודעות (listings) מהסטרימינג | ❌ | תלוי ב-Consumer→DB — **נשאר** |

---

## 6. נושאים שלא הושלמו (לפי סדר עדיפות)

1. **Consumer → אחסון**  
   לשמור listings מ-Kafka ב-PostgreSQL (טבלה ייעודית) או ב-MinIO. אחרי זה אפשר לחבר בדשבורד.

2. **טסטים**  
   Unit tests ל-parsing (Madlan), ל-gov_api, ל-ETL (אופציונלי אבל מומלץ).

3. **תיעוד**  
   README: איך להריץ (`docker compose up -d`), משתני סביבה, אילו DAGs, איך להריץ producer/consumer ידנית.

4. **אופציונלי / להמשך**  
   - מקורות נוספים (CBS, בנק ישראל)  
   - ELK: שליחת לוגים ל-Elasticsearch  
   - Deduplication ב-consumer (listing_id + source)  
   - Retry/error handling ב-producers

---

## סיכום מהיר

| אזור | הושלם | נשאר |
|------|--------|--------|
| תשתית | כל השירותים | — |
| סכמה | כל הטבלאות | — |
| Batch | Extract → MinIO → ETL → Warehouse + DAG | — |
| Streaming | Producers + DAGs + Consumer (הדפסה) | Consumer→DB/MinIO |
| דשבורד | KPIs, גרפים, טבלאות (עסקאות) | צפייה במודעות (אחרי Consumer→DB) |
| איכות/תיעוד | — | טסטים, README |

**הצעד הבא הממוקד:** Consumer שיכתוב listings ל-PostgreSQL (טבלת `stream_listings` או דומה), ואז לחבר את הדשבורד למודעות אם תרצה.
