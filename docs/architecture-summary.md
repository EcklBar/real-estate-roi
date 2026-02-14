# הנדל"ניסט החכם -- סיכום מה שנבנה עד כה

## מה הפרויקט?

פלטפורמת Data Engineering שאוספת נתוני נדל"ן ישראלי ממקורות שונים, מעבדת אותם, ומציגה תובנות בדשבורד. הכל רץ מקומית על Docker.

## קובץ 1: docker-compose.yml (233 שורות)

קובץ אחד שמגדיר 16 containers שעולים בפקודה אחת (`docker compose up -d`). כולם רצים ברשת Docker משותפת ומדברים ביניהם בשמות (לא IP).

### שכבת אחסון -- MinIO

```
minio           → שרת S3 מקומי. מאחסן קבצי JSON/Parquet
minio-init      → רץ פעם אחת, יוצר 2 buckets:
                   nadlanist-raw   = נתונים גולמיים מ-APIs
                   nadlanist-clean = נתונים אחרי עיבוד
```

**איך הם מחוברים:** `minio-init` ממתין ל-healthcheck של `minio` (בודק `mc ready local` כל 10 שניות). רק כש-MinIO בריא -- `minio-init` עולה, יוצר buckets, ויוצא.

**גישה מהמחשב שלך:** `localhost:9000` (API), `localhost:9001` (ממשק ויזואלי)

### שכבת Warehouse -- PostgreSQL + PostGIS

```
postgres-warehouse → בסיס הנתונים המרכזי. כאן יושב ה-Star Schema
                     PostGIS מופעל כדי לתמוך בשאילתות גיאוגרפיות
```

**איך הוא מאותחל:** בעלייה ראשונה, PostgreSQL מוצא את `init.sql` בתיקיית `docker-entrypoint-initdb.d/` ומריץ אותו אוטומטית. ככה הטבלאות נוצרות בלי התערבות ידנית.

**גישה מהמחשב שלך:** `localhost:5433` (לא 5432, כדי לא להתנגש עם PostgreSQL מקומי)

### שכבת Streaming -- Kafka + Zookeeper + Kafdrop

```
zookeeper → שירות תיאום שKafka צריך כדי לעבוד
kafka     → מתווך הודעות. producers שולחים מודעות מיד2/מדלן,
             consumers קוראים ומעבדים
kafdrop   → ממשק ויזואלי לצפייה ב-topics והודעות
```

**איך הם מחוברים:** Kafka מתחבר ל-Zookeeper בכתובת `zookeeper:2181`. Kafdrop מתחבר ל-Kafka בכתובת `kafka:29092` (listener פנימי).

**למה ל-Kafka יש 2 listeners?**
- `kafka:29092` -- עבור containers אחרים בתוך Docker
- `localhost:9092` -- עבורך, מהמחשב

**גישה:** `localhost:9092` (Kafka API), `localhost:9003` (Kafdrop UI)

### שכבת Compute -- Spark

```
spark-master → מנהל. מקבל jobs, מחלק משימות לworkers
spark-worker → עובד. מבצע את העיבוד בפועל (2GB RAM, 2 cores)
```

**איך הם מחוברים:** Worker מתחבר ל-Master בכתובת `spark://spark-master:7077`. שניהם רואים את אותה תיקיית `./processing` (bind mount).

**הפקודות:**
- Master מריץ: `org.apache.spark.deploy.master.Master`
- Worker מריץ: `org.apache.spark.deploy.worker.Worker spark://spark-master:7077`

**גישה:** `localhost:8081` (Spark UI)

### שכבת Orchestration -- Airflow

```
airflow-db        → PostgreSQL פנימי של Airflow (מטאדטה, היסטוריה)
airflow-init      → רץ פעם אחת: migrate DB + יצירת משתמש admin
airflow-webserver → ממשק ניהול DAGs
airflow-scheduler → כל שנייה בודק אם יש DAG שצריך לרוץ
```

**הטריק:** שלושת ה-Airflow services (init, webserver, scheduler) משתמשים ב-YAML anchor `&airflow-common` שמגדיר פעם אחת: image, environment, volumes, depends_on. כל service כותב `<<: *airflow-common` ומוסיף רק מה שייחודי לו.

**depends_on עם conditions:** Airflow ממתין שלושה services יהיו healthy לפני שעולה: `airflow-db`, `postgres-warehouse`, `minio`.

**גישה:** `localhost:8080` (user: admin, password: admin)

### שכבת Dashboard -- Streamlit

```
dashboard → דשבורד אינטראקטיבי. כרגע Dockerfile ריק (placeholder).
             ב-Lesson 5 נבנה אותו עם KPIs, גרפים ומפות.
```

**שונה משאר ה-services:** במקום `image:` יש `build:` -- Docker בונה image מקומי מ-`./dashboard/Dockerfile`.

**גישה:** `localhost:8501`

### שכבת Monitoring -- ELK (bonus)

```
elasticsearch → מנוע חיפוש logs
kibana        → ממשק ויזואלי ל-logs
```

**`profiles: - elk`** -- לא עולים כברירת מחדל. רק עם `docker compose --profile elk up -d`. חוסך זיכרון בזמן פיתוח.

### Volumes (בסוף הקובץ)

7 named volumes שמאחסנים נתונים של ה-containers על הדיסק. כש-container נעצר ועולה מחדש -- הנתונים נשמרים. רק `docker compose down -v` מוחק אותם.

---

## קובץ 2: init.sql (153 שורות)

רץ אוטומטית כש-PostgreSQL עולה בפעם הראשונה. מגדיר את כל מבנה ה-Data Warehouse.

### מה זה Star Schema?

במקום טבלה ענקית אחת עם כל העמודות, מפרידים ל:
- **Fact tables** -- אירועים (עסקאות, מדדים). שורות רבות, מספרים
- **Dimension tables** -- הקשר (איפה, מתי, מה). שורות מעטות, תיאורים

ה-fact table מפנה ל-dimensions דרך Foreign Keys. נקרא "כוכב" כי ה-fact באמצע.

### Dimension Tables (4)

**`dim_location`** -- איפה?
- עיר, שכונה, רחוב, מחוז
- קואורדינטות (`latitude`, `longitude`) + PostGIS point (`geom`)
- `UNIQUE(city, neighborhood, street)` -- מונע כפילויות

**`dim_property`** -- מה?
- סוג נכס, חדרים (3.5 אפשרי), קומה, שנת בנייה
- מעלית/חנייה/מרפסת (BOOLEAN)

**`dim_time`** -- מתי?
- `time_id` = YYYYMMDD (למשל 20250213). לא SERIAL -- אנחנו קובעים את ה-ID
- שנה, רבעון, חודש, שם חודש, יום בשבוע
- `is_post_covid` (אחרי מרץ 2020), `is_war_period` (אחרי 7.10.23)
- **מאוכלס מראש** עם `generate_series`: 5,844 ימים מ-2015 עד 2030

**`dim_macro_economic`** -- מה המצב הכלכלי?
- ריבית בנק ישראל, ריבית משכנתא ממוצעת
- שער דולר, מדד המחירים לצרכן, התחלות בנייה
- מקושר ל-`dim_time` דרך Foreign Key

### Fact Tables (3)

**`fact_transactions`** -- הטבלה המרכזית. כל שורה = עסקת נדל"ן אחת
- 3 Foreign Keys: `location_id`, `property_id`, `time_id`
- מחיר, שטח, מחיר למ"ר
- גוש/חלקה (מזהים קרקעיים ישראליים)
- ריבית ומדד ביום העסקה (denormalization לביצועים)
- כתובת גולמית לפני ניקוי (לדיבוג)
- `source DEFAULT 'nadlan.gov.il'` -- ישתנה כשנוסיף יד2/מדלן

**`fact_market_indices`** -- סיכום חודשי של מדדי שוק
- מדד מחירי דירות (למ"ס), מחיר ממוצע למ"ר, ריבית
- ברמת אזור + חודש (לא ברמת עסקה בודדת)

**`fact_roi_analysis`** -- ציון ROI לכל אזור
- עליית ערך ב-1 שנה ו-5 שנים
- תשואת שכירות משוערת
- `roi_score` (1-10): שורה תחתונה למשקיע
- מרחק לתחנת רכבת / בית ספר (מ-OpenStreetMap)

### Staging Table (1)

**`stg_nadlan_raw`** -- "חדר המתנה"
- `raw_json JSONB` -- ה-JSON כמו שהגיע מה-API, לפני כל עיבוד
- `is_processed` -- Spark ETL מסמן TRUE אחרי שטיפל בשורה
- אם ETL נכשל, הנתונים הגולמיים שמורים ואפשר להריץ מחדש

### Indexes (7)

משפרים ביצועי שאילתות על עמודות שנמצאות הרבה ב-WHERE וב-JOIN:
- B-tree indexes על `location_id`, `time_id`, `price`, `city`
- GIST index על `geom` (מיוחד ל-PostGIS, לשאילתות מרחק)
- DESC index על `roi_score` (ל-"Top 10 areas")

---

## איך הכל מתחבר

```
     שלך (Mac)                         Docker Network
  ┌────────────┐         ┌─────────────────────────────────────┐
  │            │  :9001  │  MinIO ──────── minio-init          │
  │  Browser   │─────────│    (raw + clean buckets)            │
  │            │  :8080  │                                     │
  │  Terminal  │─────────│  Airflow ─┬─ webserver               │
  │            │         │          ├─ scheduler               │
  │  Python    │  :5433  │          └─ airflow-db              │
  │  scripts   │─────────│                                     │
  │            │         │  PostgreSQL+PostGIS (warehouse)     │
  │            │  :9092  │                                     │
  │            │─────────│  Kafka ──── Zookeeper               │
  │            │  :9003  │    └─── Kafdrop                     │
  │            │  :8081  │                                     │
  │            │─────────│  Spark Master ──── Spark Worker     │
  │            │  :8501  │                                     │
  │            │─────────│  Streamlit Dashboard                │
  └────────────┘         └─────────────────────────────────────┘
```

---

## Service UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Airflow | http://localhost:8080 | admin / admin |
| Spark Master | http://localhost:8081 | -- |
| Kafdrop | http://localhost:9003 | -- |
| Dashboard | http://localhost:8501 | -- |
| Kibana (elk profile) | http://localhost:5601 | -- |
