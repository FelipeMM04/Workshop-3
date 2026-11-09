# 🌍 Taller 3: Streaming ETL con Apache Kafka y Machine Learning

Este repositorio contiene la **Parte A** del proyecto: Ingesta, Limpieza de Datos, Feature Engineering y Entrenamiento del Modelo de Regresión para predecir el puntaje de felicidad (*Happiness Score*).

---

## 📌 PARTE A: Data Profiling y Machine Learning

### Step 1 — Exploratory Data Analysis (EDA)

Realizamos el análisis exploratorio de los archivos CSV históricos del *World Happiness Report* (2015 a 2019) guardados en `data/raw/`.

#### Observaciones de Calidad de Datos (Data Quality Observations):
1. **Diferencias de Esquema (Schema Differences):** Las columnas varían significativamente entre años. Por ejemplo, la columna de país se llama `Country` (2015-2017) vs `Country or region` (2018-2019). El target cambia entre `Happiness Score`, `Happiness.Score` y `Score`.
2. **Valores Nulos (Missing Values):** No se detectaron valores nulos en 2015, 2016, 2017 ni 2019. En **2018** se identificó **1 valor nulo** en la columna `Perceptions of corruption`.
3. **Registros Duplicados (Duplicated Records):** 0 filas duplicadas exactas en todos los datasets.
4. **Tipos de Datos e Inconsistencias (Data Types):** Todos los valores numéricos están en formato flotante (`float64`) y los nombres de los países en texto (`str`).
5. **Outliers:** Las variables numéricas se mantienen dentro de los rangos metodológicos esperados (ej. `Score` entre ~2.69 y ~7.77). Los valores mínimos de `0.0` responden a la escala de países en la cola del informe.

#### Propuesta de Esquema Unificado (Unified Schema Proposal):

| Columna Estandarizada | Descripción | Mapeo Original (2015-2017 / 2018-2019) | Tipo de Dato |
| :--- | :--- | :--- | :--- |
| `country` | Nombre del país | `Country` / `Country or region` | `str` |
| `year` | Año del registro | Extraído del metadato del archivo | `int` |
| `actual_happiness_score` | Puntaje de felicidad (Target) | `Happiness Score` / `Happiness.Score` / `Score` | `float` |
| `gdp` | PIB per cápita | `Economy (GDP per Capita)` / `GDP per capita` | `float` |
| `family` | Apoyo social | `Family` / `Social support` | `float` |
| `health` | Esperanza de vida | `Health (Life Expectancy)` / `Healthy life expectancy` | `float` |
| `freedom` | Libertad de tomar decisiones | `Freedom` / `Freedom to make life choices` | `float` |
| `generosity` | Generosidad | `Generosity` | `float` |
| `corruption` | Percepción de corrupción | `Trust (Government Corruption)` / `Perceptions of corruption` | `float` |

---

### Step 2 — Data Cleaning and Harmonization

Diseñamos un pipeline de limpieza y consolidación de datos:

- **Estandarización de columnas:** Renombramos todas las variables de los 5 archivos al esquema unificado.
- **Manejo de valores nulos:** Se imputó el único valor nulo de `corruption` en 2018 utilizando la **mediana** de ese mismo año para conservar el tamaño muestral sin sesgar la distribución.
- **Resolución de inconsistencias:** Eliminamos columnas secundarias no homogéneas (rangos, intervalos de confianza, errores estándar) para mantener un esquema homogéneo de 9 columnas.
- **Unificación:** Concatenamos los 5 datasets agregando la columna `year` y exportamos el archivo estandarizado a `data/processed/cleaned_happiness.csv` (**782 filas, 9 columnas**).

---

### Step 3 — Feature Engineering

Preparación de las características para el modelo predictivo:

- **Selección de Features:** `gdp`, `family`, `health`, `freedom`, `generosity`, `corruption`.
- **Variable Objetivo (Target):** `actual_happiness_score`.
- **Prevención de Target Leakage:** Excluimos identificadores categóricos (`country`, `year`) y posiciones de ranking (`Rank`).
- **Análisis Estadístico y Correlación:** Evaluamos la matriz de correlación entre las variables independientes y el target, confirmando relaciones positivas relevantes en PIB, apoyo familiar y esperanza de vida.
- **Escalamiento:** Utilizamos `StandardScaler` de scikit-learn dentro de un `Pipeline` para normalizar las características sin provocar fugas de datos (*data leakage*) entre entrenamiento y prueba.

---

### Step 4 — Train Regression Model

Entrenamiento, evaluación y serialización del modelo de Machine Learning:

- **División de datos:** 70% entrenamiento (**547 registros**) y 30% prueba (**235 registros**), utilizando `test_size=0.30` y `random_state=42`.
- **Algoritmo:** `RandomForestRegressor(n_estimators=100, random_state=42)`.
- **Pipeline Reproducible:** Encapsulamos el preprocesamiento (`StandardScaler`) y el modelo dentro de un `Pipeline` de scikit-learn.
- **Métricas de Evaluación Obtencidas:**
  - **MAE (Mean Absolute Error):** `0.4069`
  - **RMSE (Root Mean Squared Error):** `0.5239`
  - **R² (R-Squared):** `0.7802`
- **Artefacto Serializado:** Se guardó el modelo compilado completo en `models/model.pkl`.

---

### Step 5 — Kafka Producer & Event Contract

Implementamos el script `kafka/producer.py` encargado de simular la ingesta de eventos en streaming hacia Apache Kafka en Docker.

#### Características del Producer:
1. **Creación del Topic:** Crea y valida el topic `happiness-events` con **3 particiones** y un **factor de replicación de 1**.
2. **Event Key:** Transmite cada registro utilizando el nombre del país (`country`) como clave (*key*), garantizando el particionamiento dinámico y la preservación del orden por país.
3. **Simulación de Streaming:** Envía los registros uno a uno desde `data/processed/cleaned_happiness.csv` con un retardo configurable (`DELAY_SECONDS = 0.5`).
4. **Serialización JSON:** Cada evento cumple estrictamente con el esquema JSON exigido:
   ```json
   {
     "event_id": "E001",
     "country": "Colombia",
     "year": 2019,
     "gdp": 1.2,
     "family": 0.8,
     "health": 0.9,
     "freedom": 0.6,
     "generosity": 0.3,
     "corruption": 0.1,
     "actual_happiness_score": 6.2,
     "event_time": "2026-11-07T10:58:24"
   }

#### Event Contract (`kafka/event_contract.py`):
Módulo encargado de validar las reglas de negocio antes de procesar la inferencia:
- Verificación de tipos de datos, presencia de campos obligatorios y formato ISO-8601 en `event_time`.
- Garantiza que `actual_happiness_score` no sea utilizado como feature dentro del modelo.

---

### Step 6 — Kafka Consumer & Real-Time Inference

Implementamos `kafka/consumer.py`, perteneciente al grupo consumidor `happiness-ml-team`, para la ingesta, auditoría, inferencia y persistencia en tiempo real.

#### Flujo de Procesamiento del Consumer:
1. **Auditoría RAW:** Guarda la carga útil (*payload*) original del evento en la tabla de PostgreSQL `raw_happiness_events` **antes de cualquier transformación**, registrando metadatos de Kafka (partición, offset) y asignando su estado (`VALID`, `INVALID_SCHEMA`, `INVALID_VALUES`).
2. **Validación:** Evalúa el mensaje con `event_contract.py`. Si el evento es inválido, lo registra en `raw_happiness_events` con el motivo del fallo, lo descarta de la inferencia y **continúa la ejecución sin tumbar el pipeline**.
3. **Inferencia en Tiempo Real:** Para los eventos con estado `VALID`, extrae las 6 características en el orden estricto del entrenamiento, realiza la predicción llamando a `models/model.pkl` y calcula el error absoluto (`|actual - predicted|`).
4. **Persistencia Dimensional:** Inserta las métricas finales generadas en tiempo real en la tabla `fact_predictions`.

---

### Step 7 — Database Design

Diseñamos e implementamos un modelo dimensional analítico (esquema en estrella) en PostgreSQL para separar la capa de ingesta/auditoría (*Staging*) de la capa de análisis de predicciones (*Analytics*).

#### Arquitectura Dimensional (Star Schema)
![Diagrama Esquema en Estrella](star_schema_diagram.png)

#### 1. Capa RAW / Staging (`raw_happiness_events`)
Tabla encargada de preservar el payload JSON original enviado por Apache Kafka **antes de cualquier transformación o inferencia**, garantizando la trazabilidad y auditoría de los mensajes.
- **Campos principales:** `raw_id` (PK), `event_id`, `topic_name`, `kafka_partition`, `kafka_offset`, `payload` (JSONB), `processing_status` (`VALID`, `INVALID_SCHEMA`, `INVALID_VALUES`), `error_message`, `received_at`.

#### 2. Capa de Dimensiones (Lookup Tables)
- **`dim_country`:** Normaliza los países procesados para evitar la redundancia de texto y optimizar las agrupaciones por ubicación geográfica.
  - **Campos:** `country_id` (PK), `country_name` (UNIQUE), `created_at`.
- **`dim_date`:** Desglosa la temporalidad del evento permitiendo agregaciones analíticas por año, mes, día y día de la semana.
  - **Campos:** `date_id` (PK, formato `YYYYMMDD`), `full_date`, `year`, `month`, `day`, `day_of_week`.

#### 3. Capa de Hechos (`fact_predictions`)
Tabla central de almacenamiento de las predicciones en tiempo real. Contiene las métricas numéricas del modelo y las llaves foráneas (`FK`) que la relacionan con las tablas dimensionales y la capa RAW.
- **Campos principales:** `prediction_id` (PK), `raw_event_id`, `raw_id` (FK), `country_id` (FK), `date_id` (FK), `actual_score`, `predicted_score`, `prediction_error`, `prediction_timestamp`, `created_at`.

#### 4. Automatización en el Consumer (`kafka/consumer.py`)
El script de ingesta fue adaptado para gestionar el poblamiento relacional automático:
1. Inserta el evento raw en `raw_happiness_events` obteniendo su `raw_id`.
2. Busca o registra dinámicamente el país en `dim_country` obteniendo su `country_id`.
3. Parsea `event_time`, poblando `dim_date` para generar su correspondiente `date_id`.
4. Realiza la inferencia con el modelo serializado `model.pkl` e inserta los resultados finales en `fact_predictions` manteniendo integridad referencial total.

---

### Step 8 — Load Raw Events and Prediction Results

Implementamos y validamos la persistencia doble y la trazabilidad end-to-end de los eventos consumidos desde Apache Kafka hacia PostgreSQL.

#### 1. Requerimientos Cumplidos
- **Almacenamiento RAW:** Preservación del mensaje JSON original en `raw_happiness_events` conservando metadatos de mensajería (`topic`, `partition`, `offset`).
- **Persistencia Analítica:** Registro de `actual_score`, `predicted_score` y el cálculo en tiempo real de `prediction_error` ($|actual - predicted|$).
- **Trazabilidad Temporal y Causal:** Registro de `event_time` (marca temporal del evento en el origen) y `prediction_timestamp` (marca de tiempo de la inserción en base de datos).
- **Enlace Relacional Causal:** Vinculación directa mediante `raw_event_id` y `raw_id` (FK), garantizando que cada inferencia analítica pueda rastrearse hasta el payload exacto emitido por el Producer.

#### 2. Auditoría de Integridad de Datos
Mediante el script de verificación `kafka/verify_step8_traceability.py`, se auditó el 100% de la ingesta en PostgreSQL:
- **Total de predicciones analíticas:** 782
- **Total de eventos RAW enlazados:** 782 (100% de trazabilidad garantizada)

#### Evidencia de Auditoría en Terminal
![Evidencia de Ejecución Step 8](step8_evidencia.png)

---
