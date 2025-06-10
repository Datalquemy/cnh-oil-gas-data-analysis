# 🛢️ CNH Oil & Gas Data Analysis

Este proyecto realiza un análisis exploratorio de datos abiertos de la Comisión Nacional de Hidrocarburos (CNH), transformando datos crudos en insights clave usando PySpark en Databricks.

## 📌 Objetivo
Identificar patrones de producción de petróleo y gas en México, resaltar campos y operadores líderes, y preparar la base para futuros modelos de Machine Learning.

## ⚙️ Tecnologías utilizadas
- Apache Spark (PySpark)
- Databricks Community Edition
- Python 3
- Delta Lake (Parquet)
- GitHub

## 🧪 Estructura del proyecto

```
📁 cnh-oil-gas-data-analysis/
├── 01_cnh_pipeline_oro.py       # Script principal del pipeline
├── README.md
├── images/                      # Capturas e infografías
│   └── cnh_insights_summary.png
```

## 📊 Principales hallazgos

- AKAL lidera la producción histórica de petróleo y gas.
- PEMEX sigue siendo el operador dominante.
- Los nuevos operadores no han alcanzado el impacto esperado.
- La producción muestra una tendencia descendente sostenida.

## 🤖 Siguientes pasos con Machine Learning

- Predicción de declive de producción.
- Detección de oportunidades en campos secundarios.
- Recomendaciones para inversión y mantenimiento preventivo.

## 📥 Dataset

Datos descargados directamente desde la [CNH - Datos Abiertos](https://datos.gob.mx/busca/organization/cnh).

## Datasets

Los datasets los encuentras en este mismo repositorio, uno para gas y uno para aceite

## Databricks

Use Databricks Community para trabajar la transformación de los datos 
https://community.cloud.databricks.com/
