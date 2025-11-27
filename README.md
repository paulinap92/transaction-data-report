# 📊 XYZ Sales Monitoring System  
### Real-Time Sales Analytics • ML-Based Anomaly Detection • Automated Reporting • Dockerized Architecture

---

## 🧰 Technology Stack

| Category | Technologies                                                                                                                              |
|---------|-------------------------------------------------------------------------------------------------------------------------------------------|
| **Backend** | ![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python) ![Flask](https://img.shields.io/badge/Flask-2.3-black?logo=flask)    |
| **Task Scheduling** | ![APScheduler](https://img.shields.io/badge/APScheduler-active-blue)                                                                      |
| **Data Processing** | ![Pandas](https://img.shields.io/badge/Pandas-2.0-purple?logo=pandas)                                                                     |
| **Machine Learning** | ![Scikit-Learn](https://img.shields.io/badge/IsolationForest-AnomalyDetection-orange?logo=scikitlearn)                                    |
| **Email Delivery** | ![Flask-Mail](https://img.shields.io/badge/Flask--Mail-active-green)                                                                      |
| **Networking** | ![NGINX](https://img.shields.io/badge/NGINX-Reverse%20Proxy-green?logo=nginx)                                                             |
| **Deployment** | ![Docker](https://img.shields.io/badge/Docker-Containerized-blue?logo=docker)                                                             |
| **Testing** | ![pytest](https://img.shields.io/badge/PyTest-7.4-green?logo=pytest) ![coverage](https://img.shields.io/badge/Coverage-95%25-brightgreen) |

---

# 🚀 Overview

The **XYZ Sales Monitoring System** provides automated, real-time monitoring and analysis of CSV-based sales files.

Features include:

- Automatic detection of new sales files  
- Pandas-based data processing  
- Machine-learning anomaly detection (Isolation Forest)  
- JSON + HTML sales report generation  
- Email report delivery  
- Web UI for viewing the report  
- Dockerized runtime architecture  

Flask runs directly inside the Docker container (no Gunicorn used).


# 🔬 Isolation Forest — How It Works

Isolation Forest is an unsupervised anomaly detection technique that isolates outliers by randomly splitting features.

### Why it works  
- Anomalies stand out — they are easier to isolate  
- Normal data requires more splits to isolate  

### Output  
IsolationForest returns:

```
1  → normal  
-1 → anomaly
```

This project converts it to:

```
0 = normal
1 = anomaly
```

### Why one model per category?

Each beverage category behaves differently — separate models produce better results:

```
AF → Functional Beverage model
AA → Carbonated Drink model
AC → Milkshake model
...
```

Models are stored in:

```
app/models/category_anomaly_models.pkl
```

as:

```python
{
    "Carbonated Drink": trained_model,
    "Milkshake": trained_model,
    ...
}
```

---

# 🖥️ Viewing the Report

### HTML Web Report

```
http://localhost/report
```

Shows:

- Mean sales by region  
- Sales per product category  
- 30-day analysis  
- Today’s anomalies  

### JSON API

```
http://localhost/api/report
```

Returns structured JSON:

```json
{
  "region_report_mean": {...},
  "beverage_report_total": {...},
  "today_anomalies": {...}
}
```

---
🍹### Product Category Encoding

Raw transaction files contain short ERP codes.
These are mapped to business domain names before ML processing.
```python
CATEGORY_MAP = {
    "AA": "Carbonated Drink",
    "AB": "Juice",
    "AC": "Milkshake",
    "AD": "Sports Drink",
    "AE": "Energy Drink",
    "AF": "Functional Beverage",
}

df["Category"] = df["CategoryComap(CATEGORY_MAP)
```
✔️ Enables category-specific ML
✔️ Avoids meaningless letter codes in reports
✔️ Aligns system output with business units

# ✉️ Email Reports

Flask-Mail sends an HTML version of the report using:

```
app/templates/report.html
```

Email uses environment variables from `.env`:

```env
MAIL_SERVER=smtp.gmail.com
MAIL_PORT=465
MAIL_USE_SSL=True
MAIL_USERNAME=your_email
MAIL_PASSWORD=app_password
```

---

# 🐳 Docker Usage

### Start the system

```bash
docker-compose up --build
```

### Stop containers

```bash
docker-compose down
```

### Example exposed ports

| Service | Port |
|--------|------|
| Flask App | 8000 |
| NGINX (optional) | 80 |

---

# 📂 Project Structure

```
XYZ-Sales-Monitoring/
├── app/
│   ├── data/
│   ├── file_process/
│   │   ├── file_manager.py
│   │   ├── sales_monitor.py
│   │   ├── report_generator.py
│   ├── models/
│   │   └── category_anomaly_models.pkl
│   ├── templates/
│   │   └── report.html
│   └── mail/
│       ├── mail_sender.py
│       └── configuration.py
├── tests/
│   ├── file_process/
│   ├── mail/
│   └── integration/
│       └── test_full_flow.py
├── docker-compose.yml
├── Dockerfile
├── Pipfile
├── app.py
├── .env
└── README.md
```

# 🧪 Testing

### Run tests

```bash
pytest -q
```

### Run with coverage

```bash
coverage run -m pytest
coverage html
```

Open:

```
htmlcov/index.html
```

Coverage includes:

- anomaly detection  
- file monitoring  
- report generation  
- mail sending  
- integration flow  

---

# 🚀 Quick Start

```bash
git clone <repo>
cd transaction_data_report
pipenv install
docker-compose up --build
```

Drop your CSV files into:

```
app/data/Sabores Ibéricos Company Transaction Data/
```

And the system will:

- detect new files  
- generate a report  
- mark anomalies  
- update the HTML/JSON endpoints  
- optionally send email  
- archive previous reports  

---

# 🧭 Future Enhancements

- Streamlit / Dash dashboard  
- MLflow model versioning  
- Kafka ingestion stream  
- Grafana metrics  
- Historical anomaly visualizations  

---

# 📜 License

MIT License.

---

# 🙌 Credits

- Flask  
- Pandas  
- Scikit-Learn  
- Docker  
- NGINX  
- Flask-Mail  
- APScheduler  
