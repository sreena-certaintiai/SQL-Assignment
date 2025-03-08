from flask import Flask
from celery import Celery
import pandas as pd
import datetime
import os

app = Flask(__name__)

app.config['CELERY_BROKER_URL'] = 'sqla+sqlite:///celerydb.sqlite'
app.config['CELERY_RESULT_BACKEND'] = 'db+sqlite:///celery_results.sqlite'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'], backend=app.config['CELERY_RESULT_BACKEND'])
celery.conf.update(app.config)

def fetch_sales_data():
    data = [
        {"Date": "2025-02-01", "Product": "Laptop", "Revenue": 1200},
        {"Date": "2025-02-02", "Product": "Phone", "Revenue": 800},
        {"Date": "2025-02-03", "Product": "Tablet", "Revenue": 500},
    ]
    return pd.DataFrame(data)

@celery.task
def generate_sales_report():
    sales_data = fetch_sales_data()
    filename = f"sales_report_{datetime.datetime.now().strftime('%Y-%m')}.xlsx"

    if not os.path.exists("reports"):
        os.makedirs("reports")

    filepath = os.path.join("reports", filename)
    sales_data.to_excel(filepath, index=False)
    print(f"Report saved: {filepath}")
    return filepath

@app.route('/trigger-report')
def trigger_report():
    task = generate_sales_report.delay()
    return f"Report generation started! Task ID: {task.id}", 200

if __name__ == '__main__':
    app.run(debug=True)

