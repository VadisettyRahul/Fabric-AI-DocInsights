# orchestrator.py

import azure.functions as func
import logging
import subprocess

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Orchestrator function triggered.')

    try:
        # Run Data Ingestion
        ingestion_result = subprocess.run(['python', 'data_ingestion.py'], capture_output=True, text=True)
        if ingestion_result.returncode != 0:
            logging.error(f"Data Ingestion Failed: {ingestion_result.stderr}")
            return func.HttpResponse("Data Ingestion Failed", status_code=500)
        logging.info("Data Ingestion Successful.")

        # Run Anomaly Detection
        anomaly_result = subprocess.run(['python', 'anomaly_detection.py'], capture_output=True, text=True)
        if anomaly_result.returncode != 0:
            logging.error(f"Anomaly Detection Failed: {anomaly_result.stderr}")
            return func.HttpResponse("Anomaly Detection Failed", status_code=500)
        logging.info("Anomaly Detection Successful.")

        # Refresh Power BI Report
        refresh_result = subprocess.run(['python', 'powerbi_refresh.py'], capture_output=True, text=True)
        if refresh_result.returncode != 0:
            logging.error(f"Power BI Refresh Failed: {refresh_result.stderr}")
            return func.HttpResponse("Power BI Refresh Failed", status_code=500)
        logging.info("Power BI Report Refreshed Successfully.")

        return func.HttpResponse("Pipeline Executed Successfully", status_code=200)
    
    except Exception as e:
        logging.error(f"Orchestrator Error: {e}")
        return func.HttpResponse("Orchestrator Error", status_code=500)
