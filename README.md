# 🚀 Real-Time AI-Driven Anomaly Detection & Data Integration System 🌐

Welcome to the **Real-Time AI-Driven Anomaly Detection & Data Integration System**, an end-to-end project that leverages **Apache Kafka** for real-time data streaming, **Azure Data Lake** for secure storage, and **Power BI** for dynamic reporting and visualization. This project showcases how **RTI (Real-Time Integration)** can streamline data ingestion, anomaly detection, and business intelligence.

---

## 📚 Table of Contents

- [🔍 Overview](#-overview)
- [✨ Features](#-features)
- [🛠️ Technologies Used](#️️️️️️️️️️-technologies-used)
- [⚙️ Setup & Installation](#️️️️️️️️️️-setup--installation)
- [🚀 Usage](#-usage)
- [🗂️ Project Structure](#️️️️️️️️️️-project-structure)
- [🤝 Contributing](#-contributing)
- [📜 License](#-license)
- [📞 Contact](#-contact)

---

## 🔍 Overview

Manufacturing industries rely heavily on sensor data to monitor equipment health and ensure smooth operations. Detecting anomalies early can prevent costly downtimes and extend the lifespan of machinery. This project integrates various Azure services to create a comprehensive anomaly detection pipeline:
This project integrates **real-time data ingestion**, **anomaly detection**, and **data visualization** into one powerful system. The key components of the project are:

- **Real-Time Data Streaming with Apache Kafka**
- **Anomaly Detection using Isolation Forest (scikit-learn)**
- **Data Storage and Management in Azure Data Lake**
- **Data Visualization and Reporting with Power BI**
- **Automated Power BI Data Refresh using Azure Logic Apps**
- **Containerization with Docker Compose**

1. **Data Ingestion:** Collects sensor data and stores it in Azure Data Lake.
2. **Anomaly Detection:** Processes data using machine learning models in Azure Machine Learning.
3. **Visualization:** Provides real-time dashboards with Power BI.
4. **Orchestration:** Automates workflows using Azure Functions.
5. **Security & Monitoring:** Ensures data security with Azure Key Vault and monitors system health with Azure Monitor.

---

 ### 1. **Data Ingestion with Apache Kafka** 🛠️
   - Real-time data ingestion from multiple sources using **Apache Kafka**.
   - Simulate manufacturing sensor data streams in real-time for anomaly detection.
   - Kafka brokers for streaming large amounts of data with high throughput.

### 2. **Anomaly Detection with Isolation Forest** 🤖
   - AI-driven anomaly detection using **scikit-learn's Isolation Forest** algorithm.
   - Detect anomalies in sensor data and alert based on thresholds.
   - Real-time analysis to prevent potential equipment failure.

### 3. **Azure Data Lake Integration** ☁️
   - Store raw and processed data securely in **Azure Data Lake Storage**.
   - Scalable cloud storage for long-term data management.
   - Seamless integration with Azure services for big data workloads.

### 4. **Power BI Integration & Automated Refresh** 📊
   - Visualize processed data in **Power BI** with dynamic reports and dashboards.
   - Automate Power BI data refresh using **Azure Logic Apps**.
   - Enable business intelligence teams to monitor real-time data changes.

### 5. **Real-Time Alerting and Monitoring** 🚨
   - Immediate alerting using **SMTP email notifications** when anomalies are detected.
   - Set up thresholds for critical alerts to enable proactive decision-making.

### 6. **Containerization with Docker Compose** 🐳
   - Use **Docker Compose** to orchestrate Apache Kafka, Zookeeper, and the Python applications.
   - Easily scale the system with containerized services.

---

## 🏗️ Project Architecture

1. **Data Producer:** 
   - Streams real-time sensor data from various sources into Apache Kafka.
   
2. **Data Consumer:**
   - Consumes real-time sensor data, runs the Isolation Forest model to detect anomalies, and stores results in Azure Data Lake.

3. **Power BI Reporting:**
   - Visualize the detected anomalies and generate real-time business intelligence reports.

4. **Automated Power BI Refresh:**
   - Use Azure Logic Apps to automate Power BI data refresh, keeping reports up-to-date with the latest data.

## 🛠️ Technologies Used

- **Azure Services:**
  - [Azure Data Lake Storage Gen2](https://azure.microsoft.com/en-us/services/storage/data-lake-storage/)
  - [Azure Machine Learning](https://azure.microsoft.com/en-us/services/machine-learning/)
  - [Azure Functions](https://azure.microsoft.com/en-us/services/functions/)
  - [Azure Key Vault](https://azure.microsoft.com/en-us/services/key-vault/)
  - [Azure Monitor](https://azure.microsoft.com/en-us/services/monitor/)
  - [Power BI](https://powerbi.microsoft.com/)
  - **Apache Kafka** for real-time data ingestion.
- **Docker & Docker Compose** for container orchestration.
  
- **Programming Languages & Libraries:**
  - [Python](https://www.python.org/)
  - [Pandas](https://pandas.pydata.org/)
  - [Scikit-learn](https://scikit-learn.org/)
  - [Azure SDK for Python](https://azure.github.io/azure-sdk-for-python/)
  - [Joblib](https://joblib.readthedocs.io/)
  - [MSAL](https://github.com/AzureAD/microsoft-authentication-library-for-python)

---

## ⚙️ Setup & Installation

Follow these steps to set up and run the Anomaly Detection System locally and deploy it to Azure.

### 📋 Prerequisites

- **Azure Account:** Ensure you have an active [Azure subscription](https://azure.microsoft.com/en-us/free/).
- **Azure CLI:** Install the [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) for managing Azure resources.
- **Python 3.8+:** Install [Python](https://www.python.org/downloads/) and ensure it's added to your system's PATH.
- **Git:** Install [Git](https://git-scm.com/downloads) for version control.
- **Azure Functions Core Tools:** Install [Azure Functions Core Tools](https://docs.microsoft.com/en-us/azure/azure-functions/functions-run-local) for local development.

### 🔧 Installation Steps

1. **Clone the Repository**

   ```bash
   git clone https://github.com/Gamingpro237/Anomaly-detection-system.git
   cd anomaly-detection-system
   ```

2. **Create a Virtual Environment**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Azure Services**

   - **Azure Data Lake Storage Gen2:**
     - Create a Storage Account with Data Lake Storage Gen2 enabled.
     - Note down the **Storage Account Name** and **Access Key**.
   
   - **Azure Key Vault:**
     - Create a Key Vault to store secrets.
     - Add the following secrets:
       - `storage-account-key`: Your Storage Account Access Key.
       - `azure-subscription-id`
       - `azure-resource-group`
       - `azure-ml-workspace-name`
       - `azure-ml-workspace-secret`
       - `powerbi-client-secret`
   
   - **Azure Machine Learning Workspace:**
     - Set up an Azure ML Workspace.
     - Note down the workspace details for configuration.
   
   - **Power BI:**
     - Register an application in Azure AD for Power BI API access.
     - Obtain the **Client ID**, **Client Secret**, and **Tenant ID**.

5. **Set Up Environment Variables**

   Create a `.env` file in the project root and add the necessary environment variables:

   ```env
   VAULT_URL=https://your-key-vault-name.vault.azure.net/
   STORAGE_ACCOUNT_NAME=your_storage_account_name
   STORAGE_ACCOUNT_KEY_SECRET=storage-account-key
   AZURE_SUBSCRIPTION_ID=your_subscription_id
   AZURE_RESOURCE_GROUP=your_resource_group
   AZURE_ML_WORKSPACE_NAME=your_azure_ml_workspace
   AZURE_ML_WORKSPACE_SECRET=azure-ml-workspace-secret
   POWERBI_CLIENT_ID=your_powerbi_client_id
   POWERBI_CLIENT_SECRET=your_powerbi_client_secret
   TENANT_ID=your_tenant_id
   POWERBI_WORKSPACE_ID=your_powerbi_workspace_id
   POWERBI_DATASET_ID=your_powerbi_dataset_id
   ```

6. **Run Setup Scripts**

   - **Set Up Azure Monitor Alerts**

     ```bash
     python setup_alerts.py
     ```

---

## 🚀 Usage

### 🔄 Running the Pipeline Locally

1. **Data Ingestion**

   ```bash
   python data_ingestion.py
   ```

2. **Anomaly Detection**

   ```bash
   python anomaly_detection.py
   ```

3. **Power BI Report Refresh**

   ```bash
   python powerbi_refresh.py
   ```

4. **Orchestrate the Workflow**

   Run the orchestrator script to execute all steps sequentially:

   ```bash
   python orchestrator.py
   ```
5. **Orchestrate anomaly**

   Run the orchestrator anomaly script:

   ```bash
   python orchestrator-anomaly.py
   ```
6. **Set Up Docker Containers:**
   ```bash
   docker-compose up -d
   ```

   - This will spin up Apache Kafka, Zookeeper, and the Python applications for data ingestion and anomaly detection.
### ☁️ Deploying to Azure

1. **Deploy Azure Functions**

   ```bash
   func azure functionapp publish your-function-app-name
   ```

2. **Configure Azure Functions**

   - Ensure all environment variables are set in the Azure Function App settings.
   - Set up Timer Triggers to automate the workflow execution.

3. **Monitor and Manage**

   - Use Azure Monitor to track the health and performance of your functions.
   - View logs and set up alerts as configured.

---

## 📊 Power BI Integration

- To visualize the anomaly detection results in **Power BI**, connect your Power BI reports to the data stored in **Azure Data Lake**.
- Set up a real-time dashboard that shows the status of your sensors and detected anomalies.
- Use **Azure Logic Apps** to automate the refresh process, ensuring that your Power BI reports are always up-to-date.

---

## 🔔 Real-Time Alerts

- Use **SMTP email notifications** to trigger alerts when anomalies are detected.
- You can configure threshold levels to control when an alert is triggered, ensuring you're informed of critical issues as they occur.

---

## 🎉 How It All Works

1. **Real-Time Data Ingestion:** The system ingests real-time sensor data into Apache Kafka.
2. **Anomaly Detection:** Using AI (Isolation Forest), the system detects anomalies in sensor data.
3. **Data Storage & Alerting:** Anomalies are stored in Azure Data Lake, and real-time alerts are sent via email.
4. **Business Intelligence:** Power BI visualizes the anomaly data with automated refreshes, keeping reports up-to-date.

## 🗂️ Project Structure

```plaintext
anomaly-detection-system/
├── data_ingestion.py           # Script for data ingestion
├── anomaly_detection.py        # Script for anomaly detection
├── powerbi_refresh.py          # Script to refresh Power BI reports
├── orchestrator.py & orchestrator-anomaly           # Orchestrator script to run all steps
├── setup_alerts.py             # Script to set up Azure Monitor alerts
├── requirements.txt            # Python dependencies
├── README.md                   # Project documentation
├── .env                        # Environment variables (not committed)
├── data_producer.py           # Script for producing real-time sensor data to Kafka
├── data_consumer.py           # Script for consuming data from Kafka and detecting anomalies
├── alerting.py                # Script for sending real-time alerts
├── docker-compose.yml         # Docker Compose file to set up Kafka and Zookeeper
```

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps to contribute:

1. **Fork the Repository**

   Click the **Fork** button at the top right of this page.

2. **Clone Your Fork**

   ```bash
   git clone https://github.com/Gamingpro237/Anomaly-detection-system.git
   cd anomaly-detection-system
   ```

3. **Create a Feature Branch**

   ```bash
   git checkout -b feature/your-feature-name
   ```

4. **Commit Your Changes**

   ```bash
   git commit -m "Add your descriptive commit message"
   ```

5. **Push to Your Fork**

   ```bash
   git push origin feature/your-feature-name
   ```

6. **Create a Pull Request**

   Navigate to the original repository and click **Compare & pull request**.

---

## 📜 License

This project is licensed under the [Apache 2.0 License](LICENSE).


---

## 📝 Acknowledgements

- [Azure Documentation](https://docs.microsoft.com/en-us/azure/)
- [Power BI Documentation](https://docs.microsoft.com/en-us/power-bi/)
- [Scikit-learn Documentation](https://scikit-learn.org/)
- [Pandas Documentation](https://pandas.pydata.org/)

---
```
