
🚀 ClickHouse ↔ FlatFile Data Ingestion Tool
A powerful and interactive web application that facilitates two-way data transfer between ClickHouse databases and CSV flat files.


✨ Key Features
🔁 Two-Way Data Ingestion
Seamlessly import data from CSV files to ClickHouse or export data back to flat files.

🔒 JWT-Secured Access
Protect your ClickHouse interactions with robust JWT-based authentication.

✅ Custom Column Selection
Select specific columns before ingestion to keep data clean and precise.

👁️ Live Data Preview
Instantly view the first 100 rows from either a CSV file or ClickHouse query result.

📈 Real-Time Progress Tracking
Visual indicators for background jobs to keep you updated at every step.

🔗 Multi-Table Join Support
Easily join multiple tables within ClickHouse and export the combined dataset.


🛠️ Technology Stack : 

Backend: Java with Spring Boot

Frontend: React.js + Material-UI

Database: ClickHouse

Authentication: JSON Web Tokens (JWT)


⚙️ Requirements
Ensure you have the following installed:


Java 17+

Maven 3.8+

Node.js 14+

Docker (for ClickHouse testing)


⚡ Getting Started


Backend Setup

bash:
cd backend
mvn clean install
mvn spring-boot:run

Frontend Setup

bash:
cd frontend
npm install
npm start

Launch ClickHouse via Docker

bash
docker run -d --name clickhouse-server -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server



🧱 Project Layout

.
├── backend/
│   ├── src/
│   │   ├── main/           # Backend logic (Spring Boot)
│   │   └── test/           # Unit/integration tests
│   └── pom.xml             # Maven build config
├── frontend/
│   ├── src/
│   │   ├── components/     # React UI components
│   │   ├── pages/          # Page-based layout
│   │   └── services/       # API call handling
│   └── package.json        # Frontend dependencies

└── README.md               # You're reading it!


🌐 REST API Overview

Endpoint	                               Method	     Description
/api/tables	                                 GET	     Fetch available ClickHouse tables
/api/columns	                             GET	     Retrieve column metadata for a table
/api/preview/clickhouse	                     GET	     Preview data from ClickHouse (limit 100)
/api/preview/file	                         GET	     Preview data from an uploaded CSV file
/api/ingest/clickhouse-to-file	             POST	     Export data from ClickHouse to CSV
/api/ingest/file-to-clickhouse	             POST	     Import data from CSV into ClickHouse
/api/ingest/join-tables	                     POST	     Join multiple tables and export the result


✅ Testing Coverage


Thoroughly tested for the following scenarios:

Single table export/import

Multi-table JOIN operations

JWT token validation

File parsing and error handling

Real-time progress updates

Robust connection handling


🙌 Credits


🧠 ClickHouse — Analytics DBMS

⚙️ Spring Boot — Java backend framework

💡 React — UI library

🎨 Material-UI — React UI framework