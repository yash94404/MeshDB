# **MeshDB: Natural Language Multi-Database Query Agent**

MeshDB is a system that enables natural language queries across multiple database systems, including **PostgreSQL**, **Neo4j**, and **MongoDB**. It dynamically generates multi-stage query pipelines, integrating SQL, Cypher, and NoSQL queries to provide seamless cross-database analytics.

---

## **Features**
- **Multi-Database Query Support**: Execute natural language queries across PostgreSQL, Neo4j, and MongoDB.
- **Dynamic Query Pipeline**: Uses GPT-4 to generate multi-stage pipelines for cross-database queries.
- **Schema Inference**: Automatically infers database schemas and saves them in a unified JSON format.
- **Query Optimization**: Handles multi-stage queries with dynamic placeholder resolution.
- **REST API**: Provides endpoints for database dump uploads and query execution.
- **Caching**: Accelerates repeated queries using Redis-based caching.

---

## **Technologies Used**
- **Databases**:
  - PostgreSQL: Relational database for structured data.
  - Neo4j: Graph database for relationship-heavy queries.
  - MongoDB: NoSQL database for flexible document storage.
- **Backend**:
  - Flask: Python web framework for API endpoints.
  - psycopg2: PostgreSQL driver.
  - neo4j: Neo4j Python driver.
  - pymongo: MongoDB driver.
- **AI Integration**:
  - OpenAI GPT-4: Converts natural language into database-specific queries.
- **Other Tools**:
  - Redis: Query result caching.
  - Docker: Containerization for deployment.

---

## **Setup Instructions**

### **Prerequisites**
- Python 3.8+
- PostgreSQL, Neo4j, and MongoDB installed and running.
- Redis server installed and running.
- OpenAI API key.

### **Installation**
1. Clone the repository:
   ```bash
   git clone [https://github.com/yash94404/MeshDB.git]
   cd MeshDB
