
# Apache Airflow Project

**Author:**   
Bipin Ghimire - bipin.ghimire@fusemachines.com  

## Prerequisites

- Create a virtual environment
- Install Apache Airflow inside the environment.

      pip install "apache-airflow[celery]==2.7.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.8.txt"  

      Note: In the contraints specify your python version.

- A folder named airflow will now be created in you /home/user directory
- Verify the installation running the below command in  your bash:  

         apache standalone

- Create a folder named dags inside airflow folder.
- Create python file as requred for your dag.

- Create Connection in you airflow UI.

- **For HTTP Connection**
   - Set http connection id.
   - Connection Type: HTTP
   - Host: Your API URL 

   ![Http Connection](/Screenshots/http_connection.png)  


- **For Postgres Connection**
   - Set postgres connection id.
   - Connection Type: Postgres  

         Note: If connection Type is not found You need to install the postgres dependencies for airflow  

         pip install apache-airflow['postgresql']

   - Host: localhost
   - Schema: Your database name
   - Username: Your Postgres username
   - Password: Your Postgres Password
   - Port: 5432
   - Save

    ![Postgres Connection](/Screenshots/postgres_connection.png)  
   

- **Create variables if necessary**
   - Always keep your secret keys, password etc. inside variables.
   - Example:



**Result Of My Dag**

Dag code is inside the dag folder.
The dependencies flow of my dag is as:
![Dependencies](/Screenshots/dag_flow.png)


Final Log Output is:
![Final Log](/Screenshots/final_log.png)


