# hyperleaup
Create and manipulate Tableau Hyper files from Apache Spark DataFrames and Spark SQL.

## Example
The following code snippet executes a SQL statement as an Apache Spark DataFrame,
creates a Tableau Hyper file, and publishes it as a datasource to a Tableau Server.
```python
# Create a Hyper File from SQL
query = """
select *
  from transaction_history
 where action_date > '2015-01-01'
"""

hf = HyperFile(query)

# Publish Hyper File to Tableau Server
hf.publish(tableau_server, tableau_version,
           username, password, output_path,
           site_content_url, project_name,
           datasource_name)
```
