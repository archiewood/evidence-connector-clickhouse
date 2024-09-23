#!/bin/bash
# Clean up the test-app directory if it exists
rm -rf test-app

# Download the plugin dependencies
npm install

# Dowload and install the template into a subdirectory
npx degit evidence-dev/template test-app
cd test-app
npm install

# Install the datasource into the app and add it to the evidence.plugins.yaml file
npm install ..
echo '  "evidence-connector-my-datasource": { }' >> evidence.plugins.yaml

# Create a source in the test app
mkdir sources/test_source
echo """name: test_source
type: my-datasource
""" >> sources/test_source/connection.yaml
echo "select 1" >> sources/test_source/test_query.sql

# Edit the index.md file to show the test_query in a table
cd pages
echo '## Test Plugin is working!
```sql test_plugin
select * from test_source.test_query
```
<DataTable data={test_plugin} />
' >> index.md