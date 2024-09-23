#!/bin/bash

# Prompt the user for input
read -p "Enter your source name: " source_name
# lowercase the source name
source_name=$(echo "$source_name" | tr '[:upper:]' '[:lower:]')

# Read the existing package.json into a variable
package_json=$(<package.json)

# Update the package name
package_json=$(echo "$package_json" | sed "s/\"name\": \".*\"/\"name\": \"evidence-connector-$source_name\"/")


# Prepare the datasources array
datasources_array="[ \"$source_name\" ]"
package_json=$(echo "$package_json" | sed "s/\"datasources\": \[[^]]*\]/\"datasources\": $datasources_array/")

# Write the updated package.json back to the file
echo "$package_json" > package.json
echo "package.json has been updated."

# Move the README.md into a gitignored file /plugin-template/README.md
# Create a new README.md with the title Evidence [source_name] Source Plugin
mkdir -p plugin-template
mv README.md plugin-template/README.md
echo "# Evidence $source_name Source Plugin

Install this plugin in an Evidence app with
\`\`\`bash
npm install evidence-connector-$source_name
\`\`\`

Register the plugin in your project in your evidence.plugins.yaml file with
\`\`\`bash
datasources:
  evidence-connector-$source_name: {}
\`\`\`

Launch the development server with \`npm run dev\` and navigate to the settings menu (localhost:3000/settings) to add a data source using this plugin.
" > README.md

# Move the scaffold.sh and bootstrap.sh to the plugin-template folder
mv scaffold.sh plugin-template/scaffold.sh
mv bootstrap.sh plugin-template/bootstrap.sh

echo "README.md, scaffold.sh and bootstrap.sh have been moved to plugin-template"