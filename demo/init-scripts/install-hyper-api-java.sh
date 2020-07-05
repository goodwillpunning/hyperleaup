#!/bin/sh
echo "Installing the Tableau Hyper API for Java release 9.0.0.11074..."

# Download Hyper API for Java
echo "Downloading Hyper API release for Java..."
wget -O /tmp/hyperapi_release_9.0.0.zip https://downloads.tableau.com/tssoftware/tableauhyperapi-java-linux-x86_64-release-hyperapi_release_9.0.0.11074.r1998e9f2.zip

# Unzip the archive
echo "Extracting contents from ZIP archive..."
unzip /tmp/hyperapi_release_9.0.0.zip -d /tmp/hyperapi_release_9.0.0/

# Copy all Hyper API artifacts to JAR directory on the worker nodes
echo "Installing Hyper API artifacts on worker nodes..."
cp -r /tmp/hyperapi_release_9.0.0/tableauhyperapi-java-linux-x86_64-release-hyperapi_release_9.0.0.11074.r1998e9f2/lib/hyper /databricks/jars
cp /tmp/hyperapi_release_9.0.0/tableauhyperapi-java-linux-x86_64-release-hyperapi_release_9.0.0.11074.r1998e9f2/lib/*.jar /databricks/jars

echo "Installation complete!"
