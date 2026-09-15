#!/bin/bash
pip3 install pyarrow pandas torch transformers
wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar -P /usr/lib/spark/jars/
