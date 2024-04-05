#! /usr/bin/env sh
# Let the DB start
sleep 10;
# Run migrations
FLASK_APP=run.py python -m flask db upgrade