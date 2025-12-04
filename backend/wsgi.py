import sys
import os

# Path to backend
path = '/home/SarathPolling/backend'
if path not in sys.path:
    sys.path.insert(0, path)

# community version of profile "https://dbc-f0ecd2b2-b14a.cloud.databricks.com"
# trail version of profile "https://dbc-4c3ee4bb-030f.cloud.databricks.com"

# Environment variables
os.environ.setdefault("DATABRICKS_HOST", "https://dbc-f0ecd2b2-b14a.cloud.databricks.com")
os.environ.setdefault("DATABRICKS_PAT", "***REMOVED***")

# Import Flask app
from main import app as application
