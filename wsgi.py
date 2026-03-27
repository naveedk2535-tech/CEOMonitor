import sys
import os

# Add project directory to path
project_home = '/home/zziai41/CEOMonitor'
if project_home not in sys.path:
    sys.path.insert(0, project_home)

# Set working directory so .env is found
os.chdir(project_home)

from app import app as application
