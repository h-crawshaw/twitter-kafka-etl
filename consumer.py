from datetime import datetime, timedelta
import typing

if datetime.now().strftime("%H") == '00':
            date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
print(datetime.now().strftime("%Y-%m-%d"))
print(datetime.now() - timedelta(days=1))