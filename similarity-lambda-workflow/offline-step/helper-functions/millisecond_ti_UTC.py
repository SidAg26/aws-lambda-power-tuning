from datetime import datetime, timezone

# Assuming cpu_time is the CPU time in milliseconds
cpu_time = 1722327551720

# Convert milliseconds to seconds
cpu_time_seconds = cpu_time / 1000.0

# Convert to a datetime object in UTC
utc_time = datetime.fromtimestamp(cpu_time_seconds, tz=timezone.utc)

print(utc_time)