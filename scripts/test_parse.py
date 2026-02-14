from pathlib import Path
from njsp.njsp_parse import parse_fauqstats_crashes

xml_path = Path("data/live/FAUQStats2026.xml")
result = parse_fauqstats_crashes(xml_path)

print("RUNDATE:", result.rundate)
print("STATSYEAR:", result.statsyear)
print("Crash records:", len(result.records))
print("First record keys:", list(result.records[0].keys()))
print("First record:", result.records[0])
