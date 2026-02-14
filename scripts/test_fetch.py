from njsp.njsp_fetch import fetch_current_year

path, rundate = fetch_current_year()

print("Saved to:", path)
print("RUNDATE:", rundate)
