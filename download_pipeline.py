from datetime import datetime, timedelta

from download_step2a_normalizedates import normalizedates
from download_step2b_normalizenames import normalizenames
from download_step2c_merge import merge_names
from download_step3b_fix import main_fix
from download_step1a_scraper import run_t

# from unused.download_step3a_anomaly import main_anomaly

# Compute the reference date window
yesterday = datetime.now() - timedelta(days=1)
DDMM = yesterday.strftime("%d/%m")

# Iterate through years and download contracts
YEARS = range(2009, 2012)
for y in YEARS:
    print(y)
    run_t(DDMM, y)

# Normalize, merge, and apply manual fixes
for y in YEARS:
    print(y)
    normalizedates(y)
    normalizenames(y)
    merge_names(y)
    main_fix(y)


# For analyzing anomalies in contracts
# main_anomaly()
