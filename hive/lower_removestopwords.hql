add file rmstop.py
add file english.stop
select transform(lower(review)) using 'rmstop.py' as (rmsreview STRING) from reviews;