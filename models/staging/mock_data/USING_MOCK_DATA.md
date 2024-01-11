# Using the mocked data sets

Some of the data sets have comments for days - this is used for demos
on snapshots and incremental models to replicate the changing nature of the
data. You should proceed in this order:

1. On the mock data source model that the demo references, keep day 1 
   uncommented but leave all other SQL commented.
2. Execute the model, then look at the results by ref'ing what you built
   in an untitled tab or querying it within your data platform.
3. On the mock data model, comment out day 1 and uncomment day 2, then 
   perform your execution and review again.
4. Repeat these steps for each day until you have run the last
   day's data.