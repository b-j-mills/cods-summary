### Summary tool for CODs on HDX

This script summarizes the dataset metadata of all of the CODs on HDX into four csvs (datasets_tagged_cods, country_ab_summary, country_em_summary, and country_ps_summary) and checks for "cowboy CODs", which are tagged as CODs but not designated as such in their metadata.

It emails out the list of "cowboy CODs" that is written in the errors text file.

It runs daily and takes 10 minutes to run.

It can additionally summarize population resource headers and boundary resource field names and outputs into two additional csvs. This is not enabled by default because it takes around 45 minutes to run.
