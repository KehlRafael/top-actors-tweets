# top-actors-tweets
Generate a report with the last 10 tweets about the 10 actors that did the most movies in the last 10 years. You'll need Pandas and Tweepy to run this
as a script or to import it as a module. It will generate `.csv` files with their names and a number to avoid deleting any previously generated report.
It downloads the datasets only once a day.

## Overview
The script will save all data into the `data` folder and all reports into the `reports` folder. You can import this script as a module, but you must have
these folders created on your current directory. We will briefly explain the functions below.

- `get_dataset_url(dataset)`:
Receives the dataset name as input and returns the URL for you to download the dataset file.

- `get_dataset(dataset)`:
Receives the dataset name as input and returns the path to the downloaded dataset. You can see the downloaded dataset at the `data` folder.

- `get_actors(titleBasics = None, titlePrincipals = None, nameBasics = None)`:
Downloads datasets from IMDb to generate and return a DataFrame with the names of the 10 actors that did the most movies in the last 10 years and the ammount of movies each one did. You can also pass any of the three optional parameters to provide the path for any IMDb dataset you might already have instead of downloading a new one.

- `twitter_auth()`:
Authenticates to Twitter API using keys stored into environment variables. THe variables used are `tw_consumer_key`, `tw_consumer_secret`, `tw_key` 
and `tw_secret`.

- `reports_path(filename)` and `data_path(filename)`: 
Receives a filename and returns the path it will be saved by the script.

- `tweets_report(titleBasics = None, titlePrincipals = None, nameBasics = None)`:
Runs the whole process, from downloading the data from IMDb to generating the `.csv` reports on the `reports` folder. You can also pass any of the three optional parameters to provide the path for any IMDb dataset you might already have instead of downloading a new one.

This script main only runs `tweets_report()`.
