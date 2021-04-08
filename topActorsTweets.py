import os
import requests
import pandas as pd
import tweepy
from datetime import date
from datetime import datetime


def get_dataset_url(dataset):
    """Returns the url to download an IMDb dataset given it's name"""
    if dataset == "name.basics":
        return "https://datasets.imdbws.com/name.basics.tsv.gz"
    elif dataset == "title.akas":
        return "https://datasets.imdbws.com/title.akas.tsv.gz"
    elif dataset == "title.basics":
        return "https://datasets.imdbws.com/title.basics.tsv.gz"
    elif dataset == "title.crew":
        return "https://datasets.imdbws.com/title.crew.tsv.gz"
    elif dataset == "title.episode":
        return "https://datasets.imdbws.com/title.episode.tsv.gz"
    elif dataset == "title.principals":
        return "https://datasets.imdbws.com/title.principals.tsv.gz"
    elif dataset == "title.ratings":
        return "https://datasets.imdbws.com/title.ratings.tsv.gz"
    else:
        return None


def get_dataset(dataset):
    """Downloads the dataset to data/ddmmyyyy-dataset.tsv.gz"""
    # Gets URL then checks if file was already downloaded today. If so, just returns path.
    # Returns None if its not a valid IMDb dataset
    url = get_dataset_url(dataset)
    if url == None:
        return None
    datafile = data_path(url.split('/')[-1])
    if os.path.isfile(datafile):
        return datafile
    else:
        # If it was not downloaded, then download as stream to not use tons of RAM
        with requests.get(url, stream=True) as r:
            with open(datafile, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    # If you have chunk encoded response uncomment if
                    # and set chunk_size parameter to None.
                    #if chunk: 
                    f.write(chunk)
        return datafile


def get_actors():
    """
    Downloads datasets from IMDb to generate and return a DataFrame 
    with the names of the 10 actors that did the most movies in the
    last 10 years and the ammount of movies each one did.
    """

    # Import all titles from IMDb
    movies = pd.read_csv(get_dataset("title.basics"), sep='\t', compression="gzip",
                         usecols=["tconst", "titleType", "startYear"], low_memory=False)

    # Filter only movies from all kinds of title types
    movies = movies[movies["titleType"] == "movie"]

    # Convert types so we can further filter our dataset
    movies["startYear"] = pd.to_numeric(movies["startYear"], errors="coerce")

    # Gets all movies from the last 10 years (inclusive), IMDb has some entries for future movies, which we filter out
    movies = movies[(movies["startYear"] >= date.today().year - 10) & (movies["startYear"] <= date.today().year)]

    # Now get the principals so we can get 10 actors/actresses that did the most movies in the last 10 years
    title_principals = pd.read_csv(get_dataset("title.principals"), sep='\t', compression="gzip",
                                   usecols=["tconst", "ordering", "nconst", "category"], low_memory=False)

    # Filter to get only actors/actresses
    title_principals = title_principals[(title_principals["category"] == "actor") |
                                        (title_principals["category"] == "actress")]

    # Merge datasets to get the set we want
    actors = pd.merge(title_principals, movies, on="tconst")

    # Sum unique participations, turn into a dataframe and slice the top 10
    actors = actors['nconst'].value_counts().rename_axis('nconst').to_frame('participations')[0:10]

    # Drop principals and movies from memory, can't do on function
    lst = [title_principals, movies]
    del title_principals, movies
    del lst

    # Import names from IMDb and convert types
    names = pd.read_csv(get_dataset("name.basics"), sep='\t', compression="gzip",
                        usecols=["nconst", "primaryName"], low_memory=False)

    return pd.merge(actors, names, on='nconst')


def twitter_auth():
    """Authenticate to Twitter API"""
    auth = tweepy.OAuthHandler(os.environ['tw_consumer_key'],
                               os.environ['tw_consumer_secret'])
    auth.set_access_token(os.environ['tw_key'],
                          os.environ['tw_secret'])

    return tweepy.API(auth)


def reports_path(filename):
    """Returns path for reports"""
    return os.path.join('reports',datetime.now().strftime("%Y%m%d%H%M%S-") + filename)


def data_path(filename):
    """Returns path for datasets"""
    return os.path.join("data",date.today().strftime('%Y%m%d-') + filename)


def tweets_report():
    """
    Export to csv files the last 10 tweets about the 10 actors
    that did the most movies in the last 10 years.
    """
    # Authenticate to Twitter
    api = twitter_auth()

    # Get the list of actors from our data and also prints it to csv
    actors = get_actors()
    actors = actors.convert_dtypes()
    actors.to_csv(reports_path('ActorsWithMostMovies.csv'))

    # Create DataFrame we will use to store tweet data
    tweets = pd.DataFrame()

    # Iterate through rows even though its not a good practice
    for index, actor in actors.iterrows():
        # Prepare the query and do the search
        q = "(" + actor['primaryName'] + ")"
        search = api.search(q, count=10, result_type='recent', include_entities=False)

        # Test if search is not empty
        if search != []:
            # Prepare a list to receive each tweet dict
            tweetlist = []

            # Iterate through tweets and append the json converted to dict
            for tweet in search:
                tweetlist.append(tweet._json)

            # Finally put all tweets in the DataFrame and export to .csv
            tweets = pd.DataFrame(tweetlist)
            tweets = tweets.convert_dtypes()
            tweets.to_csv(reports_path(actor['primaryName'].replace(" ", "") + '.csv'))


if __name__ == '__main__':
    """If ran as a script, it generates the tweets report"""
    tweets_report()
