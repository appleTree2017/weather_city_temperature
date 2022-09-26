import pandas as pd
import numpy as np
import os

import multiprocess
from multiprocessing import Pool
import requests
import lxml
from bs4 import BeautifulSoup
from tqdm import tqdm


def search_data(df, app_key='eecda87a4e6bb67b0176b7ea27e17f29'):
    import requests
    from tqdm import tqdm
    import numpy as np

    temperatures = []
    cities = []

    for index, row in tqdm(df.iterrows()):
        # find temperatures by postal_code
        url = "https://api.openweathermap.org/data/2.5/weather?zip=" + str(row["postal_code"]) + "&appid=" + app_key
        f = requests.get(url, headers={'Accept': 'application/json'})
        d = f.json()

        try:
            temperatures.append(d["main"]["temp"])
        except:
            temperatures.append(np.nan)

        # find city name by postal_code
        zipcode = row["postal_code"]
        urlz = "http://api.openweathermap.org/geo/1.0/zip?zip=" + str(zipcode) + "&appid=" + app_key
        fz = requests.get(urlz, headers={'Accept': 'application/json'})

        dz = fz.json()

        try:

            cities.append(dz["name"] + "_" + dz["country"])
        except:
            print(dz)
            cities.append("not found")

    df["temperature"] = temperatures
    df["city"] = cities
    return df


if __name__ == "__main__":
    # read in data
    df = pd.read_csv("survey.csv")

    # calculate cpu number
    cpus = os.cpu_count() - 1

    # split dataframe
    chunks = np.array_split(df, cpus)

    # multiprocessing
    with Pool(cpus) as pool:
        results = pool.map(search_data, chunks)
    pool.close()
    pool.join()

    # merge results
    results_df = pd.concat(results)

    # results_df=pd.read_csv("output1.csv") # incase the key is not working (reach the limit)

    # if nan for gender, give a "blank" value to replace nan
    results_df["gender"] = [i if pd.notnull(i) else "blank" for i in results_df["gender"]]

    # file 1-------------------------------------------------------------------------------------
    # File Details
    # output.csv Input data with additional metadata. Columns include:
    # user_id
    # gender
    # postal_code
    # city
    # temperature

    results_df.to_csv("output.csv", index=False)

    # file 2 ------------------------------------------------------------------------------------
    # cities_by_gender.csv List of cities by number of users. Columns include:
    # city
    # gender
    # num_users

    cities_by_gender = pd.DataFrame(results_df.groupby(["city", "gender"])["user_id"].count()).reset_index(col_level=1)
    cities_by_gender.columns = ["city", "gender", "num_users"]
    cities_by_gender.to_csv("cities_by_gender.csv", index=False)

    # file 3-------------------------------------------------------------------------------------------
    # cities_by_gender_distribution.csv List of cities by distribution of gender. Columns include:
    # city
    # female_percent
    # male_percent
    # non_binary_percent
    # blank_percent
    # total_users

    cities_count = pd.DataFrame(results_df.groupby(["city"])["user_id"].count())
    cities_count_dict = dict(zip(list(cities_count.index),
                                 list(cities_count["user_id"])))  # build a dictionary of total user count for each city

    cities_by_gender_distribution = pd.DataFrame(
        results_df.groupby(["city", "gender"])["user_id"].count()).unstack()  # count users grouped by city and gender
    cities_by_gender_distribution = cities_by_gender_distribution["user_id"]

    cities_by_gender_distribution["total_users"] = [cities_count_dict.get(i) for i in
                                                    cities_by_gender_distribution.index]  # total user number for each city

    # calculate percentage for each gender category
    cities_by_gender_distribution["female%"] = cities_by_gender_distribution["female"] * 100 / \
                                               cities_by_gender_distribution["total_users"]
    cities_by_gender_distribution["male%"] = cities_by_gender_distribution["male"] * 100 / \
                                             cities_by_gender_distribution["total_users"]
    cities_by_gender_distribution["non_binary%"] = cities_by_gender_distribution["non_binary"] * 100 / \
                                                   cities_by_gender_distribution["total_users"]
    cities_by_gender_distribution["blank%"] = cities_by_gender_distribution["blank"] * 100 / \
                                              cities_by_gender_distribution["total_users"]

    # keep the required columns

    cities_by_gender_distribution = cities_by_gender_distribution[[i for i in list(cities_by_gender_distribution)
                                                                   if i in ["total_users", "female%", "male%",
                                                                            "non_binary%", "blank%"]]]

    # set city column
    cities_by_gender_distribution["city"] = list(cities_by_gender_distribution.index)

    # remove multiple levels of row and columns index
    cities_by_gender_distribution.index = list(cities_by_gender_distribution.index)
    cities_by_gender_distribution.columns = list([i for i in cities_by_gender_distribution.columns])

    cities_by_gender_distribution.to_csv("cities_by_gender_distribution.csv")

    # file 4-----------------------------------------------------------------------------
    # cities_by_avg_temp.csv List of cities by average temp.
    # city
    # avg_temp
    female_percentage_dict = dict(zip(cities_by_gender_distribution.index, cities_by_gender_distribution[
        "female%"]))  # build dictionary for female % for each city
    cities_by_avg_temp = pd.DataFrame(
        results_df.groupby(["city"])["temperature"].mean())  # calculate average temperature for each city

    cities_by_avg_temp["female%"] = [female_percentage_dict.get(i) for i in
                                     cities_by_avg_temp.index]  # fill in female % information

    cities_by_avg_temp.to_csv("cities_by_avg_temp.csv")

    # file 5-------------------------------------------------------------------------------------

        # List of top 10 cities with highest avg temp where female_percent >=50%.

    top10 = cities_by_avg_temp[cities_by_avg_temp["female%"] >= 50].sort_values(by=["temperature", ],
                                                                                ascending=False).head(10)

    top10.to_csv("top10_cities.csv")

