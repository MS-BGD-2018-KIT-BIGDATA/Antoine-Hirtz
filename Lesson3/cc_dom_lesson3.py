#!/usr/bin/python

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import json

def getSoupFromURL(URL, method='get', form={}):
    """Extracts the source code from a specific URL

    Parameters
    ----------
    URL : string (required)
        web location of the data
   method : string (optional)
        'get' or 'post' request
    form : dict (optional)
        form encoded data embedded in the POST request

    Returns
    -------
    soup : bs4.BeautifulSoup
        content of an HTML/XML document
    """
    if method == 'get':
        res = requests.get(URL)
    elif method == 'post':
        res = requests.post(URL, data=form)
    else:
        raise ValueError("ERROR: Wrong choice of method -> 'get' or 'post'.")
    assert res.ok, "WARNING: Bad request to " + URL + " -> status not equal to 200."
    return BeautifulSoup(res.text, 'lxml')

# Récupérer via crawling la liste des 256 top contributors sur cette page:
# https://gist.github.com/paulmillr/2657075

url_topcontrib = 'https://gist.github.com/paulmillr/2657075'

def getTopGitHubContributors(URL=url_topcontrib, N=256):
    """Returns the first N top GitHub contributors

    Parameters
    ----------
    N : integer (optional)
        Size of the list - up to 256 max

    Returns
    -------
    pandas.DataFrame object
        list of the first N top contributors
        associated to the number of their contributions
    """
    list_topcontrib = []
    dict_new_contrib = {}
    N += 1
    soup = getSoupFromURL(URL)
    for tr in soup.find_all('tr')[1:N]:
        tds = tr.find_all('td')
        user = tds[0].text
        login = user[0:user.find(" (")]
        name = user[user.find("(")+1:user.find(")")]
        contribs = tds[1].text
        dict_new_contrib = {'Login': login, 'Name': name, 'Contribs': contribs}
        list_topcontrib.append(dict_new_contrib)
    df_topcontrib = pd.DataFrame(list_topcontrib)
    df_topcontrib['Contribs'] = pd.to_numeric(df_topcontrib['Contribs'], errors='coerce')
    df_topcontrib.index = range(1, N)
    return df_topcontrib

df_topcontrib = getTopGitHubContributors()

# En utilisant l'API github ( lien: https://developer.github.com/v3/ ),
# récupérer pour chacun de ces users le nombre moyens de stars des repositories qui leur appartiennent.
# Pour finir classer ces 256 contributors par leur note moyenne.﻿

def getPeerReview(users=df_topcontrib['Login']):
    """Extracts the average 'stargazers' review given per contributor

    Parameters
    ----------
    users : iterable (optional)
        list of GitHub logins

    Returns
    -------
    list_peer_review : pandas DataFrame
        list of (login, rating) pairs
    """
    personal_token = 'caf975f9a565be8b844f64369f42a23c5e0a5112'
    token_access = '/repos?access_token=' + personal_token
    url_github_api= 'https://api.github.com/users/'
    list_peer_review = []
    for user in users:
        url_profile = url_github_api + user + token_access
        res = requests.get(url_profile)
        assert res.ok, "WARNING: HTTP request to " + user + "'s GitHub profile -> status not equal to 200."
        user_repos = json.loads(res.text)
        list_count = []
        list_count = [repo['stargazers_count'] for repo in user_repos]
        rating = np.nan if not list_count else round(np.mean(list_count))
        list_peer_review.append({'Login': user, 'Peer_review': rating})
    return pd.DataFrame(list_peer_review)

df_stars = getPeerReview()
df_topcontrib['Peer_review'] = df_stars['Peer_review']
