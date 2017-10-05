import requests
from bs4 import BeautifulSoup

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
    return None

  if res.status_code == 200:
    soup = BeautifulSoup(res.text, 'lxml')
    return soup
  else:
    return None

def scrapDataFrom(URL, classname, i):
  soup = getSoupFromURL(URL)
  if soup:
    data = soup.find_all(class_=classname)
    return data[i].text.strip()
  else:
    return None

for year in range(2010, 2016):
    url = 'http://alize2.finances.gouv.fr/communes/eneuro/detail.php?icom=056&dep=075&type=BPS&param=5&exercice=' + str(year)
    print('ANALYSE DE L\'EQUILIBRE DES COMPTES DE LA COMMUNE DE PARIS - ANNEE: ', str(year))
    print('Total des produits de fonctionnement:')
    print('Euros par habitant: ', scrapDataFrom(url, 'montantpetit G', 1))
    print('Moyenne de la strate: ', scrapDataFrom(url, 'montantpetit G', 2))
    print('Total des charges de fonctionnement:')
    print('Euros par habitant: ', scrapDataFrom(url, 'montantpetit G', 4))
    print('Moyenne de la strate: ', scrapDataFrom(url, 'montantpetit G', 5))
    print('Total des ressources d\'investissement:')
    print('Euros par habitant: ', scrapDataFrom(url, 'montantpetit G', 10))
    print('Moyenne de la strate: ', scrapDataFrom(url, 'montantpetit G', 11))
    print('Total des emplois d\'investissement:')
    print('Euros par habitant: ', scrapDataFrom(url, 'montantpetit G', 13))
    print('Moyenne de la strate: ', scrapDataFrom(url, 'montantpetit G', 14))
