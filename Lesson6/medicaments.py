import pandas as pd
import re
import requests
import json


search_url =  "https://www.open-medicaments.fr/api/v1/medicaments?limit=10&query=ibupro"

result_search = requests.get(search_url)
response = json.loads(result_search.text)
regex = re.compile('\d+')
regex_ans = re.compile('(\d+) ans')
regex_kg = re.compile('(\d+) kg')

for medicament in response:
  print medicament['codeCIS']

  url = "https://www.open-medicaments.fr/api/v1/medicaments/" + medicament['codeCIS']
  result = requests.get(url)
  response_result = json.loads(result.text)
  libelle = response_result['presentations'][0]['libelle']
  denomination = response_result['denomination']
  indication = response_result['indicationsTherapeutiques']
  indic_ans = regex_ans.findall(indication)
  indic_kg = regex_kg.findall(indication)
  date = response_result['presentations'][0]['dateDeclarationCommercialisation']
  annee = date.split('-')[0]
  mois = date.split('-')[1]
  print int(regex.findall(denomination)[0]), int(regex.findall(libelle)[0]), annee, mois, indic_ans, indic_kg
