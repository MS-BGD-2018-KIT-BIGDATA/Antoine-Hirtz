import pandas as import pd

file = "rpps-medecins-tab10_44455698007120"
root = "/Users/antoinehirtz/Documents/GitHub/ms-bgd-myrepo/Lesson7/"
filename = root + file
doctors = pd.read_csv(filename + ".csv", sep=',', header=3, encoding='latin_1')
