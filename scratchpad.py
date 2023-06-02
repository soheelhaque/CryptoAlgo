from datetime import datetime


timestamp = 1682928000000/1000

date1 = datetime.strptime('230331', '%y%m%d')
date2 = datetime.strptime('230425', '%y%m%d')
delta = date2 - date1
print(delta.days)

