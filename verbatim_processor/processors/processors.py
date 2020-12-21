def recod_nps(value):
  value = int(value)
  if value>=0 and value<=6:
    return "Détracteur"
  elif value>=7 and value<=8:
    return "Neutre"
  elif value>=9:
    return "Promoteur"

def lowercase(value):
  value = str(value)
  return value.lower()

def uppercase(value):
  value = str(value)
  return value.upper()

def capitalize(value):
  value = str(value)
  return value.capitalize()