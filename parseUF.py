import re

def parse_UF(zipcode: str) -> str:
    zipcode = str(zipcode)
    zipcode = re.sub("[^0-9]", "", zipcode)

    if len(zipcode) < 3:
        return None

    if zipcode[0] == "1" or zipcode[0] == "0":
        return "SP"


    if zipcode[0] == "2":
      if int(zipcode[0:2]) <= 28:
          return "RJ"
      else:
        return "ES"

    if zipcode[0] == "3":
        return "MG"

    if zipcode[0] == "4":
      if int(zipcode[0:2]) <= 48:
        return "BA"
      else:
        return "SE"

    if zipcode[0] == "5":
      if int(zipcode[0:2]) <= 56:
          return "PE"
      elif int(zipcode[0:2]) == 57:
          return "AL"
      elif int(zipcode[0:2]) == 58:
          return "PB"
      elif int(zipcode[0:2]) == 59:
          return "RN"

    if zipcode[0] == "6":
      if int(zipcode[0:2]) <= 63:
          return "CE"
      elif int(zipcode[0:2]) == 64:
          return "PI"
      elif int(zipcode[0:2]) == 65:
          return "MA"
      elif int(zipcode[0:3]) <= 688:
          return "PA"
      elif int(zipcode[0:3]) == 689:
          return "AP"
      elif int(zipcode[0:3]) <= 692:
          return "AM"
      elif int(zipcode[0:3]) == 693:
          return "RR"
      elif int(zipcode[0:3]) <= 698:
          return "AM"
      elif int(zipcode[0:3]) == 699:
          return "AC"

    if zipcode[0] == "7":
      if int(zipcode[0:3]) <= 736:
          return "DF"
      elif int(zipcode[0:3]) <= 767:
          return "GO"
      elif int(zipcode[0:3]) <= 769:
          return "RO"
      elif int(zipcode[0:3]) <= 779:
          return "TO"
      elif int(zipcode[0:3]) <= 788:
          return "MT"
      elif int(zipcode[0:3]) <= 799:
          return "MS"

    if zipcode[0] == "8":
      if int(zipcode[0:2]) <= 87:
          return "PR"
      else:
          return "SC"

    if zipcode[0] == "9":
      return "RS"
    
    else:
        return "ZZ"

    return None
