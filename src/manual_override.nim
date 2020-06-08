#
# Author: Michael Buchel
# Company: MIM Technology Group Inc.
# Reason: Code to download and update the country mapping.
#
import
  json, parsecsv, streams, strformat,
  strutils, sugar, sequtils, tables,
  httpClient

# Data structure to hold information on the last date from csv files
type
  CoronavirusData = object
    date: string
    city: string
    province: string
    country: string
    admin_level: int
    lat: float
    long: float
    confirm: int
    recover: int
    deaths: int
    confirm_daily: int
    recover_daily: int
    deaths_daily: int
  TimeData = object
    date: string
    confirm: int
    recover: int
    deaths: int
    confirm_daily: int
    recover_daily: int
    deaths_daily: int
  TimeSeries = seq[TimeData]
  Polygon = seq[(float, float)]
  Polygons = seq[Polygon]

# Constants to help simplify coding
const
  BaseURL = "http://ec2-35-153-102-199.compute-1.amazonaws.com/viz/time_series_19-covid"
  ConfirmedURL = BaseURL & "-Confirmed.csv"
  RecoveredURL = BaseURL & "-Recovered.csv"
  DeathURL = BaseURL & "-Deaths.csv"
  SpecialCountries = @["Canada", "US", "Australia"]
  SpecialISO = @["CA", "US", "AU"]
  MapForBorders = toTable({
    "Mayotte": "France", "Curacao": "Netherlands",
    "Reunion": "France", "Bermuda": "United Kingdom",
    "Virgin Islands": "US", "Montserrat": "United Kingdom",
    "Guadeloupe": "France", "French Guiana": "France",
    "Greenland": "Denmark", "France": "France"
  })
  BorderCountries = toTable({
    "Bahamas, The": "Bahamas", "Saint Vincent and the Grenadines": "Saint Vincent and the Grenadines",
    "New Zealand": "New Zealand", "Philippines": "Philippines", "Mauritania": "Mauritania",
    "Congo (Brazzaville)": "Congo"
  })

proc `%`(c: TimeData): JsonNode =
  ## Converts time series data object into json
  result = %*{
    "date": c.date,
    "confirm": c.confirm,
    "recover": c.recover,
    "deaths": c.deaths,
    "confirm_daily": c.confirm_daily,
    "recover_daily": c.recover_daily,
    "deaths_daily": c.deaths_daily
  }

proc cityAndProvince(loc: string): (string, string) =
  ## Inputs
  ## loc - location string, ideally should be "city, province"
  ##
  ## Outputs
  ## result - tuple with city and location
  ##
  ## Reason
  ## Simplifies the change from string into city and location.
  let loc_city_province = split(loc, ", ")
  if len(loc_city_province) == 2:
    result = (loc_city_province[0], loc_city_province[1])
  else:
    result = ("", loc_city_province[0])

proc determineLevel(city: string, province: string, country: string): int =
  ## Inputs
  ## city - city
  ## province - province
  ## country - country
  ##
  ## Outputs
  ## result - admin level
  ##
  ## Reason
  ## Simplifies the resulting admin level
  result = 0
  if province in MapForBorders and MapForBorders[province] == country:
    result = 0
  elif country == "US" and city != "":
    result = 1
  elif province != country and province != "" and not (country in SpecialCountries):
    result = 1
    if city != "":
      result = 2

# If this is the main project
when isMainModule:
  # For getting everything from online
  var client = newHttpClient()

  # Dataset to work with
  var corona_dataset: seq[CoronavirusData] = @[]

  # CSV readers for different CSV files
  var
    confirmed_csv: CsvParser
    recovered_csv: CsvParser
    deaths_csv: CsvParser

  # Hash map to help with time series
  var series = initTable[string, TimeSeries]()

  # JSON files
  var country_json_file = parseJson(readFile("data/merged_map.json"))
  var countries: seq[Polygons] = @[]

  # Ideally this should be a hash table, but using JHU it is too complex
  # for the time being, future goals would be to turn this into a hash
  # table for the future.
  for i in 0 ..< len(country_json_file["features"]):
    var x = country_json_file["features"][i]
    x["properties"] = %*{"iso": x["properties"]["iso"], "name": x["properties"]["name"]}

  # Reads all the csvs
  open(confirmed_csv, newStringStream(getContent(client, ConfirmedURL)), "confirmed.csv")
  open(recovered_csv, newStringStream(getContent(client, RecoveredURL)), "recovered.csv")
  open(deaths_csv, newStringStream(getContent(client, DeathURL)), "deaths.csv")

  # Read all the headers
  readHeaderRow(confirmed_csv)
  readHeaderRow(recovered_csv)
  readHeaderRow(deaths_csv)

  # Gets the header size and the last possible date
  let header_size = len(confirmed_csv.headers)
  let last_date = confirmed_csv.headers[header_size - 1]
  echo(&"Last date updated: {last_date}")

  while readRow(confirmed_csv):
    let (city, province) = cityAndProvince(rowEntry(confirmed_csv, confirmed_csv.headers[0]))
    let country = rowEntry(confirmed_csv, confirmed_csv.headers[1])
    let lat = rowEntry(confirmed_csv, confirmed_csv.headers[2])
    let long = rowEntry(confirmed_csv, confirmed_csv.headers[3])
    let label = rowEntry(confirmed_csv, confirmed_csv.headers[4])
    let level = determineLevel(city, province, country)

    if label == "":
      echo "skip"
      continue

    for i in 0 ..< len(country_json_file["features"]):
      var x = country_json_file["features"][i]
      if strip($x["id"]) == strip(label) or strip(getStr(x["id"])) == strip(label):
        # Helper variables for time series
        var
          conf = 0
          conf_daily = 0

        var s: TimeSeries = @[]

        for i in 5 ..< len(confirmed_csv.headers):
          var
            c = 0
            date = confirmed_csv.headers[i]

          var td: TimeData

          td.date = date

          try:
            c = parseInt(rowEntry(confirmed_csv, date))
          except:
            c = conf
            echo(&"Failure reading confirmed data entry for date: {date}, country: {country}")

          conf_daily = c - conf
          conf = c

          td.confirm = conf
          td.confirm_daily = conf_daily

          s &= td

        let old_iso = x["properties"]["iso"]
        var country_name = &"{label}"
        if country_name in series:
          for j in 0 ..< len(s):
            series[country_name][j].confirm += s[j].confirm
            series[country_name][j].confirm_daily += s[j].confirm_daily
        else:
          series[country_name] = s
        country_json_file["features"][i]["properties"] = %*{
          "iso": old_iso,
          "name": country_json_file["features"][i]["properties"]["name"],
          "date": last_date,
          "city": city,
          "province": province,
          "country": country,
          "admin_level": level,
          "lat": lat,
          "long": long,
          "confirm": 0,
          "recover": 0,
          "deaths": 0,
          "confirm_daily": 0,
          "recover_daily": 0,
          "deaths_daily": 0
        }

  while readRow(recovered_csv):
    let country = rowEntry(recovered_csv, recovered_csv.headers[1])
    let label = rowEntry(recovered_csv, recovered_csv.headers[4])

    if label == "":
      echo "skip"
      continue

    for i in 0 ..< len(country_json_file["features"]):
      var x = country_json_file["features"][i]
      if strip($x["id"]) == strip(label) or strip(getStr(x["id"])) == strip(label):
        # Helper variables for time series
        var
          rcvr = 0
          rcvr_daily = 0

        var s: TimeSeries = @[]

        for i in 5 ..< len(recovered_csv.headers):
          var
            r = 0
            date = recovered_csv.headers[i]

          var td: TimeData

          td.date = date

          try:
            r = parseInt(rowEntry(recovered_csv, date))
          except:
            r = rcvr
            echo(&"Failure reading confirmed data entry for date: {date}, country: {country}")

          rcvr_daily = r - rcvr
          rcvr = r

          td.recover = rcvr
          td.recover_daily = rcvr_daily

          s &= td

        var country_name = &"{label}"
        if country_name in series:
          for j in 0 ..< len(s):
            series[country_name][j].recover += s[j].recover
            series[country_name][j].recover_daily += s[j].recover_daily
        else:
          series[country_name] = s

  while readRow(deaths_csv):
    let country = rowEntry(deaths_csv, deaths_csv.headers[1])
    let label = rowEntry(deaths_csv, deaths_csv.headers[4])

    if label == "":
      echo "skip"
      continue

    for i in 0 ..< len(country_json_file["features"]):
      var x = country_json_file["features"][i]
      if strip($x["id"]) == strip(label) or strip(getStr(x["id"])) == strip(label):
        # Helper variables for time series
        var
          dead = 0
          dead_daily = 0

        var s: TimeSeries = @[]

        for i in 5 ..< len(deaths_csv.headers):
          var
            d = 0
            date = deaths_csv.headers[i]

          var td: TimeData

          td.date = date

          try:
            d = parseInt(rowEntry(deaths_csv, date))
          except:
            d = dead
            echo(&"Failure reading deaths data entry for date: {date}, country: {country}")

          dead_daily = d - dead
          dead = d

          td.deaths = dead
          td.deaths_daily = dead_daily

          s &= td

        var country_name = &"{label}"
        if country_name in series:
          for j in 0 ..< len(s):
            series[country_name][j].deaths += s[j].deaths
            series[country_name][j].deaths_daily += s[j].deaths_daily
        else:
          series[country_name] = s

  for i in 0 ..< len(country_json_file["features"]):
    var x = country_json_file["features"][i]
    for name, s in series:
      if strip($x["id"]) == name or strip(getStr(x["id"])) == name:
        let y = s[len(s) - 1]
        x["properties"]["confirm"] = %y.confirm
        x["properties"]["confirm_daily"] = %y.confirm_daily
        x["properties"]["recover"] = %y.recover
        x["properties"]["recover_daily"] = %y.recover_daily
        x["properties"]["deaths"] = %y.deaths
        x["properties"]["deaths_daily"] = %y.deaths_daily

  writeFile("updated_map.json", $country_json_file)

  for k, v in pairs(series):
    writeFile(k & ".json", $(%v))
