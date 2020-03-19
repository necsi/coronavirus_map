#
# Author: Michael Buchel
# Company: MIM Technology Group Inc.
# Reason: Code to download and update the country mapping.
#
import
  json, httpClient, parsecsv, streams, strformat,
  strutils, sugar, sequtils, tables

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
  BaseURL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid"
  ConfirmedURL = BaseURL & "-Confirmed.csv"
  RecoveredURL = BaseURL & "-Recovered.csv"
  DeathURL = BaseURL & "-Deaths.csv"
  SpecialCountries = @["Canada", "US", "Australia"]
  SpecialISO = @["CA", "US", "AU"]

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
  if country == "US" and city != "":
    result = 1
  if province != country and province != "" and not (country in SpecialCountries):
    result = 1
    if city != "":
      result = 2

proc getPolygons(j: JsonNode): Polygons =
  ## Inputs
  ## j - json node to convert to polygon.
  ##
  ## Outputs
  ## result - polygon to for the json node
  ##
  ## Reason
  ## Helps get a polygon for the country.
  result = @[]
  var t = j["geometry"]["coordinates"]
  for x in items(t):
    var p: Polygon = @[]
    for y in items(x):
      if len(y) != 2:
        for z in items(y):
          p &= (getFloat(z[0]), getFloat(z[1]))
      else:
        p &= (getFloat(y[0]), getFloat(y[1]))
    result &= p

proc pointInPolygon(p: CoronavirusData, polygon: Polygon): bool =
  ## Inputs
  ## p - coronavirus data
  ## points - list of points to make a polygon
  ##
  ## Outputs
  ## result - true if it is inside the polygon, false otherwise
  ##
  ## Reason
  ## Abstracts the codebase allowing for simpler code.
  let
    poly_corners = len(polygon)
    poly_x = map(polygon, x => x[0])
    poly_y = map(polygon, x => x[1])
    x = p.long
    y = p.lat

  var j = poly_corners - 1

  result = false
  for i in 0 ..< poly_corners:
    if (poly_y[i] < y and poly_y[j] >= y) or
       (poly_y[j] < y and poly_y[i] >= y):
      if (poly_x[i] + (y - poly_y[i]) / (poly_y[j] - poly_y[i]) * (poly_x[j] - poly_x[i])) < x:
        result = not result
    j = i

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
  var province_json_file = parseJson(readFile("data/provinces.json"))
  var country_json_file = parseJson(readFile("data/countries.json"))
  var countries: seq[Polygons] = @[]
  var times: seq[TimeSeries]

  # Remove canada and us from country file
  country_json_file["features"] = %filterIt(country_json_file["features"], not (getStr(it["properties"]["ISO2"]) in SpecialISO))

  # Remove all none canadian and us provinces
  province_json_file["features"] = %filterIt(province_json_file["features"], getStr(it["properties"]["iso"]) in SpecialISO)

  # Ideally this should be a hash table, but using JHU it is too complex
  # for the time being, future goals would be to turn this into a hash
  # table for the future.
  for i in 0 ..< len(country_json_file["features"]):
    var x = country_json_file["features"][i]
    countries &= getPolygons(x)
    x["properties"] = %*{"iso": x["properties"]["ISO2"]}

  for i in 0 ..< len(province_json_file["features"]):
    var x = province_json_file["features"][i]
    countries &= getPolygons(x)
    x["properties"] = %*{"iso": x["properties"]["iso"]}
    country_json_file["features"] &= x

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
  let second_last_date = confirmed_csv.headers[header_size - 2]
  echo(&"Last date updated: {last_date}")

  # Grouping china together latitude and longitude
  let
    china_lat = 39.922478
    china_long = 116.443710

  # Reads all the files and move into data structure for use
  while readRow(confirmed_csv) and readRow(recovered_csv) and readRow(deaths_csv):
    let (city, province) = cityAndProvince(rowEntry(confirmed_csv, confirmed_csv.headers[0]))
    let country = rowEntry(confirmed_csv, confirmed_csv.headers[1])
    let lat = rowEntry(confirmed_csv, confirmed_csv.headers[2])
    let long = rowEntry(confirmed_csv, confirmed_csv.headers[3])
    let level = determineLevel(city, province, country)

    # Helper variables for time series
    var
      conf = 0
      rcvr = 0
      dead = 0
      conf_daily = 0
      rcvr_daily = 0
      dead_daily = 0

    var s: TimeSeries = @[]

    for i in 4 ..< len(confirmed_csv.headers):
      var
        c = 0
        r = 0
        d = 0
        date = confirmed_csv.headers[i]

      var td: TimeData

      td.date = date

      try:
        c = parseInt(rowEntry(confirmed_csv, date))
      except:
        c = conf
        echo(&"Failure reading confirmed data entry for date: {date}, country: {country}")

      try:
        r = parseInt(rowEntry(recovered_csv, date))
      except:
        r = rcvr
        echo(&"Failure reading recovered data entry for date: {date}, country: {country}")

      try:
        d = parseInt(rowEntry(deaths_csv, date))
      except:
        d = dead
        echo(&"Failure reading deaths data entry for date: {date}, country: {country}")

      conf_daily = c - conf
      rcvr_daily = r - rcvr
      dead_daily = d - dead
      conf = c
      rcvr = r
      dead = d

      td.confirm = conf
      td.recover = rcvr
      td.deaths = dead
      td.confirm_daily = conf_daily
      td.recover_daily = rcvr_daily
      td.deaths_daily = dead_daily

      s &= td

    times &= s

    # Put in the coronavirus dataset
    var corona: CoronavirusData

    corona.date = last_date
    corona.city = city
    corona.province = province
    corona.country = country
    corona.admin_level = level
    corona.confirm = conf
    corona.confirm_daily = conf_daily
    corona.recover = rcvr
    corona.recover_daily = rcvr_daily
    corona.deaths = dead
    corona.deaths_daily = dead_daily

    if corona.country != "Italy":
      try:
        corona.lat = parseFloat(lat)
        corona.long = parseFloat(long)
      except:
        corona.lat = NaN
        corona.long = NaN
    else:
        corona.lat = 41.8719
        corona.long = 12.5674

    corona_dataset &= corona

  close(confirmed_csv)
  close(recovered_csv)
  close(deaths_csv)

  var
    china_confirm = 0
    china_recover = 0
    china_deaths = 0
    china_confirm_daily = 0
    china_recover_daily = 0
    china_deaths_daily = 0
    china_time_series: TimeSeries = @[]

  for i, x in corona_dataset:
    if x.country == "China":
      if china_time_series == @[]:
        china_time_series = times[i]
      else:
        for j in 0 ..< len(times[i]):
          china_time_series[j].confirm += times[i][j].confirm
          china_time_series[j].confirm_daily += times[i][j].confirm_daily
          china_time_series[j].recover += times[i][j].recover
          china_time_series[j].recover_daily += times[i][j].recover_daily
          china_time_series[j].deaths += times[i][j].deaths
          china_time_series[j].deaths_daily += times[i][j].deaths_daily
      delete(times, i)
      if x.confirm != -1:
        china_confirm += x.confirm
      if x.confirm_daily != -1:
        china_confirm_daily += x.confirm_daily
      if x.recover != -1:
        china_recover += x.recover
      if x.recover_daily != -1:
        china_recover_daily += x.recover_daily
      if x.deaths != -1:
        china_deaths += x.deaths
      if x.deaths_daily != -1:
        china_deaths_daily += x.deaths_daily

  var china_corona: CoronavirusData

  china_corona.date = last_date
  china_corona.city = "Chaoyang"
  china_corona.province = "Beijing"
  china_corona.country = "China"
  china_corona.lat = china_lat
  china_corona.long = china_long
  china_corona.confirm = china_confirm
  china_corona.confirm_daily = china_confirm_daily
  china_corona.recover = china_recover
  china_corona.recover_daily = china_recover_daily
  china_corona.deaths = china_deaths
  china_corona.deaths_daily = china_deaths_daily

  corona_dataset = filter(corona_dataset, x => x.country != "China")
  corona_dataset &= china_corona
  times &= china_time_series

  # Finds bounding polygon for dataset
  for ind, c in corona_dataset:
    for i, polygon_list in countries:
      for polygon in polygon_list:
        if c.admin_level == 0 and pointInPolygon(c, polygon):
          let old_iso = country_json_file["features"][i]["properties"]["iso"]
          var country_name = getStr(old_iso)
          if country_name in SpecialISO:
            country_name &= "-" & c.province
          if country_name in series:
            for j in 0 ..< len(times[ind]):
              series[country_name][j].confirm += times[ind][j].confirm
              series[country_name][j].confirm_daily += times[ind][j].confirm_daily
              series[country_name][j].recover += times[ind][j].recover
              series[country_name][j].recover_daily += times[ind][j].recover_daily
              series[country_name][j].deaths += times[ind][j].deaths
              series[country_name][j].deaths_daily += times[ind][j].deaths_daily
          else:
            series[country_name] = times[ind]
          country_json_file["features"][i]["properties"] = %*{
            "iso": old_iso,
            "date": c.date,
            "city": c.city,
            "province": c.province,
            "country": c.country,
            "admin_level": c.admin_level,
            "lat": c.lat,
            "long": c.long,
            "confirm": c.confirm,
            "recover": c.recover,
            "deaths": c.deaths,
            "confirm_daily": c.confirm_daily,
            "recover_daily": c.recover_daily,
            "deaths_daily": c.deaths_daily
          }
  writeFile("updated_map.json", $country_json_file)

  for k, v in pairs(series):
    writeFile(k & ".json", $(%v))
