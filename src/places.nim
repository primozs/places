{.push raises: [].}
import pkg/duckdb
import std/options
import std/strutils
import std/httpclient
import zippy/ziparchives
import std/os

proc downloadDb(data_folder: string = "places_db") =
  try:
    data_folder.removeDir()

    if not data_folder.dirExists():
      data_folder.createDir()

    let client = newHttpClient()
    client.downloadFile(
      "https://github.com/primozs/places-db/raw/refs/heads/master/places.zip",
      data_folder / "places.zip"
    )

    extractAll(data_folder / "places.zip", data_folder / "extracted")
    moveFile(data_folder / "extracted" / "places.duckdb", data_folder / "places.duckdb")
    removeDir(data_folder / "extracted")
    removeFile(data_folder / "places.zip")

    defer:
      client.close()
  except Exception as e:
    echo e.repr()


type Places* = object
  db: string
  connection: DuckDBConn

proc connectDb(name: string): DuckDBConn =
  try:
    let dbConn = connect(name)
    return dbConn
  except Exception as e:
    echo e.repr


proc initPlaces*(db: string): Places =
  try:
    let dbConn = connectDb(db)
    dbConn.exec("install spatial")
    dbConn.exec("load spatial")

    result.db = db
    result.connection = dbConn
  except Exception as e:
    echo e.repr()

type Continent* = object
  continent_code*: string
  continent_name*: string

proc queryContinent*(p: Places, lon, lat: float): seq[Continent] =
  let query = """
  select
    cc.continent_code,
    cm.continent_name
  from continents cc
    left join places.main.continents_meta cm on cm.continent_code = cc.continent_code
  where ST_Intersects(cc.geom, ST_Point(?, ?))
  """
  try:
    for item in p.connection.rows(query, lon, lat):
      result.add Continent(continent_code: item[0], continent_name: item[1])
  except Exception as e:
    echo e.repr()

type Country* = object
  country_code*: string
  country_name*: string

proc queryCountry*(p: Places, lon, lat: float): seq[Country] =
  let query = """
  select
    c.country_a2 as country_code,
    cm.country_name
  from countries c
    left join countries_meta cm on cm.country_a2 = c.country_a2
  where ST_Intersects(c.geom, ST_Point(?, ?))
  """
  try:
    for item in p.connection.rows(query, lon, lat):
      result.add Country(country_code: item[0], country_name: item[1])
  except Exception as e:
    echo e.repr()

type Region* = object
  country_code*: string
  region_code*: string
  region_name*: string

proc queryRegion*(p: Places, lon, lat: float): seq[Region] =
  let query = """
  select
    r.country_a2 as country_code,
    r.region_code,
    rm.region_name
  from regions r
    left join regions_meta rm on rm.country_a2 = r.country_a2 and rm.region_code = r.region_code
  where ST_Intersects(r.geom, ST_Point(?, ?))
  """
  try:
    for item in p.connection.rows(query, lon, lat):
      result.add Region(country_code: item[0], region_code: item[1],
          region_name: item[2])
  except Exception as e:
    echo e.repr()

type PlaceData* = object
  name*: string
  country_code*: string
  region_code*: string
  municipality*: string
  elev*: Option[float]
  place_type*: string
  lon*: float
  lat*: float

proc queryPlace*(p: Places, lon, lat: float, distance: int = 500): seq[PlaceData] =
  let query = """
  select
    *
    exclude(id, geom),
    ST_X(geom) as lon,
    ST_Y(geom) as lat
  from places
  where ST_Distance_Spheroid(ST_Point(?, ?), geom) <= ?
  """
  try:
    for item in p.connection.rows(query, lon, lat, distance):
      result.add PlaceData(
        name: item[0],
        country_code: item[1],
        region_code: item[2],
        municipality: item[3],
        elev: if item[4] != "" and item[4] != "NULL": some(item[
            4].parseFloat) else: none(float),
        place_type: item[5],
        lon: item[6].parseFloat,
        lat: item[7].parseFloat
      )
  except Exception as e:
    echo e.repr()


when isMainModule:
  # downloadDb()

  let places = initPlaces("places_db/places.duckdb")
  let res1 = places.queryContinent(14.0330536, 45.3308666)
  assert res1.len == 1
  assert res1[0].continent_code == "EU"

  let res2 = places.queryCountry(14.0330536, 45.3308666)
  assert res2.len == 1
  assert res2[0].country_code == "HR"

  let res3 = places.queryRegion(14.0330536, 45.3308666)
  assert res3.len == 1
  assert res3[0].country_code == "HR"
  assert res3[0].region_code == "HR-IS"

  let res4 = places.queryPlace(14.3496, 45.9081)
  assert res4.len == 2
  assert res4[0].country_code == "SI"
  assert res4[0].region_code == "SI-LJ-BO"

  let res5 = places.queryPlace(14.3496, 45.9081, 10000)
  assert res5.len == 149


