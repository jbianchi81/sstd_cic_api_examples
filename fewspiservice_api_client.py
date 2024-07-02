"""Example of use:

ts, values_df = getAndParseTimeSeries(
    6362,
    "Q.sim",
    "MGB_Forecast",
    "2024-01-01T00:00:00Z",
    "2025-03-20T00:00:00Z",
    plot = True
)
"""

# test page: https://sstdfews.cicplata.org/FewsWebServices/test/fewspiservicerest/test.html

import requests
from datetime import datetime, timezone
import pandas
from plotly import express as px
from typing import Tuple, List, Union

def parseDate(date : dict):
    """parse date-time dict

    Args:
        date (dict): {"date" : str, "time" : str}

    Returns:
        datetime: datetime in utc timezone
    """
    d = [int(x) for x in date["date"].split("-")]
    t = [int(x) for x in date["time"].split(":")]
    return datetime(d[0],d[1],d[2],t[0],t[1],t[2], tzinfo=timezone.utc)

# request data for 1 location + 1 variable (/timeseries)

def getTimeSeries(
  filter_id : str = None,
  location_ids : Union[int,List[int]] = None,
  parameter_ids : Union[int,List[int]] = None,
  module_instance_ids : Union[str,List[str]] = None, # "MGB_Forecast"
  qualifier_ids : Union[str,List[str]] = None,
  task_run_ids : Union[int,List[int]] = None,
  start_time : str = None,
  end_time : str = None,
  start_creation_time : str = None,
  end_creation_time : str = None,
  forecast_count : int = None,
  start_forecast_time : str = None,
  end_forecast_time : str = None,
  external_forecast_times : str = None,
  ensemble_id : int = None,
  ensemble_member_id : int = None,
  time_step_id : str = None,
  thinning : int = None,
  export_id_map : str = None,
  export_unit_conversion_id : str = None,
  time_zone_name : str = None,
  time_series_set_index : int = None,
  default_request_parameters_id : str = None,
  match_as_qualifier_set : bool = None,
  import_from_external_data_source : bool = None,
  convert_datum : bool = None,
  show_ensemble_members_id : bool = None,
  use_display_units : bool = None,
  show_thresholds : bool = None,
  omit_missing : bool = None,
  omit_empty_time_series : bool = None,
  only_manual_edits : bool = None,
  only_headers : bool = None,
  only_forecasts : bool = None,
  show_statistics : bool = None,
  use_milliseconds : bool = None,
  show_products : bool = None,
  time_series_type : str = None,
  document_format : str = None,
  document_version : str = None
  ) -> dict:
  response = requests.get(
    "https://sstdfews.cicplata.org/FewsWebServices/rest/fewspiservice/v1/timeseries",
    params= {
      "filterId": filter_id,
      "locationIds": location_ids,
      "parameterIds": parameter_ids,
      "moduleInstanceIds": module_instance_ids, # "MGB_Forecast"
      "qualifierIds": qualifier_ids,
      "taskRunIds": task_run_ids,
      "startTime": start_time,
      "endTime": end_time,
      "startCreationTime": start_creation_time,
      "endCreationTime": end_creation_time,
      "forecastCount": forecast_count,
      "startForecastTime": start_forecast_time,
      "endForecastTime": end_forecast_time,
      "externalForecastTime": external_forecast_times,
      "ensembleId": ensemble_id,
      "ensembleMemberId": ensemble_member_id,
      "timeStepId": time_step_id,
      "thinning": thinning,
      "exportIdMap": export_id_map,
      "exportUnitConversionId": export_unit_conversion_id,
      "timeZoneName": time_zone_name,
      "timeSeriesSetIndex": time_series_set_index,
      "defaultRequestParametersId": default_request_parameters_id,
      "matchAsQualifierSet": match_as_qualifier_set,
      "importFromExternalDataSource": import_from_external_data_source,
      "convertDatum": convert_datum,
      "showEnsembleMembersId": show_ensemble_members_id,
      "useDisplayUnits": use_display_units,
      "showThresholds": show_thresholds,
      "omitMissing": omit_missing,
      "omitEmptyTimeSeries": omit_empty_time_series,
      "onlyManualEdits": only_manual_edits,
      "onlyHeaders": only_headers,
      "onlyForecasts": only_forecasts,
      "showStatistics": show_statistics,
      "useMilliseconds": use_milliseconds,
      "showProducts": show_products,
      "timeSeriesType": time_series_type,
      "documentFormat": document_format,
      "documentVersion": document_version
  })
  return response.json()

def parseTimeSeries(ts : dict) -> dict:
  """Parse timeseries response

  Args:
      ts (dict): FewsWebService /timeseries response item (response["timeSeries"][i])

  Returns:
      dict: dict with valuable metadata and time-value pairs
  """
  variable = {
      "id": ts["header"]["parameterId"],
      "dt": {
      "seconds": int(ts["header"]["timeStep"]["multiplier"])
      },
      "units": ts["header"]["units"]
  }
  station = {
      "id": ts["header"]["locationId"],
      "name": ts["header"]["stationName"],
      "geometry": {
      "type": "Point",
      "coordinates": [
          float(ts["header"]["lon"]),
          float(ts["header"]["lat"])
      ]
      }
  }
  data = {
      "date": parseDate(ts["header"]["forecastDate"]),
      "values": [
          {
              "date": parseDate({"date": x["date"], "time": x["time"]}),
              "value": float(x["value"])
          } 
          for x in ts["events"] if float(x["value"]) != -999
      ]
  }
  return {
      "variable": variable,
      "station": station,
      "data": data
  }

def valuesToDataFrame(l : list) -> pandas.DataFrame:
  return pandas.DataFrame([
    {
        "date": v["date"],
        "value": v["value"]
    }
    for v in l
  ])

def plotValues(ts, module_instance_id = "MGB_Forecast") -> None:
  title = "%s - %s [%s] - %s [%s]" % (module_instance_id, ts["variable"]["id"], ts["variable"]["units"], ts["station"]["name"], ts["station"]["id"]) # 'MGB Q.sim - Rosario'
  fig = px.line(valuesToDataFrame(ts["data"]["values"]).head(-1), x="date", y="value", title=title)
  fig.show()

def getAndParseTimeSeries(
  filter_id : str = None,
  location_ids : Union[int,List[int]] = None,
  parameter_ids : Union[int,List[int]] = None,
  module_instance_ids : Union[str,List[str]] = None, # "MGB_Forecast"
  qualifier_ids : Union[str,List[str]] = None,
  task_run_ids : Union[int,List[int]] = None,
  start_time : str = None,
  end_time : str = None,
  start_creation_time : str = None,
  end_creation_time : str = None,
  forecast_count : int = None,
  start_forecast_time : str = None,
  end_forecast_time : str = None,
  external_forecast_times : str = None,
  ensemble_id : int = None,
  ensemble_member_id : int = None,
  time_step_id : str = None,
  thinning : int = None,
  export_id_map : str = None,
  export_unit_conversion_id : str = None,
  time_zone_name : str = None,
  time_series_set_index : int = None,
  default_request_parameters_id : str = None,
  match_as_qualifier_set : bool = None,
  import_from_external_data_source : bool = None,
  convert_datum : bool = None,
  show_ensemble_members_id : bool = None,
  use_display_units : bool = None,
  show_thresholds : bool = None,
  omit_missing : bool = None,
  omit_empty_time_series : bool = None,
  only_manual_edits : bool = None,
  only_headers : bool = None,
  only_forecasts : bool = None,
  show_statistics : bool = None,
  use_milliseconds : bool = None,
  show_products : bool = None,
  time_series_type : str = None,
  document_format : str = None,
  document_version : str = None
  ) -> List[dict]:
  timeseries_response = getTimeSeries(
    filter_id,
    location_ids,
    parameter_ids,
    module_instance_ids, # "MGB_Forecast"
    qualifier_ids,
    task_run_ids,
    start_time,
    end_time,
    start_creation_time,
    end_creation_time,
    forecast_count,
    start_forecast_time,
    end_forecast_time,
    external_forecast_times,
    ensemble_id,
    ensemble_member_id,
    time_step_id,
    thinning,
    export_id_map,
    export_unit_conversion_id,
    time_zone_name,
    time_series_set_index,
    default_request_parameters_id,
    match_as_qualifier_set,
    import_from_external_data_source,
    convert_datum,
    show_ensemble_members_id,
    use_display_units,
    show_thresholds,
    omit_missing,
    omit_empty_time_series,
    only_manual_edits,
    only_headers,
    only_forecasts,
    show_statistics,
    use_milliseconds,
    show_products,
    time_series_type,
    document_format,
    document_version
  )
  if not len(timeseries_response["timeSeries"]):
    raise ValueError("No timeseries found")
  timeseries = [parseTimeSeries(ts) for ts in timeseries_response["timeSeries"]]
  return timeseries

def getLocations(
  filter_id: str = None,
  parameter_ids : str = None,
  parameter_group_id : str = None,
  show_attributes : bool = None,
  include_location_relations : bool = None,
  include_time_dependency : bool = None,
  # document_format : str = None,
  document_version : str = None
  ) -> dict:
  response = requests.get(
    "https://sstdfews.cicplata.org/FewsWebServices/rest/fewspiservice/v1/locations",
    params= {
      "filterId": filter_id,
      "parameterIds" : parameter_ids,
      "parameterGroupId": parameter_group_id,
      "showAttributes": show_attributes,
      "includeLocationRelations": include_location_relations,
      "includeTimeDependency": include_time_dependency,
      "documentFormat": "PI_JSON", # document_format,
      "documentVersion": document_version
    })
  return response.json()

def parseLocations(
  locations : List[dict]
  ):

  sites = []

  for location in locations:
    # {'locationId': '1',
    # 'shortName': '- MiniBacia.MGB.1',
    # 'lat': '-14.336580160150447',
    # 'lon': '-56.833167156626416',
    # 'x': '-56.833167156626416',
    # 'y': '-14.336580160150447',
    # 'z': '0.0',
    # 'attributes': []}
    sites.append({
      "id": location["locationId"],
      "name": location["shortName"],
      "geometry": {
        "type": "Point",
        "coordinates": [
          float(location["lon"]),
          float(location["lat"])
        ]
      }
    })
  return sites

def locationsToDataFrame(l : list) -> pandas.DataFrame:
  return pandas.DataFrame([
    {
      "id": v["id"],
      "name": v["name"],
      "lon": v["geometry"]["coordinates"][0],
      "lat": v["geometry"]["coordinates"][1]
    }
    for v in l
  ])

def getAndParseLocations(
  filter_id: str = None,
  parameter_ids : str = None,
  parameter_group_id : str = None,
  show_attributes : bool = None,
  include_location_relations : bool = None,
  include_time_dependency : bool = None,
  # document_format : str = None,
  document_version : str = None
  ) -> list:
  locations_response = getLocations(
    filter_id,
    parameter_ids,
    parameter_group_id,
    show_attributes,
    include_location_relations,
    include_time_dependency,
    # document_format : str = None,
    document_version 
  )
  if not len(locations_response["locations"]):
    raise ValueError("No locations found")
  return parseLocations(locations_response["locations"])
