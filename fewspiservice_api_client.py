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


import requests
from datetime import datetime, timezone
import pandas
from plotly import express as px
from typing import Tuple

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
  location_id : int,
  parameter_id : str,
  module_instance_id : str = "MGB_Forecast",
  start_time : str = None,
  end_time : str = None) -> dict:
  response = requests.get(
    "https://sstdfews.cicplata.org/FewsWebServices/rest/fewspiservice/v1/timeseries",
    params= {
      "locationIds": location_id,
      "parameterIds": parameter_id,
      "moduleInstanceIds": module_instance_id,
      "startTime": start_time,
      "endTime": end_time,
      "documentFormat": "PI_JSON"
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
  location_id : int,
  parameter_id : str,
  module_instance_id : str = "MGB_Forecast",
  start_time : str = None,
  end_time : str = None,
  timeseries_index : int = 0,
  plot : bool = False) -> Tuple[dict, pandas.DataFrame]:
  timeseries_response = getTimeSeries(location_id, parameter_id, module_instance_id, start_time, end_time)
  if not len(timeseries_response["timeSeries"]):
    raise ValueError("No timeseries found")
  if len(timeseries_response["timeSeries"]) -1 < timeseries_index:
    raise ValueError("timeseries_index not found in response")
  ts = parseTimeSeries(timeseries_response["timeSeries"][timeseries_index])
  if plot:
    plotValues(ts, module_instance_id)
  return ts, valuesToDataFrame(ts["data"]["values"])

