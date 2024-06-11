import requests
from datetime import datetime, timezone
import pandas
from plotly import express as px

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

# request data

start_time = "2024-01-01T00:00:00Z"
end_time = "2025-03-20T00:00:00Z"

response = requests.get(
  "https://sstdfews.cicplata.org/FewsWebServices/rest/fewspiservice/v1/timeseries/displaygroups",
  params= {
    "plotId": "MGB",
    "startTime": start_time,
    "endTime": end_time,
    "documentFormat": "PI_JSON"
  })

display_groups = response.json()

# parse results into list of dict

series = []
for ts in display_groups["timeSeries"]:
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
  forecast = {
    "date": parseDate(ts["header"]["forecastDate"]),
    "values": [
        {
            "date": parseDate({"date": x["date"], "time": x["time"]}),
            "value": float(x["value"])
        } 
        for x in ts["events"] if float(x["value"]) != -999
    ]
  }
  series.append({
    "variable": variable,
    "station": station,
    "forecast": forecast
  })

# series metadata to dataframe

series_df = pandas.DataFrame(
    [
        {
            "parameterId": ts["header"]["parameterId"],
            "units": ts["header"]["units"],
            "timeStep_unit": ts["header"]["timeStep"]["unit"],
            "timeStep_multiplier": int(ts["header"]["timeStep"]["multiplier"]),
            "startDate": parseDate(ts["header"]["startDate"]),
            "endDate": parseDate(ts["header"]["endDate"]),
            "locationId": ts["header"]["locationId"],
            "stationName": ts["header"]["stationName"],
            "lon": float(ts["header"]["lon"]),
            "lat": float(ts["header"]["lat"]),
            "forecastDate": parseDate(ts["header"]["forecastDate"])
        }
        for ts in display_groups["timeSeries"]
    ]
)

# filter by parameterId, locationId and convert to dict

location_id = "5965"
parameter_id = "Q.sim"

serie = series_df[series_df.locationId==location_id][series_df.parameterId==parameter_id].reset_index().to_dict(orient='records')[0]

# forecast to dataframe

forecast_df = pandas.DataFrame([
    {
        "date": v["date"],
        "value": v["value"]
    }
    for v in series[serie["index"]]["forecast"]["values"]
])

# plot

title = 'MGB Q.sim - Ladario'

fig = px.line(forecast_df.head(-1), x="date", y="value", title=title)
fig.show()