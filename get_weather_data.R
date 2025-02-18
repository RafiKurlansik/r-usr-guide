get_weather_forecast <- function(lat, lon, trip_date) {
  
  httr2::request("https://api.open-meteo.com/v1/forecast") |> 
    httr2::req_url_query(
      latitude = lat,
      longitude = lon,
      current = "temperature_2m,wind_speed_10m",
      hourly = "temperature_2m,precipitation_probability,precipitation,cloud_cover",
      start_date = trip_date,
      end_date = trip_date
    ) |>
    httr2::req_perform() |>
    httr2::resp_body_json()

}

get_weather_data <- function(df, trip_date) {
 
  purrr::pmap(df, function(lat, lon, Park, Description) {

    # get forecast and create tibble
    forecast <- get_weather_forecast(lat, lon, trip_date = trip_date) |>
      purrr::pluck("hourly") |>
      purrr::map(purrr::list_c) |>
      tibble::as_tibble()

    # return forecast
    tibble::tibble(
      Park = Park,
      Description = Description,
      lat = lat, 
      lon = lon,
      forecast = list(forecast)
    )
  }) |>
  purrr::list_rbind() |>
  tidyr::unnest(cols = forecast)

}
