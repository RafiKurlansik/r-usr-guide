# Databricks notebook source
# MAGIC %md
# MAGIC # National Parks Explorer
# MAGIC
# MAGIC Feeling like some fresh air?  In this tutorial, you'll use R to create an interactive visualization for planning a trip to National Parks in the USA. Combine data from the [National Parks Service API](https://www.nps.gov/subjects/developer/api-documentation.htm) with weather forecasts from [Open-Meteo.com](https://open-meteo.com/en/docs) to help make your decision! Along the way, you will learn how to use Databricks Notebooks, the Workspace File System, Unity Catalog tables, and Volumes. By the end, you'll have an automated Databricks Workflow to check the latest conditions in the parks. <br><br>
# MAGIC
# MAGIC <img src="https://assets.simpleviewinc.com/simpleview/image/upload/c_limit,h_1200,q_75,w_1200/v1/clients/poconos/skyhawsunmit_Instagram_3636_ig_18304974394032434_3525eaed-3f15-4294-b747-3a54d861303e.jpg">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Park data
# MAGIC
# MAGIC In the next cell we define a function - `get_park_data()` - to fetch data from the National Parks Service API.  This API returns details about the parks, such as their full names, latitude / longitude, descriptions, and other relevant information.  
# MAGIC
# MAGIC The `get_park_data()` function takes two parameters: `state_code` to limit the response to a specific state, and `api_key` to authenticate.  Using the `httr` and `jsonlite` libraries, a GET request is made to the API endpoint (https://developer.nps.gov/api/v1/parks). The JSON response is then parsed and a data.frame is returned. 
# MAGIC
# MAGIC >**Note:** To get an API key, you can sign up for free [here](https://www.nps.gov/subjects/developer/get-started.htm).  It takes less than 5 minutes to get your own key.
# MAGIC

# COMMAND ----------

library(httr)
library(jsonlite)

get_park_data <- function(state_code, api_key) {
  # Define the API endpoint
  url <- "https://developer.nps.gov/api/v1/parks"

  # Define the query parameters
  params <- list(
    stateCode = state_code,
    api_key = api_key
  )

  # Send GET request to the API endpoint with query parameters
  response <- httr::GET(url, query = params, httr::add_headers(accept = "application/json"))

  # Extract the content from the response
  content <- httr::content(response, "text")

  # Parse the JSON content into a data frame
  parks_raw <- jsonlite::fromJSON(content)

  # Access the data frame containing the data
  parks_df <- data.frame(
  lat = parks_raw$data$latitude, 
  lon = parks_raw$data$longitude, 
  Park = parks_raw$data[3]$fullName, 
  Description = parks_raw$data[5]$description)

  return(parks_df)
}

# COMMAND ----------

# Define our params
state_code <- "NJ"
api_key <- "<insert api key>"

# Pass them to our function and retrieve the data 
parks_df <- get_park_data(state_code=state_code, api_key=api_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Displaying data
# MAGIC Now that we've gotten data from the NPS, lets take a look at it.  All of the normal ways of looking at data from the R console will work in a Databricks Notebook - `head()`, `tail()`, `glimpse`, etc. A more powerful way to begin inspecting your data, however, is to use the `display()` function.

# COMMAND ----------

display(parks_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC `display()` will give you an interactive table that can be searched or filtered, with options to copy specific observations to the clipboard or download the data as a flat file.  Tables can also be turned into a [wide variety of visualizations](https://docs.databricks.com/en/visualizations/visualization-types.html) (more on that soon), with support for multiple tabs of visualizations for a single table.
# MAGIC
# MAGIC Of course, you can always use your favorite visualization libraries like `ggplot2` to represent the data however you like.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get weather data
# MAGIC
# MAGIC Now that we have coordinates for National Parks in our state of interest, we can use Open-Meteo's API to get current and forecasted weather conditions. To accomplish this, we'll source a function called `get_weather_data()` in a .R file that lives next to this notebook on the [Workspace File System](https://docs.databricks.com/en/files/workspace.html#what-you-can-do-with-workspace-files) (WSFS).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sourcing files and the Workspace File System
# MAGIC
# MAGIC Databricks provides functionality _similar to local development_ for many workspace file types, including a built-in file editor. In this tutorial, we are working with two files that live next to eachother in the WSFS:
# MAGIC <br><br>
# MAGIC
# MAGIC ```
# MAGIC /Workspace/Users/rafi.kurlansik@databricks.com/r_user_guide_2024
# MAGIC ├── National Parks Explorer
# MAGIC └── get_weather_data.R
# MAGIC ```
# MAGIC
# MAGIC If you check the current working directory with `getwd()`, you'll find yourself in the WSFS path for the notebook you are working in.  Running `dir()` will show you the contents of the working directory.  You can browse the WSFS through the Databricks UI on the side of your notebook and in the left nav bar. <br><br>
# MAGIC
# MAGIC <img src="https://github.com/RafiKurlansik/notebook2/blob/main/assets/workspace_explorer.gif?raw=true">
# MAGIC
# MAGIC The WSFS does not support all use cases for all file types. For example, while you can include images in an imported directory or repository, you cannot embed images in notebooks. A full list of limitations can be found [here](https://docs.databricks.com/en/files/index.html#workspace-files-limitations).
# MAGIC
# MAGIC **Long story short, treat the WSFS like a local file system where you can save code and small arbitrary files.** We'll discuss where to save your data towards the end of the tutorial.

# COMMAND ----------

source("./get_weather_data.R")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine Park and weather data

# COMMAND ----------

# Pass the parks_df to look up weather for the park coordinates on the trip date
# The trip date used is yesterday, you can test with a custom date in the following format : '2024-05-24'
weather_data <- get_weather_data(parks_df, trip_date=Sys.Date()-1) 
display(weather_data)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have a pretty useful dataset in memory - hourly weather data for National Parks - it would be useful to save it somewhere for later use.  This is a good time to introduce **Unity Catalog**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Working with Unity Catalog
# MAGIC
# MAGIC [What is Unity Catalog (UC)?](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#what-is-unity-catalog) It is a unified governance solution for data and AI assets on Databricks, providing centralized access control, auditing, lineage, and data discovery. Practically speaking, as an R user you should think of Unity Catalog as the place to discover, read, and write data and files. You can browse UC from a Databricks Notebook or in the left navigation bar: <br><br>
# MAGIC
# MAGIC <img src="https://github.com/RafiKurlansik/notebook2/blob/main/assets/catalog_explore.gif?raw=true">
# MAGIC
# MAGIC To save data into Unity Catalog, you have two options: **tables** and **volumes**. 
# MAGIC
# MAGIC >**Note:** The Workspace File System is for files you want to manage alongside your code. Unity Catalog is for data that you want to persist and make discoverable for others.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tables
# MAGIC Tables are used to store structured data in Unity Catalog itself. They are similar to tables in a traditional database (Redshift, Microsoft SQL Server, Oracle, etc.), but the processing engine is Apache Spark and the data is stored in cloud object storage (AWS S3, Azure Data Lake Storage, Google Cloud Storage). Tables in Unity Catalog can be accessed using a three-tier identifier:
# MAGIC
# MAGIC `<catalog_name>.<schema_name>.<table_name>`
# MAGIC
# MAGIC  The next cell contains a simple example of how to read and write data using Apache Spark in R through the `sparklyr` package. Keep in mind that there is a lot more to say about how to work with Apache Spark; in this notebook we are barely scratching the surface!
# MAGIC

# COMMAND ----------

library("sparklyr")

# Insert your table name
table_name <- "users.rafi_kurlansik.weather_data"

# Connect to Spark and copy our R data.frame to a Spark DataFrame
sc <- spark_connect(method = "databricks")
weather_data_sdf <- copy_to(sc, weather_data, overwrite = TRUE)

# Save the Spark DataFrame as a table in UC
spark_write_table(
  x = weather_data_sdf, 
  name = table_name,
  mode = "overwrite"
  )

## To read it back, uncomment these lines
# weather_data_sdf2 <- spark_read_table(sc, name = table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC `sparklyr` is the recommended and most intuitive and simple way to use Apache Spark in R.  To grow your skills with the package, we recommend reading and bookmarking [The R in Spark](https://therinspark.com/). 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Volumes
# MAGIC [Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html) represent a logical volume of storage in a cloud object storage location. You can use them to store and access files in any format, including structured, semi-structured, and unstructured data (e.g., flat files, .rds files) in Unity Catalog. Volumes are intended for _path-based data access only_ and cannot be used as a location for tables.  File management operations such as copying paths, downloading files, deleting files, and creating tables can be performed on them.  In general, paths to volumes are constructed as follows:
# MAGIC
# MAGIC `/Volumes/<catalog_name>/<schema_name>/<volume_name>/<path_to_file>`
# MAGIC
# MAGIC If you have permission, you can create a volume from the catalog explorer, or you can use a volume that someone created and shared with you.  <br><br>
# MAGIC
# MAGIC <img src="https://github.com/RafiKurlansik/notebook2/blob/main/assets/create_volume.gif?raw=true">
# MAGIC
# MAGIC To use a volume you will see the path to it mounted on the file system in Databricks: 

# COMMAND ----------

# Replace this with your own path after creating a volume
dir("/Volumes/users/rafi_kurlansik/")

# COMMAND ----------

# MAGIC %md
# MAGIC Writing data to a volume is simple - let's write the `weather_data` to a .csv file in our user folder.

# COMMAND ----------

# Again, replace with your path
volume_path <- "/Volumes/users/rafi_kurlansik/my_files/"
filename <- "weather_data.csv"
write.csv(weather_data, file = paste0(volume_path, filename))

dir(volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Now the weather data is persisted into storage via Unity Catalog Volumes.  We can interact with this file from R like we would any other file.

# COMMAND ----------

# Read the data back in
data_from_volume <- read.csv(paste0(volume_path, filename))
head(data_from_volume)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create interactive data products
# MAGIC We are ready to create the final output of this notebook: a fully interactive visualization with data from the National Parks Service and Open-Meteo.  Before we do so let's discuss how we can parameterize the notebook to make it more _functional_. ;)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameterization with widgets
# MAGIC
# MAGIC There is a special library that is available when working inside of Databricks called `dbutils`.  It contains utilities for working with Databricks - getting secrets, listing files, and for our purposes, creating interactive widgets with `dbutils.widgets`.  The easiest way to think of widgets is as parameters for your notebook; adding widgets to your notebook turns it into a function where values from the widgets can be passed into code that you've written.  
# MAGIC
# MAGIC In the following cell, we'll create a _dropdown_ widget with a list of all state abbreviations in the USA.  We'll also create a _text_ widget that will allow users to input a date.

# COMMAND ----------

# Set up widgets with drop down options
state_codes <- list("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY")

dbutils.widgets.dropdown("state", "NJ", state_codes)
dbutils.widgets.text("date", as.character(Sys.Date()))

# COMMAND ----------

# MAGIC %md
# MAGIC Widget values are accessible with `dbutils.widgets.get()`:

# COMMAND ----------

# Fetch parameters from widgets
state <- dbutils.widgets.get("state")
date <- dbutils.widgets.get("date")

print(state)
print(date)

# COMMAND ----------

# MAGIC %md
# MAGIC In the next cell we pass the `state` and `date` values to get specific park and weather data.  
# MAGIC > **Note:** Make sure to replace the `api_key` with your own value from [here](https://www.nps.gov/subjects/developer/get-started.htm).

# COMMAND ----------

# Auth token for NPS API goes here
api_key <- "<insert_key>"

# Get park data for the state
parks_df <- get_park_data(state_code=state, api_key=api_key)

# Get weather data for the date
weather_data <- get_weather_data(parks_df, trip_date = date)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Data wrangling with `dplyr`
# MAGIC
# MAGIC We can use any 3rd party or custom R package with Databricks, and many of the most [popular ones](https://docs.databricks.com/en/release-notes/runtime/14.3lts.html#installed-r-libraries) are already available as part of [Databricks Runtime](https://docs.databricks.com/en/release-notes/runtime/index.html).  In the next cell, we prepare our forecast data using `dplyr`.  The `park_forecast` dataframe has everything we need to create an interactive visualization. 

# COMMAND ----------

display(weather_data)

# COMMAND ----------

library(dplyr)

# Summarize weather data from hourly to daily, with min-max temps and precipitation probability
park_forecast <- weather_data %>% 
  mutate(
    timestamp = as.POSIXct(time, format = "%Y-%m-%dT%H:%M"), 
    date = as.Date(timestamp)) %>% 
  group_by(date, Park, Description, lat, lon) %>% 
  summarize(
    min_tmp = round(min((temperature_2m*9/5)+32)), 
    max_tmp = round(max((temperature_2m*9/5)+32)),
    precipitation = max(precipitation), 
    precip_prob = paste0(max(precipitation_probability), "%")) %>%
  mutate(
        temp_F = paste(min_tmp, max_tmp, sep = "-")
  ) %>%
  select(Park, lat, lon, date, temp_F, precip_prob, Description)

  display(park_forecast)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks Notebook visualizations
# MAGIC There are a wide variety of visualizations accessible to you as a R developer, including [geospatial](https://docs.databricks.com/en/visualizations/maps.html) options.  Check out the animation below for how to use our `park_forecast` dataframe in conjunction with the `display()` function.
# MAGIC <br><br>
# MAGIC <img src="https://github.com/RafiKurlansik/notebook2/blob/main/assets/create_viz.gif?raw=true">
# MAGIC
# MAGIC Like dark mode?  You can turn it on in the [Notebook settings](https://docs.databricks.com/en/notebooks/notebook-ui.html#view-notebooks-in-dark-mode).
# MAGIC
# MAGIC In the next cell, go ahead and create the visualization yourself.
# MAGIC

# COMMAND ----------

display(park_forecast)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Automate with Databricks Workflows
# MAGIC
# MAGIC [Workflows](https://docs.databricks.com/en/workflows/index.html) is one of the most powerful features on Databricks, allowing you to automate and orchestrate data processing, analytics, and machine learning tasks.  Workflows support automating R notebooks and R scripts (via the [Spark Submit task type](https://docs.databricks.com/en/workflows/jobs/create-run-jobs.html#task-type-options)), and can be accessed on the left menu of the Databricks UI.  
# MAGIC
# MAGIC You can also schedule this tutorial directly from the Notebook interface - simply click on the **Schedule** button in the top right corner of the UI, right next to **Share**.  Then you can configure the task to run on a regular basis, run with different parameters, receive email alerts, and more!  
# MAGIC
# MAGIC <br><br>
# MAGIC <img src="https://github.com/RafiKurlansik/notebook2/blob/main/assets/create_workflow.gif?raw=true">

# COMMAND ----------

# MAGIC %md
# MAGIC # Wrapping up
# MAGIC
# MAGIC To recap what we've learned so far:
# MAGIC - **Databricks Notebooks** are fully compatible with R, tightly integrated into the overall platform, and can be used to develop data products like parameterized, interactive visualizations
# MAGIC - Data is managed in **Unity Catalog**, with support for both traditional tables as you would have in a data warehouse, as well as arbitrary files that you would have on a file system
# MAGIC - **Databricks Workflows** is a powerful and easy to use task automation service
# MAGIC
# MAGIC If you want to continue your learning, we recommend reading the documentation  on [developing code in Databricks](https://docs.databricks.com/en/notebooks/notebooks-code.html#develop-code-in-databricks-notebooks). It includes details on the variable explorer, AI assistant, and so much more. We also recommend learning about [Git integration](https://docs.databricks.com/en/repos/index.html) so that you can link the code you develop in Databricks to version control systems.
# MAGIC
# MAGIC Happy coding!
# MAGIC
# MAGIC

# COMMAND ----------


