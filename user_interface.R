
# install.packages("pacman")
library(pacman)
p_load(tidyverse,RPostgreSQL,RSQLite,RODBC,dbplyr,dplyr,ggthemes,viridis,plotly,leaflet)


# choose.dir() %>% setwd()
# getwd()
setwd("C:/Users/Takis/Google Drive/_projects_/flight-hopping")

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, host = '127.0.0.1',
                 dbname = "postgres",
                 user = "postgres",
                 password = "postgres",
                 port = '5432')

# dbWriteTable(con,"iris_data",iris,overwrite=TRUE)
# copy_to(con,mtcars,"db_mtcars",indexes=list("cyl"))

src_dbi(con)
dbListTables(con)
dbListFields(con,"flights_opt")

# df <- dbGetQuery(con,my_query)
# sqlite does not seem to support a partition over by clause out of the box
# tables are precalculated as tables in the sqlite db
#
# flights4k %>% 
#   mutate(diff_stay = abs(Hop_1_duration - Hop_2_duration),
#          total_stay= abs(Hop_2_duration + Hop_1_duration)) %>%
#   filter(diff_stay<(total_stay/2)+1) %>%
#   group_by(d1date) %>% 
#   mutate()
#   summarise(min_price = min(total_list))
# collect() %>% 
# dim()
#
# (rank(), vars_group = "ID")
# vars_order


# FETCH TABLES FROM THE DATABASE ------------------------------------------

main <- tbl(con,"flights_opt") %>% collect()

main %>% dim()
main %>% arrange(RN_date) %>% head() %>% View()
main %>% names()


# LOOK FOR COLORS ---------------------------------------------------------

# palette <- sample(c("#1C8115", "#04044D"), number, replace = TRUE)
# number <- number of distinct values (how many groupings are there)

library("RColorBrewer")
display.brewer.all()
display.brewer.pal(n = 8, name = 'PiYG')
ls("package:ggthemes")[grepl("theme_", ls("package:ggthemes"))]


# SANITY CHECKS -----------------------------------------------------------

main %>% dim()
main %>% filter(td > 4) %>% dim()

main %>% 
  filter(td > 4) %>% 
  filter(h1d > 1, h2d > 1) %>% dim()

main %>% 
  filter(td > 4) %>% 
  filter(h1d > 1, h2d > 1) %>%
  filter(RN_date==1) -> df1

main %>% 
  filter(td > 4) %>% 
  filter(h1d > 1, h2d > 1) %>%
  filter(RN_cdate==1) -> df2

df1 %>% dim #25 47
df2 %>% dim() #300  47

# GENERATE PLOTS ----------------------------------------------------------

# ### Box plot prices per departure date (from origin)

# EFFORT 1
main %>% 
  # mutate(means_day = as.factor(round(ave(main$total_list, as.factor(main$d1date))))) %>%
  mutate(means_day = (round(ave(main$tp, as.factor(main$d1date))))) %>%
  mutate(d1date = as.factor(as.Date(d1date))) %>% 
  ggplot() + 
  aes(x = d1date, y= tp) +
  geom_boxplot(aes(fill = means_day)) +
  # geom_jitter(aes(fill = means_day)) +
  # geom_boxplot() +
  
  # geom_smooth() +
  
  # SCALE COLOURING
  # scale_fill_manual(values=c("#1C8115", "#04044D"))+
  # scale_colour_gradient2()+
  # scale_fill_gradientn(colours = terrain.colors(10))+
  # scale_colour_brewer(palette="PiYG", direction=-1)+
  scale_fill_gradientn(colours = rev(brewer.pal(n = 8, name = 'PiYG')))+
  # scale_color_viridis(option = "D")+
  # scale_color_viridis(discrete = TRUE, option = "D")+
  # scale_color_gradient(low = "green", high = "red")+
  # scale_color_gradientn(colours = rainbow(5))+
  # scale_color_gradient2(midpoint = means_day, low = "blue", mid = "white",
  #                       high = "red", space = "Lab" )+
  # scale_fill_gradient(..., low = "#1C8115", high = "#04044D",
  #                     space = "Lab", na.value = "grey50", guide = "colourbar",
  #                     aesthetics = "fill")
  # THEMES
  # theme_tufte()+
  # theme_economist()+
  # theme_fivethirtyeight()+
  # theme_wsj()+
  # theme_economist_white()+
  # theme_stata()+
  # theme_few()+
  # theme_excel()+
  # theme_foundation()+
  # theme_solarized()+
  theme_pander()+
  # theme_igray()+
  # theme_gdocs()+
  
  # THEME SETTINGS
  theme(axis.text.x = element_text(angle = 60))+
  theme(legend.position="bottom",
        axis.title.x = element_blank(),
        axis.title.y = element_blank()) -> p

pp <- ggplotly(p)
pp

######################################################
##### ADD MIN LABELS, ADD JITTER, LOESS MIN, MAX
######################################################
  

ave(sugg_day_hopcombo_lvl$total_list, as.factor(sugg_day_hopcombo_lvl$d1date)) %>% unique()
ave(sugg_day_hopcombo_lvl$total_list, as.factor(sugg_day_hopcombo_lvl$d1date)) %>% n_distinct()



# AIRPORTS ----------------------------------------------------------------

airports <- read_csv("https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat?fbclid=IwAR3gMnLCInN_1iJwPL7FOlU2GO2iPjnkwbxK7ltXABrrT7HbIYsfHzZyGp0") #col_names 
names(airports) <- c("index","AirportName","City","Country","Code","Code_iso","lat","lon","alt","timezone","dst","tzone","Type","Info")
airports <- airports %>% select(-index)
airports %>% glimpse()
airports %>% head() %>% View()

dbWriteTable(con,"airports",airports,overwrite=TRUE)

# List airports in neo4j
list_airports <- sugg_day_hopcombo_lvl$airportCode_list_h1 %>% unique()
# sugg_day_hopcombo_lvl$airportCode_list_h2 %>% unique()
# sugg_day_hopcombo_lvl$airportCode_list_end %>% unique()

# airport_code = 'ATH'
# airports %>% filter(Code==airport_code) %>% dim()
# airports %>% filter(Code %in% list_airports) %>% select(AirportName, City, Code, lon, lat, tzone) %>% head()
my_airports_df <- airports %>% filter(Code %in% list_airports) %>% select(AirportName, City, Code, lon, lat, tzone)

# TODO : CHECK THIS
# https://rpubs.com/ambra1982/271586
# C:\Users\takis\Desktop\Charm\leaflet.R
# https://datahub.io/core/airport-codes
# https://datasn.io/p/116

# POLYLINES ----------------------------------------------------

rm(list=ls())
data <- data.frame(source_airport=c("MAG","HGU","CDG"),dest_airport=c("DKA","DKA","JFK"),
                   source_airport_longitude=c(145.789,144.296,2.538),
                   source_airport_latitude=c(-5.207080,-5.826790,49.008),
                   dest_airport_longitude=c(145.392,145.392,-73.49),
                   dest_airport_latitude=c(-6.08169,-6.08169,40.38),
                   id=c(1,2,4),stringsAsFactors = F)



flights_lines <- apply(data,1,function(x){
  points <- data.frame(lng=as.numeric(c(x["source_airport_longitude"], 
                                        x["dest_airport_longitude"])),
                       lat=as.numeric(c(x["source_airport_latitude"], 
                                        x["dest_airport_latitude"])),stringsAsFactors = F)
  coordinates(points) <- c("lng","lat")
  Lines(Line(points),ID=x["id"])
})

row.names(data) <- data$id
flights_lines <- SpatialLinesDataFrame(SpatialLines(flights_lines),data)


leaflet() %>%
  addTiles() %>%
  addPolylines(data=flights_lines,label=~as.character(id))

# Customize the Map ------------------------------------------------------------------

customPalette <- colorFactor(c("yellow", "red", "green",
                               "#00274c","cyan","brown",
                               "black","gray"),
                             domain = df$DST)

m <- df %>% 
  filter(Type=="airport") %>% 
  leaflet(data = .) %>% 
  addTiles() %>% 
  setView(25, 39, zoom = 5) %>%
  # choose map style :
  # http://leaflet-extras.github.io/leaflet-providers/preview/index.html
  addProviderTiles("CartoDB.Positron") %>% 
  addCircleMarkers(lng = ~LON,lat = ~LAT,
                   popup = ~paste("<h4>Airport</h4>", Name, 
                                  "<h4>Country</h4>", Country,sep=" ",
                                  "<br>","<br>",
                                  LON, LAT),
                   color = ~customPalette(DST),
                   radius = ~ifelse(DST == "U", 2, 4),
                   weight = 2)

m


