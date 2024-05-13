library(aopdata)
library(dplyr)
library(ggplot2)
library(sf)
library(viridis)
library(tidyr)
library(ggspatial)
library(readr)
library(ggnewscale)
library(ggthemes)
#devtools::install_git("https://gitlab.ipea.gov.br/rafael.pereira/ipeadatalake")
library(ipeadatalake)



### inputs para figuras
my_theme <- theme(
  plot.background = element_rect(fill = "white", color='white'))


caption_fonte <- "Fonte: Censo 2010, via Projeto Acesso a Oportunidades"


#### function to create maps


generate_maps <- function(city, sal_minimo = 510){

  # city <- 'bho'

  ########## Carregar dados do AOP de populacao e acesso ##########

###### Dados acesso ########

data_acc <- read_access(city = city, geometry = TRUE)

# spatial projects to use together with tile
data_acc <- st_transform(data_acc, 3857)

# Criar layer de CRAS
CRAS_centroid <- data_acc |>
                 filter(C001 >= 1) |>
                 st_centroid(CRAS_centroid)



######## Map tiles ##########

map_tile_data <- read_rds(paste0("../../data/acesso_oport/maptiles_crop/2019/mapbox/maptile_crop_mapbox_", city, "_2019.rds"))


map_tile <- ggplot() +
  geom_raster(data = map_tile_data, aes(x, y, fill = hex), alpha = 1) +
  coord_equal() +
  scale_fill_identity() +
  ggnewscale::new_scale_fill()






####### Figura 1 - CRAS e hexágonos com renda média per capita dentro do critério ####



# Criterio CAD unico “renda familiar mensal per capita de\naté meio salário-mínimo”
# Salario mínimo em 2010: R$ 510,00

data_acc <- data_acc |>
            mutate(publico_alvo = ifelse(R001 <= sal_minimo, P001, 0),
                   acesso_15 = ifelse(TMICT > 15, publico_alvo, 0),
                   acesso_30 = ifelse(TMICT > 30, publico_alvo, 0),
                   acesso_45 = ifelse(TMICT > 45, publico_alvo, 0),
                   acesso_60 = ifelse(TMICT > 60, publico_alvo, 0))


  fig1 <- map_tile +
            geom_sf(data = subset(data_acc, publico_alvo>0), aes(fill = publico_alvo), color = NA) +
            geom_sf(data = CRAS_centroid, aes(color = "CRAS"), size = 1, show.legend = TRUE) +
            scale_fill_viridis_c(option = "viridis",
                                 name = "População",
                                 guide = guide_colourbar(direction = "vertical", title.position = "top", title.hjust = 0.5)) +
            scale_color_manual(values = c("CRAS" = "red"), name = "CRAS", labels = "CRAS") +
            labs(title = "Regiões com renda média per capita de\naté meio salário mínimo",
                 caption = caption_fonte) +
            theme_map() +
            theme(legend.position = "right") +
            annotation_scale(location = "br", width_hint = 0.5) +
            my_theme


  ggsave(fig1,  filename = paste0('./figures/mapa_', city, '_1.png'),
         width = 15, height = 15, units = 'cm', dpi = 200)


###### Figura 2 - CRAS e hexágonos com renda média per capita dentro do critério com linha de pobreza de acesso ########

# Belo Horizonte


fig2 <- map_tile +
  geom_sf(data = subset(data_acc, acesso_15>0), aes(fill = acesso_15), color = NA) +
  geom_sf(data = CRAS_centroid, aes(color = "CRAS"), size = 1, show.legend = TRUE) +
  scale_fill_viridis_c(option = "viridis",
                       name = "População",
                       guide = guide_colourbar(direction = "vertical", title.position = "top", title.hjust = 0.5)) +
  scale_color_manual(values = c("CRAS" = "red"), name = "CRAS", labels = "CRAS") +
  labs(title = "Regiões com renda média per capita de\naté meio salário mínimo e\na mais de 15 minutos de caminhada de uma CRAS.",
       caption = caption_fonte) +
  theme_map() +
  theme(legend.position = "right") +
  annotation_scale(location = "br", width_hint = 0.5) +
  my_theme



  ggsave(fig2,  filename = paste0('./figures/mapa_', city, '_2.png'),
         width = 15, height = 15, units = 'cm', dpi = 200)



fig3 <- map_tile +
  geom_sf(data = subset(data_acc, acesso_30>0), aes(fill = acesso_30), color = NA) +
  geom_sf(data = CRAS_centroid, aes(color = "CRAS"), size = 1, show.legend = TRUE) +
  scale_fill_viridis_c(option = "viridis",
                       name = "População",
                       guide = guide_colourbar(direction = "vertical", title.position = "top", title.hjust = 0.5)) +
  scale_color_manual(values = c("CRAS" = "red"), name = "CRAS", labels = "CRAS") +
  labs(title = "Regiões com renda média per capita de\naté meio salário mínimo e \na mais de 30 minutos de caminhada de uma CRAS.",
       caption = caption_fonte) +
  theme_map() +
  theme(legend.position = "right") +
  annotation_scale(location = "br", width_hint = 0.5) +
  my_theme

ggsave(fig3,  filename = paste0('./figures/mapa_', city, '_3.png'),
       width = 15, height = 15, units = 'cm', dpi = 200)


fig4 <- map_tile +
  geom_sf(data = subset(data_acc, acesso_45>0), aes(fill = acesso_45), color = NA) +
  geom_sf(data = CRAS_centroid, aes(color = "CRAS"), size = 1, show.legend = TRUE) +
  scale_fill_viridis_c(option = "viridis",
                       name = "População",
                       guide = guide_colourbar(direction = "vertical", title.position = "top", title.hjust = 0.5)) +
  scale_color_manual(values = c("CRAS" = "red"), name = "CRAS", labels = "CRAS") +
  labs(title = "Regiões com renda média per capita de\naté meio salário mínimo e \na mais de 45 minutos de caminhada de uma CRAS.",
       caption = caption_fonte) +
  theme_map() +
  theme(legend.position = "right") +
  annotation_scale(location = "br", width_hint = 0.5) +
  my_theme

ggsave(fig4,  filename = paste0('./figures/mapa_', city, '_4.png'),
       width = 15, height = 15, units = 'cm', dpi = 200)


fig5 <- map_tile +
  geom_sf(data = subset(data_acc, acesso_60>0), aes(fill = acesso_60), color = NA) +
  geom_sf(data = CRAS_centroid, aes(color = "CRAS"), size = 1, show.legend = TRUE) +
  scale_fill_viridis_c(option = "viridis",
                       name = "População",
                       guide = guide_colourbar(direction = "vertical", title.position = "top", title.hjust = 0.5)) +
  scale_color_manual(values = c("CRAS" = "red"), name = "CRAS", labels = "CRAS") +
  labs(title = "Regiões com renda média per capita de\naté meio salário mínimo e \na mais de 60 minutos de caminhada de uma CRAS.",
       caption = caption_fonte) +
  theme_map() +
  theme(legend.position = "right") +
  annotation_scale(location = "br", width_hint = 0.5) +
  my_theme

ggsave(fig5,  filename = paste0('./figures/mapa_', city, '_5.png'),
       width = 15, height = 15, units = 'cm', dpi = 200)

}


generate_maps(city = 'bho')
generate_maps(city = 'rio')



