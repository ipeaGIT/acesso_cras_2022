# Carregar bibliotecas
library(arrow)
library(dplyr)
library(ipeadatalake)
library(aopdata)
library(sf)
library(ggplot2)
library(viridis)
library(ggspatial)
library(readr)
library(tidyr)
library(tidygeocoder)


map_acc_CRAS <- function(nome_cidade, tempo_corte) {
  # Lista de cidades com códigos IBGE
  cidades_ibge <- list(
    "Belem" = 1501402,
    "Belo Horizonte" = 3106200,
    "Brasilia" = 5300108,
    "Campinas" = 3509502,
    "Campo Grande" = 5002704,
    "Curitiba" = 4106902,
    "Duque de Caxias" = 3301702,
    "Fortaleza" = 2304400,
    "Goiania" = 5208707,
    "Guarulhos" = 3518800,
    "Maceio" = 2704302,
    "Manaus" = 1302603,
    "Natal" = 2408102,
    "Porto Alegre" = 4314902,
    "Recife" = 2611606,
    "Rio de Janeiro" = 3304557,
    "Salvador" = 2927408,
    "Sao Goncalo" = 3304904,
    "Sao Luis" = 2111300,
    "Sao Paulo" = 3550308
  )
  
  # Obter o código IBGE correspondente
  codigo_ibge <- cidades_ibge[[nome_cidade]]
  
  if (is.null(codigo_ibge)) {
    stop("Cidade não encontrada na lista de cidades disponíveis.")
  }
  
  # Ler dados Cadúnico
  df <- ipeadatalake::read_cadunico(date = 202212, type = 'familia') %>%
    filter(cd_ibge_cadastro %in% c(codigo_ibge)) %>%
    collect()
  
  # Juntar dados Cadúnico com base georreferenciada e calcular número total de pessoas por hexágono
  geo <- arrow::open_dataset('//storage6/bases/DADOS/RESTRITO/CADASTRO_UNICO/geocode/cad_geolocalizado_2022.parquet') %>%
    filter(id_familia %in% df$co_familiar_fam) %>%
    collect()
  
  df_geo <- df %>%
    left_join(geo, by = c("co_familiar_fam" = "id_familia")) %>%
    group_by(h3_res9) %>%
    summarise(total_pessoas = sum(qt_pessoas_domic_fam, na.rm = TRUE), .groups = 'drop') %>%
    dplyr::rename(id_hex = h3_res9)
  
  # Ler dados Acesso a oportunidades
  data_city_acc <- read_grid(city = nome_cidade)
  
  # Carregar matriz de tempo de viagem
  ttm_path <- paste0("L:/data/acesso_oport_v2/travel_time_matrix/res_9/2010/walk/", 
                     codigo_ibge, "_", tolower(gsub(" ", "_", nome_cidade)), ".rds")
  ttm <- readRDS(ttm_path)
  
  # Identificação dos Hexágonos já com CRAS instalado
  caminho_csv <- "L:/git_gregorio/acc_CRAS_cadUnico/data/CENSO_SUAS/Censo_SUAS_2023_CRAS_Dados_Gerais.csv"
  
  # Leitura do arquivo CSV usando base R
  censo_suas <- read.csv(file = caminho_csv, 
                         header = TRUE, 
                         sep = ";", 
                         stringsAsFactors = FALSE, 
                         encoding = "latin1")
  
  cras <- censo_suas %>%
    mutate(capacidade_familias = as.numeric(gsub("[^0-9]", "", q1)),
           capacidade = capacidade_familias * 3.3,
           complete_address = paste(paste(q03, q04, sep = " "), q06, q09, q010, q08, sep = ", "),
           municipio = q09) %>%
    select(NU_IDENTIFICADOR, capacidade_familias, capacidade, complete_address, municipio) %>%
    filter(municipio == nome_cidade)
  
  results <- cras %>%
    geocode(address = complete_address, method = 'arcgis', lat = latitude, long = longitude, full_results = FALSE)
  
  grid <- read_grid(city = nome_cidade)
  
  # Converter o dataframe `results` em um objeto sf
  results_sf <- st_as_sf(results, coords = c("longitude", "latitude"), crs = 4326)
  
  # Garantir que os polígonos (`geom`) estão no mesmo sistema de coordenadas (CRS)
  grid <- st_transform(grid, crs = st_crs(results_sf))
  
  # Realizar o spatial join para encontrar o id_hex correspondente
  results_with_hex <- st_join(results_sf, grid, join = st_within)
  
  # Selecionar apenas as colunas de interesse
  results_with_hex <- results_with_hex %>%
    select(NU_IDENTIFICADOR, id_hex, everything()) %>%
    mutate(C001 = 1)
  
  cras_spatial <- results_with_hex
  cras <- results_with_hex %>% st_drop_geometry() %>%
    select(id_hex, C001)
  
  # Filtrar apenas os tempos de viagem para os hexágonos que possuem CRAS
  ttm_cras <- ttm %>%
    filter(to_id %in% cras$id_hex)
  
  # Identificar o menor tempo de viagem para cada hexágono único em from_id
  min_travel_time <- ttm_cras %>%
    group_by(from_id) %>%
    summarise(min_travel_time = min(travel_time_p50, na.rm = TRUE)) %>%
    ungroup() %>%
    dplyr::rename(id_hex = from_id,
                  TMICT = min_travel_time)
  
  data_city_acc <- read_access(city = nome_cidade, mode = "walk", geometry = TRUE) %>%
    select(id_hex) %>%
    left_join(cras, by = "id_hex") %>%
    replace_na(list(C001 = 0)) %>%
    left_join(min_travel_time, by = "id_hex")
  
  # Juntar CadUnico com AOP
  df_city <- data_city_acc %>%
    left_join(df_geo, by = "id_hex") %>%
    replace_na(list(total_pessoas = 0))
  
  # Fazer o corte de hexágonos dentro do tempo de caminhada
  df_city2 <- df_city %>%
    mutate(status = ifelse(TMICT <= tempo_corte, "in", "out")) %>%
    replace_na(list(status = "out"))
  
  # Calcular o percentual de população "in"
  total_pop <- sum(df_city$total_pessoas, na.rm = TRUE)
  total_in <- sum(df_city2$total_pessoas[df_city2$status == "in"], na.rm = TRUE)
  perc_in <- (total_in / total_pop) * 100
  perc_out <- (100 - perc_in)
  
  # Adicionar rótulos "in" e "out" com o percentual para a legenda
  df_city2 <- df_city2 %>%
    mutate(status_label = ifelse(
      status == "in",
      sprintf("Acessível (%.2f%%)", perc_in),
      sprintf("Não Acessível (%.2f%%)", perc_out)
    ))
  
  # Mapa
  city_map <- ggplot() +
    geom_sf(data = df_city, fill = NA, color = 'gray') +
    geom_sf(data = df_city2, aes(fill = total_pessoas), color = NA) + 
    geom_sf(data = cras_spatial, aes(color = "CRAS"), size = 1, alpha = 0.6, show.legend = TRUE) +
    facet_wrap(. ~ status_label) +
    scale_fill_viridis_c(option = "viridis",
                         name = "Total de pessoas",
                         guide = guide_colourbar(direction = "vertical", title.position = "top", title.hjust = 0.5)) +
    scale_color_manual(values = c("CRAS" = "red"), name = NULL) +
    theme_minimal() +
    theme(
      legend.position = "right",
      legend.title = element_text(size = 10),
      legend.text = element_text(size = 8),
      plot.background = element_rect(fill = "white", color = 'white'),
      plot.margin = unit(c(1, 1, 1, 1), "cm")
    ) +
    labs(title = paste("População cadastrada no Cadúnico a menos e a mais de", tempo_corte, 
                       "minutos de caminhada de um CRAS"),
         caption = "Fonte: Cadúnico e Acesso a Oportunidades, IPEA",
         x = "Longitude",
         y = "Latitude") +
    annotation_scale(location = "br", width_hint = 0.5, pad_x = unit(0.5, "cm"), pad_y = unit(0.5, "cm"))
  
  # Salvar mapa
  ggsave(city_map, filename = paste0('./figures/mapa_', nome_cidade, '_', tempo_corte, 'min.png'),
         width = 30, height = 24, units = 'cm', dpi = 200)
  
  return(city_map)
}

map_acc_CRAS("Curitiba", 30)
map_acc_CRAS("Recife", 30)
map_acc_CRAS("Belo Horizonte", 30)
map_acc_CRAS("Belo Horizonte", 15)
map_acc_CRAS("Campinas", 30)
