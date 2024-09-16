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

# Função mapa_acc_CRAS
mapa_acc_CRAS <- function(nome_cidade, tempo_corte) {
  if (!is.numeric(tempo_corte) || tempo_corte < 0 || tempo_corte > 120) {
    stop("O 'Tempo de Corte' deve ser um número entre 0 e 120.")
  }
  
  # Lista de cidades e com códigos IBGE
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
  
  # Ler dados CadÚnico
  df <- ipeadatalake::read_cadunico(date = 202212, type = 'familia') %>%
    filter(cd_ibge_cadastro %in% c(codigo_ibge)) %>%
    collect()
  
  # Juntar dados CadÚnico com base georreferenciada e calcular número total de pessoas por hexágono
  geo <- arrow::open_dataset('//storage6/bases/DADOS/RESTRITO/CADASTRO_UNICO/geocode/cad_geolocalizado_2022.parquet') %>%
    filter(id_familia %in% df$co_familiar_fam) %>%
    collect()

  df_geo <- df %>%
    left_join(geo, by = c("co_familiar_fam" = "id_familia")) %>%
    group_by(h3_res9) %>%
    summarise(total_pessoas = sum(qt_pessoas_domic_fam, na.rm = TRUE), .groups = 'drop') %>%
    rename(id_hex = h3_res9)
  
  # Ler dados Acesso a oportunidades
  
  data_city_acc <- read_access(city = nome_cidade, mode = "walk", geometry = TRUE) %>%
    select(id_hex, TMICT, C001) %>%
    mutate(TMICT = pmin(TMICT, 120, na.rm = TRUE))
  
  # Filtrar hexágonos com pelo menos um CRAS
    CRAS_city <- data_city_acc %>% 
    filter(C001 >= 1) %>% 
    st_centroid()
  
  # Juntar CadUnico com AOP
  df_city <- data_city_acc %>%
    left_join(df_geo, by = "id_hex") %>%
    replace_na(list(total_pessoas = 0))
  
  # Fazer o corte de hexágonos dentro do tempo de caminhada
  
  df_city2 <- df_city %>%
    mutate(status = ifelse(TMICT <= tempo_corte, "in", "out"))
  
  # Mapa
  
  city_map <- ggplot() +
    geom_sf(data = df_city, fill = NA, color = 'gray') +
    geom_sf(data = df_city2, aes(fill = total_pessoas), color = NA) + 
    geom_sf(data = CRAS_city, aes(color = "CRAS"), size = 1,  alpha = 0.6, show.legend = TRUE) +
    facet_wrap(. ~ status) +
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
    labs(title = paste("População cadastrada no CadÚnico a menos e a mais de", tempo_corte, "minutos de caminhada de um CRAS"),
         caption = "Fonte: CadÚnico e Acesso a Oportunidades, IPEA",
         x = "Longitude",
         y = "Latitude") +
    annotation_scale(location = "br", width_hint = 0.5, pad_x = unit(0.5, "cm"), pad_y = unit(0.5, "cm"))
  
  return(city_map)
}

# Teste
mapa <- mapa_acc_CRAS("Belo Horizonte", 30)
print(mapa)
