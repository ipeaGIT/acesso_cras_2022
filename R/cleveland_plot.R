library(aopdata)
library(dplyr)
library(ggplot2)
library(sf)
library(viridis)
library(tidyr)
library(ggspatial)
library(readr)
# devtools::install_git("https://gitlab.ipea.gov.br/rafael.pereira/ipeadatalake")
library(ipeadatalake)


cad_geocode_path <- "//storage6/bases/DADOS/RESTRITO/CADASTRO_UNICO/geocode/cad_geolocalizado_2022.parquet"
date_cadunico <- 202212

# Mapeamento de códigos IBGE
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

# Inicializa o dataframe para armazenar todas as cidades
populacao <- data.frame()

for (cidade in names(cidades_ibge)) {
  # Obter código IBGE da cidade
  codigo_ibge <- cidades_ibge[[cidade]]
  
  # 1. Carregar dados do CadÚnico para o Município
  df <- ipeadatalake::read_cadunico(date = date_cadunico, type = 'familia') %>%
    filter(cd_ibge_cadastro %in% codigo_ibge) %>%
    collect()
  
  # 2. Juntar dados do CadÚnico com base georreferenciada do CadÚnico
  geo <- arrow::open_dataset(cad_geocode_path) %>%
    filter(id_familia %in% df$co_familiar_fam) %>%
    collect()
  
  df_geo <- dplyr::left_join(df, geo, by = join_by('co_familiar_fam' == 'id_familia'))
  rm(df, geo)
  
  # 3. Agrupar população do CadÚnico em hexágonos (id_hex)
  df_geo_pop_h3 <- df_geo %>%
    select(qt_pessoas_domic_fam, h3_res9) %>%
    group_by(h3_res9) %>%
    summarise(populacao = sum(qt_pessoas_domic_fam, na.rm = TRUE)) %>%
    dplyr::rename(id_hex = h3_res9)
  rm(df_geo)
  
  # 4. Juntar a população do CadÚnico em hexágonos no grid de hexágonos da cidade
  grid <- read_grid(city = cidade)
  populacao_geom <- left_join(grid, df_geo_pop_h3, by = "id_hex") %>%
    select(id_hex, populacao) %>%
    mutate(populacao = replace_na(populacao, 0))
  
  # Adicionar coluna com o nome do município
  populacao_muni <- populacao_geom %>%
    st_drop_geometry() %>%
    mutate(name_muni = cidade)
  
  # Combinar com o dataframe principal
  populacao <- bind_rows(populacao, populacao_muni)
}


all_cities <- read_access(city = 'all', mode = 'walk', year = 2019, showProgress = FALSE)


# Substituindo valor Inf de tempo de caminhada por 90, substituindo NA da populacao por 0, e filtrando público alvo

all_cities <- all_cities %>%
  left_join(populacao, by = c("id_hex", "name_muni")) %>%
  mutate(TMICT = ifelse(is.infinite(TMICT), 90, TMICT),
         populacao = ifelse(is.na(populacao), 0, populacao))


# Adiciona uma nova coluna para verificar se o acesso à CRAS é feito em menos de 15 minutos
all_cities <- all_cities %>%
  mutate(acesso_15_min = ifelse(TMICT <= 15, 1, 0),
         acesso_30_min = ifelse(TMICT <= 30, 1, 0),
         acesso_45_min = ifelse(TMICT <= 45, 1, 0),
         acesso_60_min = ifelse(TMICT <= 60, 1, 0))

# Agrupa por município e calcula o percentual de pessoas com e sem acesso em 15 minutos
resumo_municipios <- all_cities %>%
  group_by(name_muni) %>%
  summarise(total_pessoas = sum(populacao, na.rm = TRUE),
            acessam_15_min = sum(populacao[acesso_15_min == 1], na.rm = TRUE),
            nao_acessam_15_min = sum(populacao[acesso_15_min == 0], na.rm = TRUE),
            perc_acessam_15_min = acessam_15_min / total_pessoas * 100,
            perc_nao_acessam_15_min = nao_acessam_15_min / total_pessoas * 100,
            acessam_30_min = sum(populacao[acesso_30_min == 1], na.rm = TRUE),
            nao_acessam_30_min = sum(populacao[acesso_30_min == 0], na.rm = TRUE),
            perc_acessam_30_min = acessam_30_min / total_pessoas * 100,
            perc_nao_acessam_30_min = nao_acessam_30_min / total_pessoas * 100,
            acessam_45_min = sum(populacao[acesso_45_min == 1], na.rm = TRUE),
            nao_acessam_45_min = sum(populacao[acesso_45_min == 0], na.rm = TRUE),
            perc_acessam_45_min = acessam_45_min / total_pessoas * 100,
            perc_nao_acessam_45_min = nao_acessam_45_min / total_pessoas * 100,
            acessam_60_min = sum(populacao[acesso_60_min == 1], na.rm = TRUE),
            nao_acessam_60_min = sum(populacao[acesso_60_min == 0], na.rm = TRUE),
            perc_acessam_60_min = acessam_60_min / total_pessoas * 100,
            perc_nao_acessam_60_min = nao_acessam_60_min / total_pessoas * 100)

# Transformar os dados para formato longo
resumo_municipios_long <- resumo_municipios %>%
  select(name_muni, perc_acessam_15_min, perc_acessam_30_min, perc_acessam_45_min, perc_acessam_60_min) %>%
  pivot_longer(
    cols = starts_with("perc_acessam"),
    names_to = "Tempo",
    values_to = "Percentual"
  ) %>%
  mutate(Tempo = case_when(
    Tempo == "perc_acessam_15_min" ~ "15 minutos",
    Tempo == "perc_acessam_30_min" ~ "30 minutos",
    Tempo == "perc_acessam_45_min" ~ "45 minutos",
    Tempo == "perc_acessam_60_min" ~ "60 minutos"
  ))

# Definir as cores manuais para os tempos
cores_tempo <- c(
  "15 minutos" = "#2ca02c",  # Verde
  "30 minutos" = "#1f77b4",  # Azul
  "45 minutos" = "#ff7f0e",  # Laranja
  "60 minutos" = "#d62728"   # Vermelho
)

# Criar o Cleveland Plot ajustado
cleveland_plot <- ggplot(resumo_municipios_long, aes(x = Percentual, y = reorder(name_muni, Percentual), group = name_muni)) +
  # Adicionar a linha cinza conectando os pontos
  geom_line(color = "gray60", size = 0.8, alpha = 0.6) +
  # Adicionar os pontos coloridos
  geom_point(aes(color = Tempo), size = 3) +
  # Definir a escala de cores manualmente
  scale_color_manual(name = "Tempo de Acesso", values = cores_tempo) +
  # Rótulos e título
  labs(
    title = "Proporção de acesso à CRAS por município e tempod e acesso",
    x = "Percentual de População Cadastrada no CadÚnico",
    y = "Municípios"
  ) +
  # Tema minimalista
  theme_minimal() +
  theme(
    axis.text.y = element_text(size = 10),
    axis.title = element_text(size = 12),
    plot.title = element_text(size = 14, hjust = 0.5),
    legend.position = "bottom"
  )

ggsave(cleveland_plot, filename = paste0('./figures/cleveland_plot.png'),
       width = 22, height = 15, units = 'cm', dpi = 200)


