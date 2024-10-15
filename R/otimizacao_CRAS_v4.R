library(dplyr)
library(tidyr)
library(arrow)
library(ipeadatalake)
library(r5r)
library(aopdata)
library(sf)
library(lpSolve)
library(parallel)
library(future)
library(future.apply)
library(ggplot2)
library(viridis)
library(ggspatial)


##### Dados do modelo #####

# 1. Carregar dados do CadÚnico para o Município

df <- ipeadatalake::read_cadunico(date = 202212, type = 'familia') %>%
  filter(cd_ibge_cadastro %in% c(4314902)) %>%
  collect()

# 2. Juntar dados do CadÚnico com base georreferenciada do CadÚnico
geo <- arrow::open_dataset('//storage6/bases/DADOS/RESTRITO/CADASTRO_UNICO/geocode/cad_geolocalizado_2022.parquet') %>%
  filter(id_familia %in% df$co_familiar_fam) %>%
  collect()

df_geo <- dplyr::left_join(df, geo, by = join_by('co_familiar_fam' == 'id_familia'))

rm(df, geo)

# 3. Agrupar população do CadÚnico em hexágonos (id_hex)
df_geo_pop_h3 <- df_geo %>%
  select(qt_pessoas_domic_fam, h3_res9) %>%
  group_by(h3_res9) %>%
  summarise(populacao = sum(qt_pessoas_domic_fam, na.rm = TRUE)) %>%
  rename(id_hex = h3_res9)

rm(df_geo)

# 4. Juntar a população do CadÚnico em hexágonos no grid de hexágonos da cidade

grid <- read_grid(city = "poa")

populacao <- left_join(grid,df_geo_pop_h3, by = "id_hex") %>%
  select(id_hex, populacao) %>%
  mutate(populacao = replace_na(populacao, 0)) %>%
  st_drop_geometry


# 5. Setup do r5r
?r5r()
data_path <- system.file("extdata/poa", package = "r5r")
r5r_core <- setup_r5("L:/git_gregorio/acc_CRAS_cadUnico/data/poa_pbf")

# 6. Definição dos parâmetros para cálculo da matriz de tempo de viagem
points <- grid %>%
  mutate(centroid = st_centroid(geom)) %>% 
  mutate(lat = st_coordinates(centroid)[,2], 
         lon = st_coordinates(centroid)[,1],
         id = id_hex) %>%
  st_drop_geometry() %>%
  select(id, lat, lon)



departure_datetime <- as.POSIXct(
  "13-05-2019 14:00:00",
  format = "%d-%m-%Y %H:%M:%S"
)

# 7. Cálculo da matriz de tempo de viagem

ttm <- travel_time_matrix(
  r5r_core,
  origins = points,
  destinations = points,
  mode = c("WALK"),
  departure_datetime = departure_datetime,
  max_trip_duration = 40
)

rm(points)

# 8. Identificação dos Hexágonos já com CRAS instalado

cras <- read_landuse(city = 'poa', year = 2019, showProgress = FALSE) %>%
  filter(C001 > 0) %>%
  select(id_hex)

# 9. Atribuir capacidade aletória entre 4000 e 7000 aos CRAS existentes
set.seed(123)  # Definir uma seed para reprodutibilidade, se necessário

existing_facilities <- cras %>%
  mutate(capacidade = sample(4000:7000, n(), replace = TRUE))

rm(cras)

# 10. Selecionar lista de hexágonos candidados a receber os novos CRAS
candidate_hex <-  populacao %>%
  #sample_n(100) %>%
  select(id_hex)

##### Modelo de Otimizacao #####

# Função completa para otimização de máxima cobertura
modelo_maxima_cobertura_completo <- function(populacao_df, travel_time, cras_existentes, n_facilities, tempo_corte, candidatos = NULL) {
  
  # Função para identificar hexágonos cobertos pelos CRAS existentes
  identificar_cobertura_existente <- function(travel_time, cras_existentes, tempo_corte) {
    hexagonos_cobertos <- travel_time %>%
      filter(from_id %in% cras_existentes$id_hex & travel_time_p50 <= tempo_corte) %>%
      select(from_id, to_id) %>%
      distinct()
    return(hexagonos_cobertos)
  }
  
  # Função para paralelizar a filtragem
  parallel_filter <- function(df, tempo_corte, n_cores = detectCores() - 1) {
    plan(multisession, workers = n_cores)
    filtered_df <- future_lapply(1:nrow(df), function(i, df, tempo_corte) {
      row <- df[i, ]
      if (row$travel_time_p50 <= tempo_corte) {
        return(row)
      } else {
        return(NULL)
      }
    }, df = df, tempo_corte = tempo_corte)
    return(do.call(rbind, filtered_df))
  }
  
  # Identificar hexágonos cobertos pelos CRAS existentes
  cobertura_existente <- identificar_cobertura_existente(travel_time, cras_existentes, tempo_corte)
  
  # População coberta pelas facilities existentes
  populacao_coberta_existente <- populacao_df %>%
    filter(id_hex %in% cobertura_existente$to_id) %>%
    summarise(total_coberto = sum(populacao)) %>%
    pull(total_coberto)
  
  total_populacao <- sum(populacao_df$populacao)
  
  # Percentual da população coberta pelas facilities existentes
  percentual_existente <- (populacao_coberta_existente / total_populacao) * 100
  
  # Remover hexágonos já cobertos dos dados de população
  populacao_descoberta <- populacao_df %>%
    filter(!id_hex %in% cobertura_existente$to_id)
  
  # Filtrar a matriz de tempo de viagem para manter apenas viagens dentro do corte
  travel_time_filtered <- parallel_filter(travel_time, tempo_corte)
  
  # Se não houver lista de candidatos, usar todos os hexágonos da população como candidatos
  if (is.null(candidatos)) {
    candidatos <- populacao_descoberta %>%
      arrange(desc(populacao)) %>%
      head(50) %>%
      select(id_hex)
  }
  
  # Matriz de variáveis de decisão (sem os hexágonos já cobertos)
  decision_vars <- travel_time_filtered %>%
    filter(from_id %in% candidatos$id_hex) %>%
    filter(to_id %in% populacao_descoberta$id_hex) %>%
    mutate(decision_var = 1:n())
  
  # Definir as variáveis de decisão para o solver (uso do lpSolve)
  n_locs <- nrow(populacao_descoberta)
  n_vars <- nrow(decision_vars)
  
  # Função objetivo: maximizar a população coberta
  obj <- sapply(decision_vars$to_id, function(x) populacao_descoberta$populacao[populacao_descoberta$id_hex == x])
  
  # Restrições:
  cobertura_restr <- matrix(0, nrow = n_locs, ncol = n_vars)
  for (j in 1:n_locs) {
    for (i in 1:n_vars) {
      if (decision_vars$to_id[i] == populacao_descoberta$id_hex[j]) {
        cobertura_restr[j, i] <- 1
      }
    }
  }
  cobertura_rhs <- rep(1, n_locs)
  
  facility_restr <- matrix(0, nrow = 1, ncol = n_vars)
  for (i in 1:n_vars) {
    facility_restr[1, i] <- 1
  }
  facility_rhs <- n_facilities
  
  # Resolver o problema com lpSolve
  constr <- rbind(cobertura_restr, facility_restr)
  rhs <- c(cobertura_rhs, facility_rhs)
  dir <- c(rep("<=", n_locs), "<=")
  
  result <- lp("max", obj, constr, dir, rhs, all.bin = TRUE)
  
  # Identificar os hexágonos com novas facilities
  novas_facilities <- decision_vars$from_id[which(result$solution == 1)]
  
  # População coberta pelas novas facilities
  cobertura_novas <- decision_vars %>%
    filter(from_id %in% novas_facilities) %>%
    group_by(to_id) %>%
    summarise(facility_id = first(from_id))
  
  # Atualizar cobertura existente com 'facility_id'
  cobertura_existente_com_id <- cobertura_existente %>%
    rename(facility_id = from_id, id_hex = to_id)
  
  # População coberta pelas novas facilities
  populacao_coberta_novas <- populacao_df %>%
    filter(id_hex %in% cobertura_novas$to_id) %>%
    summarise(total_coberto = sum(populacao)) %>%
    pull(total_coberto)
  
  percentual_novas <- (populacao_coberta_novas / total_populacao) * 100
  
  # População total coberta (existentes + novas)
  populacao_total_coberta <- populacao_coberta_existente + populacao_coberta_novas
  percentual_total <- (populacao_total_coberta / total_populacao) * 100
  
  # População não coberta
  populacao_nao_coberta <- total_populacao - populacao_total_coberta
  percentual_nao_coberta <- (populacao_nao_coberta / total_populacao) * 100
  
  # Criar o dataframe resumo com 4 colunas
  df_resumo <- bind_rows(
    # Facilities existentes
    cobertura_existente_com_id %>%
      left_join(populacao_df, by = "id_hex") %>%
      mutate(atendimento = "existente") %>%
      select(id_hex, populacao, facility_id, atendimento),
    # Novas facilities
    cobertura_novas %>%
      left_join(populacao_df, by = c("to_id" = "id_hex")) %>%
      rename(id_hex = to_id) %>%
      mutate(atendimento = "nova") %>%
      select(id_hex, populacao, facility_id, atendimento)
  )
  
  # Criar um novo dataframe de resumo por facility_id
  df_summary_by_facility <- df_resumo %>%
    group_by(facility_id, atendimento) %>%
    summarise(populacao_atendida = sum(populacao, na.rm = TRUE), .groups = 'drop')
  
  # Resultado final estruturado
  resumo <- list(
    resumo_texto = paste(
      "Resumo dos Resultados:\n",
      "População coberta pelas facilities existentes: ", populacao_coberta_existente, "\n",
      "Percentual de população coberta pelas facilities existentes: ", round(percentual_existente, 2), "%\n",
      "População coberta pelas novas facilities: ", populacao_coberta_novas, "\n",
      "Percentual de população coberta pelas novas facilities: ", round(percentual_novas, 2), "%\n",
      "População total coberta: ", populacao_total_coberta, "\n",
      "Percentual de população total coberta: ", round(percentual_total, 2), "%\n",
      "População não coberta: ", populacao_nao_coberta, "\n",
      "Percentual de população não coberta: ", round(percentual_nao_coberta, 2), "%\n",
      "Hexágonos das novas facilities: ", paste(novas_facilities, collapse = ", "), "\n"
    ),
    df_resumo = df_resumo,
    df_summary_by_facility = df_summary_by_facility
  )
  
  return(resumo)
}

##### Funcao de geração de mapas #####

# Função para gerar os três mapas
gerar_tres_mapas <- function(populacao, ttm, existing_facilities, novas_facilities, grid, cobertura_detalhada, tempo_corte = 30) {
  
  # Mapa 1: CRAS existentes
  gerar_mapa_acesso_cras <- function() {
    # Calcular a população dentro e fora do tempo de corte
    populacao_coberta <- ttm %>%
      filter(from_id %in% existing_facilities$id_hex) %>%
      mutate(status = ifelse(travel_time_p50 <= tempo_corte, "in", "out")) %>%
      select(to_id, travel_time_p50, status) %>%
      rename(id_hex = to_id)
    
    # Unir com o dataframe de população
    df_poa <- left_join(populacao, populacao_coberta, by = "id_hex")
    df_poa$status[is.na(df_poa$status)] <- "out"
    df_poa_geom <- left_join(grid, df_poa, by = "id_hex")
    
    # Atribuir geometria ao existing_facilities
    existing_facilities_geom <- left_join(existing_facilities, grid, by = "id_hex")
    
    # Criar o mapa
    poa_map <- ggplot() +
      geom_sf(data = df_poa_geom, aes(fill = populacao), color = NA) + 
      geom_sf(data = existing_facilities_geom, aes(geometry = geom), color = "red", size = 1, alpha = 0.6) +
      facet_wrap(. ~ status) +
      scale_fill_viridis_c(option = "viridis",
                           name = "Total de pessoas",
                           guide = guide_colourbar(direction = "vertical", title.position = "top", title.hjust = 0.5)) +
      theme_minimal() +
      theme(
        legend.position = "right",
        legend.title = element_text(size = 10),
        legend.text = element_text(size = 8),
        plot.background = element_rect(fill = "white", color = 'white'),
        plot.margin = unit(c(1, 1, 1, 1), "cm")
      ) +
      labs(title = "População cadastrada no CadÚnico a menos e a mais de 30 minutos de caminhada de um CRAS existente em Porto Alegre",
           caption = "Fonte: CadÚnico e Acesso a Oportunidades, IPEA",
           x = "Longitude",
           y = "Latitude") +
      annotation_scale(location = "br", width_hint = 0.5, pad_x = unit(0.5, "cm"), pad_y = unit(0.5, "cm"))
    
    return(poa_map)
  }
  
  # Mapa 2: Novas facilities
  gerar_mapa_novas_facilities <- function() {
    # Calcular a população dentro e fora do tempo de corte
    populacao_coberta <- ttm %>%
      filter(from_id %in% novas_facilities) %>%
      mutate(status = ifelse(travel_time_p50 <= tempo_corte, "in", "out")) %>%
      select(to_id, travel_time_p50, status) %>%
      rename(id_hex = to_id)
    
    df_poa <- left_join(populacao, populacao_coberta, by = "id_hex")
    df_poa$status[is.na(df_poa$status)] <- "out"
    df_poa_geom <- left_join(grid, df_poa, by = "id_hex")
    
    novas_facilities_geom <- grid %>%
      filter(id_hex %in% novas_facilities) %>%
      mutate(tipo_facility = "nova")
    
    poa_map <- ggplot() +
      geom_sf(data = df_poa_geom, aes(fill = populacao), color = NA) + 
      geom_sf(data = novas_facilities_geom, aes(geometry = geom), color = "red", size = 1, alpha = 0.6) +
      facet_wrap(. ~ status) +
      scale_fill_viridis_c(option = "viridis",
                           name = "Total de pessoas",
                           guide = guide_colourbar(direction = "vertical", title.position = "top", title.hjust = 0.5)) +
      theme_minimal() +
      theme(
        legend.position = "right",
        legend.title = element_text(size = 10),
        legend.text = element_text(size = 8),
        plot.background = element_rect(fill = "white", color = 'white'),
        plot.margin = unit(c(1, 1, 1, 1), "cm")
      ) +
      labs(title = "População cadastrada no CadÚnico a menos e a mais de 30 minutos de caminhada dos novos CRAS em Porto Alegre",
           caption = "Fonte: CadÚnico e Acesso a Oportunidades, IPEA",
           x = "Longitude",
           y = "Latitude") +
      annotation_scale(location = "br", width_hint = 0.5, pad_x = unit(0.5, "cm"), pad_y = unit(0.5, "cm"))
    
    return(poa_map)
  }
  
  # Mapa 3: Cobertura por CRAS (novos e existentes)
  gerar_mapa_cobertura <- function() {
    # Juntar o grid com o dataframe cobertura_detalhada
    dados <- left_join(grid, cobertura_detalhada, by = "id_hex")
    
    dados <- dados %>%
      mutate(tipo_cobertura = case_when(
        atendimento == "existente" ~ "Coberto por CRAS Existente",
        atendimento == "nova" ~ "Coberto por CRAS Novo",
        TRUE ~ NA_character_
      ))
    
    # Criar o mapa
    poa_map <- ggplot() +
      geom_sf(data = dados, aes(geometry = geom, fill = tipo_cobertura), color = NA, show.legend = TRUE) +
      scale_fill_manual(name = "Cobertura",
                        values = c("Coberto por CRAS Existente" = "blue", 
                                   "Coberto por CRAS Novo" = "red"),
                        labels = c("CRAS Existente", "CRAS Novo")) +
      geom_sf(data = filter(dados, atendimento == "existente"), 
              aes(geometry = geom), fill = NA, color = "white", size = 2, show.legend = FALSE) +
      geom_sf(data = filter(dados, atendimento == "nova"), 
              aes(geometry = geom), fill = NA, color = "black", size = 2, show.legend = FALSE) +
      theme_minimal() +
      labs(title = "Mapa de Cobertura por CRAS",
           subtitle = "Hexágonos cobertos por CRAS novos (vermelho) e existentes (azul)",
           fill = "Tipo de Cobertura") +
      theme(legend.position = "right",
            legend.box = "vertical",
            legend.title = element_text(size = 10),
            legend.text = element_text(size = 9))
    
    return(poa_map)
  }
  
  # Gerar os três mapas
  mapa_existentes <- gerar_mapa_acesso_cras()
  mapa_novas <- gerar_mapa_novas_facilities()
  mapa_cobertura <- gerar_mapa_cobertura()
  
  # Retornar os três mapas
  return(list(mapa_existentes = mapa_existentes, mapa_novas = mapa_novas, mapa_cobertura = mapa_cobertura))
}


#Testando o Modelo

resultado <- modelo_maxima_cobertura_completo(populacao, ttm, existing_facilities, 3, 30, candidate_hex)
  
# Gerar o mapa incluindo apenas as novas facilities instaladas
novas_facilities <- resultado$df_resumo %>% filter(atendimento == "nova") %>% pull(facility_id)

# Gerar os três mapas

mapas <- gerar_tres_mapas(populacao, ttm, existing_facilities, novas_facilities, grid, resultado$df_resumo)

# Exibir os três mapas
print(mapas$mapa_existentes)
print(mapas$mapa_novas)
print(mapas$mapa_cobertura)

cat(resultado$resumo_texto)



