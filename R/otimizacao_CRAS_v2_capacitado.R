library(lpSolve)
library(dplyr)
library(arrow)
library(ipeadatalake)
library(ompr)
library(ompr.roi)
library(ROI.plugin.glpk)
library(r5r)
library(aopdata)
library(sf)
library(h3jsr)
library(ggplot2)
library(viridis)

##### Dados do modelo #####
### Populacao que precisa ser atendida

df <- ipeadatalake::read_cadunico(date = 202212, type = 'familia') %>%
  filter(cd_ibge_cadastro %in% c(4314902)) %>%
  collect()

# Juntar dados CadÚnico com base geo
geo <- arrow::open_dataset('//storage6/bases/DADOS/RESTRITO/CADASTRO_UNICO/geocode/cad_geolocalizado_2022.parquet') %>%
  filter(id_familia %in% df$co_familiar_fam) %>%
  collect()

df_geo <- dplyr::left_join(df, geo, by = join_by('co_familiar_fam' == 'id_familia'))

# Selecionar população e agrupar por id_hex
df_geo_pop_h3 <- df_geo %>%
  select(qt_pessoas_domic_fam, h3_res9) %>%
  group_by(h3_res9) %>%
  summarise(populacao = sum(qt_pessoas_domic_fam, na.rm = TRUE)) %>%
  rename(id_hex = h3_res9)

grid <- read_grid(city = "poa")

populacao <- left_join(grid,df_geo_pop_h3, by = "id_hex") %>%
  select(id_hex, populacao) %>%
  mutate(populacao = replace_na(populacao, 0)) %>%
  st_drop_geometry

### Matriz tempo de viagem
r5r_core <- setup_r5("L:/git_gregorio/acc_CRAS_cadUnico/data/poa_pbf")
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

ttm <- travel_time_matrix(
  r5r_core,
  origins = points,
  destinations = points,
  mode = c("WALK"),
  departure_datetime = departure_datetime,
  max_trip_duration = 40
)

# CRAS existentes

cras <- read_landuse(city = 'poa', year = 2019, showProgress = FALSE) %>%
  filter(C001 > 0) %>%
  select(id_hex, C001) %>%
  rename(facilities = C001)

# Gerar números aleatórios entre 4000 e 7000 para cada facility existente
set.seed(123)  # Definir uma seed para reprodutibilidade, se necessário

cras <- cras %>%
  mutate(capacidade = sample(4000:7000, n(), replace = TRUE))

# Selecionar Candidatos
candidatos <-  populacao %>%
  #sample_n(100) %>%
  select(id_hex)


##### Definicao do modelo #####

maxima_cobertura_lp <- function(n_facilities, existing_facilities, capacidade_novas_facilities, populacao, distancia, corte_tempo, candidatos_especificos = NULL) {
  
  # 1. Filtrar as distâncias que respeitam o corte de tempo
  distancia_filtrada <- distancia[distancia$travel_time_p50 <= corte_tempo, ]
  
  # 2. Priorizar atendimento pela proximidade para facilities existentes
  distancia_existentes <- distancia_filtrada[distancia_filtrada$from_id %in% existing_facilities$id_hex, ]
  distancia_existentes <- distancia_existentes[order(distancia_existentes$from_id, distancia_existentes$travel_time_p50), ]
  
  # Inicializar o dataframe de população com as colunas 'atendida', 'facility_id', e 'tipo_facility'
  populacao$atendida <- 0
  populacao$facility_id <- NA
  populacao$tipo_facility <- NA  # Para indicar se a facility é existente ou nova
  
  # 3. Atribuir capacidade de atendimento para cada facility existente
  for (i in seq_len(nrow(existing_facilities))) {
    facility_id <- existing_facilities$id_hex[i]
    capacidade <- existing_facilities$capacidade[i]
    
    # Encontrar hexágonos de usuários dentro do tempo de corte da facility
    hexagonos_proximos <- distancia_existentes[distancia_existentes$from_id == facility_id, "to_id"]
    
    for (hex in hexagonos_proximos$to_id) {
      # Verificar população do hexágono e capacidade restante da facility
      populacao_hex <- populacao[populacao$id_hex == hex, "populacao"]
      populacao_atendida <- populacao[populacao$id_hex == hex, "atendida"]
      capacidade_restante <- capacidade - sum(populacao$atendida[populacao$id_hex == facility_id], na.rm = TRUE)
      
      if (capacidade_restante <= 0) {
        break  # Se a capacidade foi esgotada, parar
      }
      
      # Atender parcialmente ou totalmente de acordo com a capacidade restante
      if (populacao_hex - populacao_atendida <= capacidade_restante) {
        populacao$atendida[populacao$id_hex == hex] <- populacao_hex
        populacao$facility_id[populacao$id_hex == hex] <- facility_id
        populacao$tipo_facility[populacao$id_hex == hex] <- "existente"
        capacidade <- capacidade - (populacao_hex - populacao_atendida)
      } else {
        populacao$atendida[populacao$id_hex == hex] <- populacao_atendida + capacidade_restante
        populacao$facility_id[populacao$id_hex == hex] <- facility_id
        populacao$tipo_facility[populacao$id_hex == hex] <- "existente"
        capacidade <- 0  # Esgotou a capacidade
      }
    }
  }
  
  # 4. Restante da população que não foi atendida
  populacao_nao_atendida <- populacao %>% filter(populacao > atendida)
  
  # 5. Criar uma lista de candidatos a receber novas facilities
  if (is.null(candidatos_especificos)) {
    candidatos <- unique(distancia_filtrada$from_id)
  } else {
    candidatos <- candidatos_especificos
  }
  
  hexagonos <- unique(distancia_filtrada$to_id)
  
  n_hexagonos <- length(hexagonos)
  n_candidatos <- length(candidatos)
  
  # 6. Construir a matriz de cobertura (binária)
  matriz_cobertura <- matrix(0, nrow = n_hexagonos, ncol = n_candidatos)
  
  for (i in 1:nrow(distancia_filtrada)) {
    from_idx <- which(candidatos == distancia_filtrada$from_id[i])
    to_idx <- which(hexagonos == distancia_filtrada$to_id[i])
    
    if (length(from_idx) > 0 && length(to_idx) > 0) {
      matriz_cobertura[to_idx, from_idx] <- 1
    }
  }
  
  # 7. Criar a função objetivo: Maximizar a população coberta não atendida
  populacao_ordenada <- populacao_nao_atendida[match(hexagonos, populacao_nao_atendida$id_hex), "populacao"]
  populacao_ordenada[is.na(populacao_ordenada)] <- 0
  
  # Função objetivo com base na capacidade das novas facilities
  capacidades_novas <- capacidade_novas_facilities  # Exemplo: c(5000, 6000, 7000)
  
  funcao_objetivo_ajustada <- rep(0, n_candidatos)
  
  for (j in 1:n_candidatos) {
    capacidade_restante <- capacidades_novas[min(j, length(capacidades_novas))]  # Alocar capacidade fixa
    populacao_total <- sum(matriz_cobertura[, j] * populacao_ordenada)
    
    # Limitar a população coberta pela capacidade restante da nova facility
    funcao_objetivo_ajustada[j] <- min(populacao_total, capacidade_restante)
  }
  
  # 8. Definir restrições e resolver o modelo
  restricao_1 <- rep(1, n_candidatos)  # Somente `n_facilities` facilities podem ser instaladas
  restricao_2 <- matriz_cobertura  # Cada hexágono só pode ser coberto por uma facility
  
  # Resolver o modelo de otimização
  resultado <- lp(direction = "max", 
                  objective.in = funcao_objetivo_ajustada, 
                  const.mat = rbind(restricao_1, restricao_2), 
                  const.dir = c("=", rep("<=", n_hexagonos)), 
                  const.rhs = c(n_facilities, rep(1, n_hexagonos)),
                  all.bin = TRUE)
  
  # 9. Obter as novas facilities selecionadas
  novas_facilities <- candidatos[which(resultado$solution == 1)]
  
  # Inicializar variáveis de população coberta pelas novas facilities
  populacao_coberta_por_nova_facility <- data.frame(facility_id = novas_facilities, populacao_coberta = 0)
  
  # 10. Distribuir a capacidade das novas facilities instaladas, excluindo hexágonos já atendidos por outras facilities
  for (facility in seq_len(length(novas_facilities))) {
    facility_id <- novas_facilities[facility]
    capacidade <- capacidades_novas[min(facility, length(capacidades_novas))]
    
    hexagonos_proximos <- distancia_filtrada[distancia_filtrada$from_id == facility_id, "to_id"]
    
    for (hex in hexagonos_proximos$to_id) {
      # Verificar se o hexágono já foi atendido por uma facility existente
      if (populacao$atendida[populacao$id_hex == hex] == populacao[populacao$id_hex == hex, "populacao"]) {
        next  # Pular hexágonos que já foram atendidos integralmente
      }
      
      populacao_hex <- populacao[populacao$id_hex == hex, "populacao"]
      populacao_atendida <- populacao[populacao$id_hex == hex, "atendida"]
      capacidade_restante <- capacidade - populacao_coberta_por_nova_facility$populacao_coberta[facility]
      
      if (capacidade_restante <= 0) {
        break  # Se a capacidade foi esgotada, parar
      }
      
      # Atender parcialmente ou totalmente de acordo com a capacidade restante
      if (populacao_hex - populacao_atendida <= capacidade_restante) {
        populacao$atendida[populacao$id_hex == hex] <- populacao_hex
        populacao$facility_id[populacao$id_hex == hex] <- facility_id
        populacao$tipo_facility[populacao$id_hex == hex] <- "nova"
        capacidade_restante <- capacidade_restante - (populacao_hex - populacao_atendida)
        populacao_coberta_por_nova_facility$populacao_coberta[facility] <- populacao_coberta_por_nova_facility$populacao_coberta[facility] + populacao_hex
      } else {
        populacao$atendida[populacao$id_hex == hex] <- populacao_atendida + capacidade_restante
        populacao$facility_id[populacao$id_hex == hex] <- facility_id
        populacao$tipo_facility[populacao$id_hex == hex] <- "nova"
        populacao_coberta_por_nova_facility$populacao_coberta[facility] <- populacao_coberta_por_nova_facility$populacao_coberta[facility] + capacidade_restante
        capacidade <- 0
      }
    }
  }
  
  # 11. Dataframe de cobertura detalhada
  cobertura_detalhada <- populacao %>%
    mutate(cobertura_parcial = ifelse(populacao > atendida, "Parcial", "Integral")) %>%
    filter(atendida > 0) %>%
    select(id_hex, populacao, atendida, facility_id, tipo_facility, cobertura_parcial)
  
  # 12. Cálculo de cobertura existente e nova
  cobertura_existente <- cobertura_detalhada %>%
    group_by(tipo_facility) %>%
    summarise(atendida_existente = sum(atendida)) %>%
    filter(tipo_facility == "existente")
  
  cobertura_nova <- cobertura_detalhada %>%
    group_by(tipo_facility) %>%
    summarise(atendida_existente = sum(atendida)) %>%
    filter(tipo_facility == "nova")
  
  cobertura_total <- cobertura_nova$atendida_existente + cobertura_existente$atendida_existente
  percentual_nova <- cobertura_nova$atendida_existente / sum(populacao$populacao)
  percentual_existente <- cobertura_existente$atendida_existente / sum(populacao$populacao)
  percentual_total <- cobertura_total / sum(populacao$populacao)
  
  # Retornar os resultados solicitados
  return(list(
    novas_facilities = novas_facilities, 
    populacao_coberta_por_nova_facility = populacao_coberta_por_nova_facility,
    cobertura_detalhada = cobertura_detalhada,
    cobertura_existente = cobertura_existente,
    cobertura_nova = cobertura_nova,
    cobertura_total = cobertura_total,
    percentual_nova = percentual_nova,
    percentual_existente = percentual_existente,
    percentual_total = percentual_total
  ))
}


### Gerar Mapas ###

gerar_mapa_facilities <- function(grid, populacao, cras, novas_facilities) {
  
  # Verifique se o grid contém a geometria (formato sf)
  if (!inherits(grid, "sf")) {
    stop("O objeto 'grid' precisa estar no formato 'sf' com geometria definida.")
  }
  
  # Criar um data frame que combine grid, população e CRAS existentes
  dados <- left_join(grid, populacao, by = "id_hex") %>%
    mutate(populacao = replace_na(populacao, 0)) %>%
    left_join(cras, by = "id_hex") %>%
    mutate(facilities = ifelse(!is.na(facilities), "existente", NA))  # Marcar CRAS existentes
  
  # Marcar os novos CRAS no data frame
  dados$nova_facility <- ifelse(dados$id_hex %in% novas_facilities, "nova", NA)
  
  # Adicionar uma coluna para identificar o tipo de facility (existente ou nova)
  dados <- dados %>%
    mutate(tipo_facility = case_when(
      !is.na(nova_facility) ~ "nova",
      !is.na(facilities) ~ "existente",
      TRUE ~ NA_character_
    ))
  
  # Criar o mapa
  ggplot() +
    # Primeiro geom_sf para colorir os hexágonos pela população, usando a paleta viridis
    geom_sf(data = dados, aes(geometry = geom, fill = populacao), color = NA) +
    scale_fill_viridis(name = "População", option = "viridis", na.value = "gray90") +
    
    # Destacar as novas facilities em vermelho
    geom_sf(data = filter(dados, tipo_facility == "nova"), 
            aes(geometry = geom), fill = "red", color = "black", size = 2, alpha = 1) +
    
    # Destacar as facilities existentes em branco
    geom_sf(data = filter(dados, tipo_facility == "existente"), 
            aes(geometry = geom), fill = "white", color = "black", size = 2, alpha = 1) +
    
    # Ajustar o tema e títulos
    theme_minimal() +
    labs(title = "Mapa de Facilities e População",
         subtitle = "Facilities novas em vermelho, facilities existentes em branco",
         fill = "População") +
    theme(legend.position = "right")
}


gerar_mapa_cobertura <- function(grid, cobertura_detalhada, cras, novas_facilities) {
  
  # Verificar se o grid contém a geometria (formato sf)
  if (!inherits(grid, "sf")) {
    stop("O objeto 'grid' precisa estar no formato 'sf' com geometria definida.")
  }
  
  # Juntar o grid com o dataframe cobertura_detalhada
  dados <- left_join(grid, cobertura_detalhada, by = "id_hex")
  
  # Adicionar uma coluna para identificar o tipo de cobertura (existente ou nova)
  dados <- dados %>%
    mutate(tipo_cobertura = case_when(
      tipo_facility == "existente" ~ "Coberto por CRAS Existente",
      tipo_facility == "nova" ~ "Coberto por CRAS Novo",
      TRUE ~ NA_character_
    ))
  
  # Criar o mapa
  ggplot() +
    # Colorir os hexágonos cobertos por CRAS novos e existentes
    geom_sf(data = dados, aes(geometry = geom, fill = tipo_cobertura), color = NA, show.legend = TRUE) +
    
    # Usar a paleta viridis para a cobertura parcial e integral
    scale_fill_manual(name = "Cobertura",
                      values = c("Coberto por CRAS Existente" = "blue", 
                                 "Coberto por CRAS Novo" = "red"),
                      labels = c("CRAS Existente", "CRAS Novo")) +
    
    # Adicionar bordas de CRAS novos e existentes com cor e tamanho diferentes
    geom_sf(data = filter(dados, tipo_facility == "existente"), 
            aes(geometry = geom), fill = NA, color = "white", size = 2, show.legend = FALSE) +
    
    geom_sf(data = filter(dados, tipo_facility == "nova"), 
            aes(geometry = geom), fill = NA, color = "black", size = 2, show.legend = FALSE) +
    
    # Ajustar o tema e títulos
    theme_minimal() +
    labs(title = "Mapa de Cobertura por CRAS",
         subtitle = "Hexágonos cobertos por CRAS novos (vermelho) e existentes (azul)",
         fill = "Tipo de Cobertura") +
    theme(legend.position = "right",
          legend.box = "vertical",
          legend.title = element_text(size = 10),
          legend.text = element_text(size = 9))
}



### Teste ####


teste <- maxima_cobertura_lp(
  n_facilities = 8, 
  existing_facilities = cras, 
  capacidade_novas_facilities = c(4000, 6000, 5000, 4500, 7000, 3000, 4500, 9000), 
  populacao = populacao, 
  distancia = ttm, 
  corte_tempo = 30, 
  candidatos_especificos = candidatos$id_hex
)

# Visualizar os resultados
print(teste$novas_facilities)
print(teste$cobertura_existente)
print(teste$cobertura_nova)
print(teste$cobertura_total)
print(teste$percentual_nova)
print(teste$percentual_existente)
print(teste$percentual_total)
print(teste$cobertura_detalhada)

# Gerar Mapas

gerar_mapa_facilities(grid = grid, 
                      populacao = populacao, 
                      cras = cras, 
                      novas_facilities = teste$novas_facilities)

gerar_mapa_cobertura(grid = grid, 
                     cobertura_detalhada = teste$cobertura_detalhada, 
                     cras = cras, 
                     novas_facilities = teste$novas_facilities)
