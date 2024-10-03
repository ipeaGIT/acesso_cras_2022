library(arrow)
library(dplyr)
library(ipeadatalake)
library(ompr)
library(ompr.roi)
library(ROI.plugin.glpk)
library(r5r)
library(aopdata)
library(sf)
library(lpSolve)
library(h3jsr)
library(lpSolve)
library(ggplot2)
library(viridis)
library(tidyr)
library(maxcovr)



##### Dados do modelo #####
### Populacao que precisa ser atendida

df <- ipeadatalake::read_cadunico(date = 202212, type = 'familia') %>%
  filter(cd_ibge_cadastro %in% c(4314902)) %>%
  collect()

# Juntar dados CadÚnico com base geo
geo <- arrow::open_dataset('//storage6/bases/DADOS/RESTRITO/CADASTRO_UNICO/geocode/cad_geolocalizado_2022.parquet') %>%
  filter(id_familia %in% df$co_familiar_fam) %>%
  collect()

df_geo <- dplyr::left_join(df, geo, by = join_by('co_familiar_fam' == 'id_familia')) %>% sample_frac(0.1)

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

## Calcular distancia
# Função para converter graus em radianos
deg2rad <- function(deg) {
  return(deg * (pi / 180))
}

# Função vetorizada para calcular a distância esférica usando a fórmula Haversine
spherical_distance <- function(lat1, lon1, lat2, lon2) {
  radius_earth <- 6371  # Raio da Terra em quilômetros
  
  # Converter graus em radianos
  lat1 <- deg2rad(lat1)
  lon1 <- deg2rad(lon1)
  lat2 <- deg2rad(lat2)
  lon2 <- deg2rad(lon2)
  
  # Fórmula de Haversine para calcular a distância
  a <- sin((lat2 - lat1) / 2) ^ 2 + cos(lat1) * cos(lat2) * sin((lon2 - lon1) / 2) ^ 2
  d <- 2 * atan2(sqrt(a), sqrt(1 - a)) * radius_earth
  
  # Retornar a distância em metros
  return(d * 1000)
}


points <- grid %>%
  mutate(centroid = st_centroid(geom)) %>% 
  mutate(lat = st_coordinates(centroid)[,2], 
         lon = st_coordinates(centroid)[,1],
         id = id_hex) %>%
  st_drop_geometry() %>%
  select(id, lat, lon)

# Criar todas as combinações possíveis de `from_id` e `to_id` (pares)
resultados <- expand.grid(from_id = points$id, to_id = points$id, stringsAsFactors = FALSE)

# Juntar o dataframe para associar as coordenadas de origem e destino
resultados <- resultados %>%
  left_join(points, by = c("from_id" = "id")) %>%
  rename(from_lat = lat, from_lon = lon) %>%
  left_join(points, by = c("to_id" = "id")) %>%
  rename(to_lat = lat, to_lon = lon)

# Aplicar a função vetorizada para calcular as distâncias de uma só vez
resultados <- resultados %>%
  mutate(travel_time_p50 = spherical_distance(from_lat, from_lon, to_lat, to_lon))

# Selecionar apenas as colunas necessárias no formato longo: from_id, to_id, travel_time_p50
ttm <- resultados %>%
  select(from_id, to_id, travel_time_p50)


# CRAS existentes

cras <- read_landuse(city = 'poa', year = 2019, showProgress = FALSE) %>%
  filter(C001 > 0) %>%
  select(id_hex, C001) %>%
  rename(facilities = C001)

##### Definindo o modelo #####

maxima_cobertura_lp <- function(n_facilities, existing_facilities, populacao, distancia, corte_tempo) {
  
  # 1. Filtrar as distâncias que respeitam o corte de tempo
  distancia_filtrada <- distancia[distancia$travel_time_p50 <= corte_tempo, ]
  
  # 2. Priorizar os hexágonos mais próximos para atendimento (ordenar por tempo de viagem)
  distancia_filtrada <- distancia_filtrada[order(distancia_filtrada$travel_time_p50), ]
  
  # 3. Criar uma matriz binária onde cada linha representa um hexágono de usuário
  # e cada coluna representa um hexágono candidato a receber uma nova facility
  hexagonos <- unique(distancia_filtrada$to_id)  # Hexágonos de usuários
  candidatos <- unique(distancia_filtrada$from_id)  # Hexágonos candidatos a novas facilities
  
  n_hexagonos <- length(hexagonos)
  n_candidatos <- length(candidatos)
  
  # 4. Calcular a população que já foi atendida pelas facilities existentes
  # Inicializar a população atendida com 0
  populacao$atendida <- 0
  
  # Atribuir a cobertura existente
  for (i in seq_len(nrow(existing_facilities))) {
    facility_id <- existing_facilities$id_hex[i]
    
    # Hexágonos cobertos por essa facility
    hexagonos_cobertos <- distancia_filtrada[distancia_filtrada$from_id == facility_id, ]
    
    # Atualizar a população atendida
    if (nrow(hexagonos_cobertos) > 0) {
      populacao$atendida[populacao$id_hex %in% hexagonos_cobertos$to_id] <- 
        populacao$populacao[populacao$id_hex %in% hexagonos_cobertos$to_id]
    }
  }
  
  # 5. Calcular a população não atendida
  populacao_nao_atendida <- populacao
  populacao_nao_atendida$populacao <- populacao_nao_atendida$populacao - populacao_nao_atendida$atendida
  populacao_nao_atendida$populacao[populacao_nao_atendida$populacao < 0] <- 0
  
  # 6. Construir a matriz de cobertura (binária) para a população não atendida
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
  
  # A função objetivo é a soma ponderada da matriz de cobertura pela população não atendida
  funcao_objetivo <- colSums(matriz_cobertura * populacao_ordenada)
  
  # 8. Restrições
  # Restrição 1: Somente `n_facilities` facilities podem ser instaladas
  restricao_1 <- rep(1, n_candidatos)
  
  # Restrição 2: Cada hexágono de usuário só pode ser coberto por uma facility
  restricao_2 <- matriz_cobertura
  
  # 9. Configurar o modelo com lpSolve
  resultado <- lp(direction = "max", 
                  objective.in = funcao_objetivo, 
                  const.mat = rbind(restricao_1, restricao_2), 
                  const.dir = c("=", rep("<=", n_hexagonos)), 
                  const.rhs = c(n_facilities, rep(1, n_hexagonos)),
                  all.bin = TRUE)
  
  # 10. Obter os hexágonos selecionados para instalar as novas facilities
  novas_facilities <- candidatos[which(resultado$solution == 1)]
  
  return(list(novas_facilities = novas_facilities, resultado = resultado))
}




#### Funcoes para resultado e geracao de mapa ####

resultados_maxima_cobertura <- function(novas_facilities, cras, populacao, distancia, corte_tempo) {
  
  # 1. Filtrar as distâncias que respeitam o corte de tempo para os CRAS existentes
  distancia_existentes <- distancia[distancia$travel_time_p50 <= corte_tempo & distancia$from_id %in% cras$id_hex, ]
  
  # 2. Filtrar as distâncias que respeitam o corte de tempo para os novos CRAS
  distancia_novas <- distancia[distancia$travel_time_p50 <= corte_tempo & distancia$from_id %in% novas_facilities, ]
  
  # 3. População coberta pelos CRAS existentes
  populacao_coberta_existentes <- populacao[populacao$id_hex %in% distancia_existentes$to_id, ]
  total_pop_coberta_existentes <- sum(populacao_coberta_existentes$populacao, na.rm = TRUE)
  
  # 4. População coberta pelos novos CRAS, excluindo hexágonos já cobertos pelos CRAS existentes
  hexagonos_cobertos_existentes <- unique(distancia_existentes$to_id)
  populacao_coberta_novas <- populacao[populacao$id_hex %in% setdiff(distancia_novas$to_id, hexagonos_cobertos_existentes), ]
  total_pop_coberta_novas <- sum(populacao_coberta_novas$populacao, na.rm = TRUE)
  
  # 5. População total coberta pelos CRAS existentes e novos (sem duplicações)
  populacao_total_coberta <- populacao[populacao$id_hex %in% union(distancia_existentes$to_id, distancia_novas$to_id), ]
  total_pop_coberta_total <- sum(populacao_total_coberta$populacao, na.rm = TRUE)
  
  # 6. População coberta por cada nova facility
  pop_coberta_por_nova_facility <- lapply(novas_facilities, function(facility) {
    hexagonos_cobertos <- distancia[distancia$from_id == facility & distancia$travel_time_p50 <= corte_tempo, "to_id"]
    hexagonos_validos <- setdiff(hexagonos_cobertos$to_id, hexagonos_cobertos_existentes)
    populacao_coberta <- populacao[populacao$id_hex %in% hexagonos_validos, ]
    sum(populacao_coberta$populacao, na.rm = TRUE)
  })
  
  # Combinar a população coberta com os IDs das novas facilities
  pop_coberta_por_nova_facility <- data.frame(
    nova_facility = novas_facilities,
    populacao_coberta = unlist(pop_coberta_por_nova_facility)
  )
  
  # 7. Retornar os resultados
  return(list(
    pop_coberta_existentes = total_pop_coberta_existentes,
    pop_coberta_novas = total_pop_coberta_novas,
    pop_coberta_total = total_pop_coberta_total,
    pop_coberta_por_nova_facility = pop_coberta_por_nova_facility
  ))
}


### Funcao de Geracao de mapas

gerar_mapa_cras <- function(grid, populacao, cras, novas_facilities) {
  
  # Certificar-se de que o grid contém a geometria
  if (!inherits(grid, "sf")) {
    stop("O objeto 'grid' precisa estar no formato 'sf' com geometria definida.")
  }
  
  # Criar o data frame 'dados' juntando grid, populacao e cras
  dados <- left_join(grid, populacao, by = "id_hex") %>%
    select(id_hex, populacao, geom) %>%
    mutate(populacao = replace_na(populacao, 0)) %>%
    left_join(cras, by = "id_hex") %>%
    mutate(facilities = replace_na(facilities, 0))
  
  # Marcar os novos CRAS no data frame
  dados$nova_facility <- 0
  dados$nova_facility[dados$id_hex %in% novas_facilities] <- 1
  dados <- dados %>%
    mutate(nova_facility = replace_na(nova_facility, 0))
  
  # Criar uma nova coluna 'tipo_facility' que identifique o tipo de facility (nova ou existente)
  dados <- dados %>%
    mutate(tipo_facility = case_when(
      nova_facility == 1 ~ "Novo CRAS",
      facilities == 1 ~ "CRAS Existente",
      TRUE ~ NA_character_
    ))
  
  # Plotar o mapa
  ggplot() +
    # Primeiro geom_sf para a população com a escala viridis
    geom_sf(data = dados, aes(geometry = geom, fill = populacao), color = NA) +
    scale_fill_viridis(name = "População", option = "viridis") +
    
    # Segundo geom_sf para destacar as novas e existentes CRAS
    geom_sf(data = dados[dados$nova_facility == 1, ], aes(geometry = geom), 
            fill = "red", color = "black", size = 2, alpha = 1, show.legend = TRUE) +
    geom_sf(data = dados[dados$facilities == 1, ], aes(geometry = geom), 
            fill = "white", color = "black", size = 2, alpha = 1, show.legend = TRUE) +
    
    # Ajustar o tema e títulos
    theme_minimal() +
    labs(title = "Mapa de CRAS e População Coberta",
         subtitle = "Hexágonos vermelhos indicam novos CRAS, brancos indicam CRAS existentes",
         fill = "População") +
    theme(legend.position = "right")
}


#### Aplicacao ####


teste <- maxima_cobertura_lp(3, cras, populacao, ttm, 1500)
teste$novas_facilities
#### Resultados


gerar_mapa_cras(grid, populacao, cras, teste$novas_facilities)



##### Teste com Maxcovr #####

## Ajustando dados ###

# Calcular os centroides dos polígonos no dataframe grid
grid_centroides <- grid %>%
  st_centroid() %>%
  mutate(lat = st_coordinates(.)[, 2], # Extrair latitude do centroide
         long = st_coordinates(.)[, 1]) # Extrair longitude do centroide

# Criar o dataframe cras_lat_long juntando o 'cras' com os centroides do 'grid'
cras_lat_long <- cras %>%
  left_join(grid_centroides %>% select(id_hex, lat, long), by = "id_hex") %>%
  st_drop_geometry() %>%
  select(id_hex, lat, long)


# Excluir do grid_centroides os hexágonos que já estão no cras_lat_long
locais_candidatos <- grid_centroides %>%
  anti_join(cras_lat_long, by = "id_hex") %>%
  st_drop_geometry() %>%
  select(id_hex,lat,long)

# Ajustar paopulacao
users <- df_geo %>%
  rename(long = lon)


mc_result <- max_coverage(existing_facility = cras_lat_long,
                          proposed_facility = locais_candidatos,
                          user = users,
                          distance_cutoff = 1500,
                          n_added = 3)


mc_result$facility_selected



gerar_mapa_facilities2 <- function(grid, populacao, cras, novas_facilities) {
  
  # Certificar-se de que o grid contém a geometria
  if (!inherits(grid, "sf")) {
    stop("O objeto 'grid' precisa estar no formato 'sf' com geometria definida.")
  }
  
  # Garantir que os id_hex estão no mesmo formato (character)
  grid <- grid %>% mutate(id_hex = as.character(id_hex))
  cras <- cras %>% mutate(id_hex = as.character(id_hex))
  novas_facilities <- as.data.frame(mc_result$facility_selected)
  novas_facilities <- novas_facilities %>% mutate(id_hex = as.character(id_hex))
  
  
  # Criar o data frame 'dados' juntando grid, populacao e cras
  dados <- left_join(grid, populacao, by = "id_hex") %>%
    select(id_hex, populacao, geom) %>%
    mutate(populacao = replace_na(populacao, 0)) %>%
    left_join(cras, by = "id_hex") %>%
    mutate(facilities = replace_na(facilities, 0))
  
  # Marcar os novos facilities no data frame
  dados$nova_facility <- 0
  dados$nova_facility[dados$id_hex %in% novas_facilities$id_hex] <- 1
  dados <- dados %>%
    mutate(nova_facility = replace_na(nova_facility, 0))
  
  # Criar uma nova coluna 'tipo_facility' que identifique o tipo de facility (nova ou existente)
  dados <- dados %>%
    mutate(tipo_facility = case_when(
      nova_facility == 1 ~ "Nova Facility",
      facilities == 1 ~ "Facility Existente",
      TRUE ~ NA_character_
    ))
  
  # Plotar o mapa
  ggplot() +
    # Primeiro geom_sf para a população com a escala viridis
    geom_sf(data = dados, aes(geometry = geom, fill = populacao), color = NA) +
    scale_fill_viridis(name = "População", option = "viridis") +
    
    # Segundo geom_sf para destacar as novas facilities em vermelho
    geom_sf(data = dados[dados$nova_facility == 1, ], aes(geometry = geom), 
            fill = "red", color = "black", size = 2, alpha = 1, show.legend = TRUE) +
    
    # Destacar as facilities existentes em branco
    geom_sf(data = dados[dados$facilities == 1, ], aes(geometry = geom), 
            fill = "white", color = "black", size = 2, alpha = 1, show.legend = TRUE) +
    
    # Ajustar o tema e títulos
    theme_minimal() +
    labs(title = "Mapa de Facilities Selecionadas e População Coberta",
         subtitle = "Hexágonos vermelhos indicam novas facilities, brancos indicam facilities existentes",
         fill = "População") +
    theme(legend.position = "right")
}

# Exemplo de uso:
# Aqui, mc_result$facility_selected é passado como 'novas_facilities'
gerar_mapa_facilities2(grid, populacao, cras, mc_result$facility_selected)



