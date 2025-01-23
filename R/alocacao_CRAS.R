library(dplyr)
library(tidygeocoder)
library(sf)
library(arrow)
library(ipeadatalake)

processar_cras <- function(
    cidade, 
    capacidades, 
    tempo_limite
) {
  # Parâmetros fixos
  cras_ano <- 2019
  date_cadunico <- 202212
  cad_geocode_path <- "//storage6/bases/DADOS/RESTRITO/CADASTRO_UNICO/geocode/cad_geolocalizado_2022.parquet"
  
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
  
  # Obter código IBGE
  codigo_ibge <- cidades_ibge[[cidade]]
  if (is.null(codigo_ibge)) {
    stop("Cidade inválida. Verifique o parâmetro 'cidade'.")
  }
  
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
  
  populacao <- populacao_geom %>%
    st_drop_geometry()
  
  # 5. Carregar matriz de tempo de viagem
  ttm_path <- paste0("L:/data/acesso_oport_v2/travel_time_matrix/res_9/2010/walk/", 
                     codigo_ibge, "_", tolower(gsub(" ", "_", cidade)), ".rds")
  ttm <- readRDS(ttm_path)
  
  # 6. Identificação dos Hexágonos já com CRAS instalado
  caminho_csv <- "L:/git_gregorio/acc_CRAS_cadUnico/data/CENSO_SUAS/Censo_SUAS_2023_CRAS_Dados_Gerais.csv"
  
  # Leitura do arquivo CSV usando base R
  censo_suas <- read.csv(file = caminho_csv, 
                         header = TRUE, 
                         sep = ";", 
                         stringsAsFactors = FALSE, 
                         encoding = "latin1")
  
  cras <- censo_suas %>%
    mutate(capacidade_familias = as.numeric(gsub("[^0-9]", "", q1)),
           capacidade = capacidade_familias*3.3,
           complete_address = paste(paste(q03,q04, sep = " "), q06, q09, q010, q08, sep = ", "),
           municipio = q09) %>%
    select(NU_IDENTIFICADOR, capacidade_familias, capacidade, complete_address, municipio) %>%
    mutate(municipio = ifelse(municipio == "Brasília", "Brasilia",
                              ifelse(municipio == "Belém", "Belem",
                                     ifelse(municipio == "Goiânia", "Goiania",
                                            ifelse(municipio == "Maceió", "Maceio",
                                                   ifelse(municipio == "São Luís", "Sao Luis",
                                                          ifelse(municipio == "São Paulo", "Sao Paulo",
                                                                 ifelse(municipio == "São Gonçalo", "Sao Goncalo", municipio)))))))) %>%
    filter(municipio == cidade)
  
  
  results <- cras %>%
    geocode(address = complete_address, method = 'arcgis', lat = latitude, long = longitude, full_results = FALSE)
  
  grid <- read_grid(city = cidade)
  
  # 1. Converter o dataframe `results` em um objeto sf
  results_sf <- st_as_sf(results, coords = c("longitude", "latitude"), crs = 4326)
  
  # 2. Garantir que os polígonos (`geom`) estão no mesmo sistema de coordenadas (CRS)
  grid <- st_transform(grid, crs = st_crs(results_sf))
  
  # 3. Realizar o spatial join para encontrar o id_hex correspondente
  results_with_hex <- st_join(results_sf, grid, join = st_within)
  
  # 4. Selecionar apenas as colunas de interesse
  results_with_hex <- results_with_hex %>%
    select(NU_IDENTIFICADOR, id_hex, everything())
  
  cras_spatial <- results_with_hex
  
  # 5. Converter de volta para um dataframe, se necessário
  results_with_hex <- st_drop_geometry(results_with_hex)
  
  cras <- results_with_hex %>%
    select(id_hex, capacidade)
  
  # 8. Selecionar lista de hexágonos candidatos a receber os novos CRAS
  candidate_hex <- populacao %>%
    select(id_hex) %>%
    filter(!id_hex %in% cras$id_hex)
  
  # 9. Definir o tempo limite de caminhada e remover pares fora do limite
  ttm_limpa <- ttm %>%
    filter(travel_time_p50 <= tempo_limite)
  
  # 10. Identificar os hexágonos já cobertos
  cobertura <- ttm_limpa %>%
    filter(to_id %in% cras$id_hex) %>%
    left_join(populacao, by = c("from_id" = "id_hex")) %>%
    mutate(populacao = replace_na(populacao, 0)) %>%
    arrange(from_id, travel_time_p50) %>%
    distinct(from_id, .keep_all = TRUE) %>%
    dplyr::rename(id_hex = from_id, id_cras = to_id) %>%
    arrange(id_cras, travel_time_p50) %>%
    left_join(cras, by = c("id_cras" = "id_hex")) %>%
    group_by(id_cras) %>%
    mutate(
      populacao_acumulada = cumsum(populacao),
      atendido = if_else(populacao_acumulada <= capacidade, 1, 0)
    ) %>%
    ungroup()
  
  # 11. Filtrar hexágonos cobertos
  hexagonos_cobertos <- cobertura %>%
    filter(atendido == 1) %>%
    select(id_hex, id_cras, populacao)
  
  # 12. População a ser coberta
  populacao_descoberta <- populacao %>%
    filter(!id_hex %in% hexagonos_cobertos$id_hex)
  
  # 13. Limpar matriz de tempo de viagem para manter apenas hexágonos descobertos
  ttm_limpa_2 <- ttm_limpa %>%
    filter(from_id %in% populacao_descoberta$id_hex)
  #14, Funcao de localizacao de novos_cras
  
  localizar_novos_cras <- function(ttm_limpa_2, populacao_descoberta, capacidades) {
    
    # Inicializar lista para armazenar os resultados
    novos_cras <- list()
    
    # Dataframe inicial da população descoberta
    populacao_restante <- populacao_descoberta
    
    # Dataframe para armazenar as coberturas dos hexágonos
    coberturas <- data.frame(id_hex = character(), id_cras = character(), populacao = numeric())
    
    # Iterar para cada capacidade de CRAS a ser instalado
    for (capacidade in capacidades) {
      
      # Combinar dados de tempo de viagem com população restante
      nova_cobertura <- ttm_limpa_2 %>%
        left_join(populacao_restante, by = c("from_id" = "id_hex")) %>%
        arrange(from_id, travel_time_p50) %>%
        dplyr::rename(id_hex = from_id, id_cras = to_id) %>%
        arrange(id_cras, travel_time_p50) %>%
        group_by(id_cras) %>%
        mutate(populacao_acumulada = cumsum(populacao)) %>%
        ungroup()
      
      # Calcular potencial de cobertura considerando a capacidade atual
      potencial_cobertura <- nova_cobertura %>%
        filter(populacao_acumulada <= capacidade) %>%
        group_by(id_cras) %>%
        summarise(populacao_atendida = sum(populacao), .groups = "drop") %>%
        arrange(desc(populacao_atendida)) # Ordenar para selecionar o melhor candidato
      
      # Selecionar o CRAS com maior cobertura
      melhor_cras <- potencial_cobertura$id_cras[1]
      populacao_atendida <- potencial_cobertura$populacao_atendida[1]
      
      # Adicionar o CRAS selecionado à lista de resultados
      novos_cras <- append(novos_cras, list(list(id_cras = melhor_cras, populacao_atendida = populacao_atendida)))
      
      # Identificar os hexágonos cobertos pelo CRAS selecionado
      hexagonos_cobertos <- nova_cobertura %>%
        filter(id_cras == melhor_cras, populacao_acumulada <= capacidade) %>%
        select(id_hex, populacao)
      
      # Atualizar o dataframe de coberturas
      hexagonos_cobertos <- hexagonos_cobertos %>%
        mutate(id_cras = melhor_cras)
      
      coberturas <- bind_rows(coberturas, hexagonos_cobertos)
      
      # Atualizar a população descoberta removendo os hexágonos já cobertos
      populacao_restante <- populacao_restante %>%
        filter(!id_hex %in% hexagonos_cobertos$id_hex)
    }
    
    # Retornar os resultados
    return(list(novos_cras = novos_cras, coberturas = coberturas))
  }
  
  # 14. Otimização: Localizar novos CRAS
  resultados <- localizar_novos_cras(ttm_limpa_2, populacao_descoberta, capacidades)
  
  # 15. Gerar dataframe com cobertura final
  hexagonos_cobertos_antes <- hexagonos_cobertos %>% mutate(tipo_cras = "existente")
  hexagonos_cobertos_depois <- resultados$coberturas %>% mutate(tipo_cras = "novo")
  cobertura_final <- bind_rows(hexagonos_cobertos_antes, hexagonos_cobertos_depois)
  atendimento_cras <- cobertura_final %>% 
    group_by(id_cras) %>%
    summarise(populacao_atendida = sum(populacao)) %>%
    left_join(cobertura_final, by = "id_cras") %>%
    select(id_cras, populacao_atendida, tipo_cras) %>%
    distinct(id_cras, .keep_all = TRUE)
  
  # Retornar resultados
  return(list(
    novos_cras = resultados$novos_cras,
    cobertura_final = cobertura_final,
    atendimento_cras = atendimento_cras,
    tempo_limite = tempo_limite,
    cidade = cidade,
    cras = cras_spatial,
    df_city = populacao_geom,
    ttm = ttm
    
  ))
}

######## Aplicação ##########

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

belem <- processar_cras(
  cidade = "Belem",
  tempo_limite = 30,
  capacidades = c(60000, 50000, 40000, 90000, 90000, 90000, 90000, 90000, 90000, 90000)
)

belo_horizonte <- processar_cras(
  cidade = "Belo Horizonte",
  tempo_limite = 30,
  capacidades = c(60000, 50000, 40000, 90000, 90000, 90000, 90000, 90000, 90000, 90000)
)

brasilia <- processar_cras(
  cidade = "Brasilia",
  tempo_limite = 30,
  capacidades = c(60000, 50000, 40000, 90000, 90000, 90000, 90000, 90000, 90000, 90000)
)

campinas <- processar_cras(
  cidade = "Campinas",
  tempo_limite = 30,
  capacidades = c(60000, 50000, 40000, 90000, 90000, 90000, 90000, 90000, 90000, 90000)
)

campo_grande <- processar_cras(
  cidade = "Campo Grande",
  tempo_limite = 30,
  capacidades = c(60000, 50000, 40000, 90000, 90000, 90000, 90000, 90000, 90000, 90000)
)

curitiba <- processar_cras(
  cidade = "Curitiba",
  tempo_limite = 30,
  capacidades = c(60000, 50000, 40000, 90000, 90000, 90000, 90000, 90000, 90000, 90000)
)

fortaleza <- processar_cras(
  cidade = "Fortaleza",
  tempo_limite = 30,
  capacidades = c(60000, 50000, 40000, 90000, 90000, 90000, 90000, 90000, 90000, 90000)
)

goiania <- processar_cras(
  cidade = "Goiania",
  tempo_limite = 30,
  capacidades = c(60000, 50000, 40000, 90000, 90000, 90000, 90000, 90000, 90000, 90000)
)

maceio <- processar_cras(
  cidade = "Maceio",
  tempo_limite = 30,
  capacidades = c(60000, 50000, 40000, 90000, 90000, 90000, 90000, 90000, 90000, 90000)
)

manaus <- processar_cras(
  cidade = "Manaus",
  tempo_limite = 30,
  capacidades = c(60000, 50000, 40000, 90000, 90000, 90000, 90000, 90000, 90000, 90000)
)

natal <- processar_cras(
  cidade = "Natal",
  tempo_limite = 30,
  capacidades = c(60000, 50000, 40000, 90000, 90000, 90000, 90000, 90000, 90000, 90000)
)

porto_alegre <- processar_cras(
  cidade = "Porto Alegre",
  tempo_limite = 30,
  capacidades = c(60000, 50000, 40000, 90000, 90000, 90000, 90000, 90000, 90000, 90000)
)

recife <- processar_cras(
  cidade = "Recife",
  tempo_limite = 30,
  capacidades = c(60000, 50000, 40000, 90000, 90000, 90000, 90000, 90000, 90000, 90000)
)

rio_de_janeiro <- processar_cras(
  cidade = "Rio de Janeiro",
  tempo_limite = 30,
  capacidades = c(60000, 50000, 40000, 90000, 90000, 90000, 90000, 90000, 90000, 90000)
)

salvador <- processar_cras(
  cidade = "Salvador",
  tempo_limite = 30,
  capacidades = c(60000, 50000, 40000, 90000, 90000, 90000, 90000, 90000, 90000, 90000)
)

sao_luis <- processar_cras(
  cidade = "Sao Luis",
  tempo_limite = 30,
  capacidades = c(60000, 50000, 40000, 90000, 90000, 90000, 90000, 90000, 90000, 90000)
)

sao_paulo <- processar_cras(
  cidade = "Sao Paulo",
  tempo_limite = 30,
  capacidades = c(60000, 50000, 40000, 90000, 90000, 90000, 90000, 90000, 90000, 90000)
)

# Compilar resultados

processar_multiplos_cras <- function(...) {
  # Obter a lista de dados fornecidos como argumentos
  lista_dados <- list(...)
  
  # Obter os nomes das variáveis de entrada
  nomes_variaveis <- as.character(match.call()[-1])
  
  # Inicializar um dataframe vazio
  resultado_consolidado <- data.frame(linha = 1:10)  # Coluna de índice para garantir 10 linhas
  
  # Iterar sobre cada entrada na lista
  for (i in seq_along(lista_dados)) {
    data <- lista_dados[[i]]$novos_cras
    
    # Nome da variável correspondente
    nome_variavel <- nomes_variaveis[i]
    
    # Processar os dados
    df <- data.frame(
      populacao_atendida = sapply(data, function(x) x$populacao_atendida)
    )
    
    # Ordenar por `populacao_atendida` em ordem decrescente
    df <- df[order(-df$populacao_atendida), , drop = FALSE]
    
    # Garantir que o dataframe tenha 10 linhas (preenchendo com NA se necessário)
    df <- df[1:10, , drop = FALSE]
    df <- cbind(linha = 1:10, df)  # Adicionar índice para alinhamento
    
    # Renomear a coluna com o nome da variável
    colnames(df)[2] <- nome_variavel
    
    # Unir ao dataframe consolidado
    resultado_consolidado <- merge(resultado_consolidado, df, by = "linha", all = TRUE)
  }
  
  # Remover a coluna de índice antes de retornar o dataframe final
  resultado_consolidado$linha <- NULL
  
  return(resultado_consolidado)
}

resultado_df <- processar_multiplos_cras(belem,
                                         belo_horizonte,
                                         brasilia,
                                         campinas,
                                         campo_grande,
                                         curitiba,
                                         fortaleza,
                                         goiania,
                                         maceio,
                                         manaus,
                                         natal,
                                         porto_alegre,
                                         recife,
                                         rio_de_janeiro,
                                         salvador,
                                         sao_luis,
                                         sao_paulo)


# Exibir o dataframe consolidado
print(resultado_df)

colnames(resultado_df) <- c(
  "Belém", "Belo Horizonte", "Brasília", "Campinas", "Campo Grande", "Curitiba",
  "Fortaleza", "Goiânia", "Maceió", "Manaus", "Natal", "Porto Alegre", "Recife",
  "Rio de Janeiro", "Salvador", "São Luís", "São Paulo"
)

# Transformar o dataframe para o formato longo
resultados_long <- resultado_df %>%
  mutate(num_cras = row_number()) %>%
  pivot_longer(cols = -num_cras, names_to = "cidade", values_to = "pessoas_cobertas")

# Criar o gráfico
ggplot(resultados_long, aes(x = num_cras, y = pessoas_cobertas, color = cidade)) +
  geom_line(size = 1) + 
  geom_point(size = 2) +
  labs(
    title = "Cobertura de pessoas por número de CRAS",
    x = "Número de CRAS",
    y = "Número de pessoas cobertas",
    color = "Cidade"
  ) +
  theme_minimal() +
  theme(legend.position = "bottom")



generate_city_map <- function(resultado) {
  # Pré-processamento dos dados
  df_city <- resultado$df_city %>%
    dplyr::rename(total_pessoas = populacao)
  
  cobertura_final <- resultado$cobertura_final
  
  df_city_2 <- left_join(df_city, cobertura_final, by = "id_hex") %>%
    replace_na(list(tipo_cras = "nao_coberto")) %>%
    dplyr::rename(status = tipo_cras) %>%
    select(id_hex, total_pessoas, status)
  
  # Calcular os percentuais
  total_pop <- sum(df_city_2$total_pessoas, na.rm = TRUE)
  total_existente <- sum(df_city_2$total_pessoas[df_city_2$status == "existente"], na.rm = TRUE)
  total_novo <- sum(df_city_2$total_pessoas[df_city_2$status == "novo"], na.rm = TRUE)
  total_nao_coberto <- sum(df_city_2$total_pessoas[df_city_2$status == "nao_coberto"], na.rm = TRUE)
  
  perc_existente <- (total_existente / total_pop) * 100
  perc_novo <- (total_novo / total_pop) * 100
  perc_nao_coberto <- (total_nao_coberto / total_pop) * 100
  
  # Adicionar rótulos
  df_city_2 <- df_city_2 %>%
    mutate(status_label = case_when(
      status == "existente" ~ sprintf("Acessível CRAS existente (%.2f%%)", perc_existente),
      status == "novo" ~ sprintf("Acessível CRAS novo (%.2f%%)", perc_novo),
      status == "nao_coberto" ~ sprintf("Não Acessível (%.2f%%)", perc_nao_coberto)
    ))
  
  # Preparar dados para CRAS
  cras_id <- cobertura_final %>%
    select(id_cras) %>%
    distinct()
  
  df_city_geom <- read_grid(resultado$cidade)
  
  cras <- df_city_geom %>%
    filter(id_hex %in% cras_id$id_cras) %>%
    st_centroid()
  
  # Gerar o mapa
  city_map <- ggplot() +
    geom_sf(data = df_city_2, fill = NA, color = 'gray') +
    geom_sf(data = df_city_2, aes(fill = total_pessoas), color = NA) + 
    geom_sf(data = cras, aes(color = "CRAS"), size = 1, alpha = 0.6, show.legend = TRUE) +
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
    labs(title = paste("População cadastrada no Cadúnico a menos e a mais de", resultado$tempo_limite, 
                       "minutos de caminhada de um CRAS"),
         caption = "Fonte: Cadúnico e Acesso a Oportunidades, IPEA",
         x = "Longitude",
         y = "Latitude") +
    annotation_scale(location = "br", width_hint = 0.5, pad_x = unit(0.5, "cm"), pad_y = unit(0.5, "cm"))
  
  # Salvar mapa
  ggsave(city_map, filename = paste0('./figures/mapa_otimizacao_', resultado$cidade, '_', resultado$tempo_limite, 'min.png'),
         width = 30, height = 24, units = 'cm', dpi = 200)
  
  return(city_map)

}

curitiba_3 <- processar_cras(
  cidade = "Curitiba",
  tempo_limite = 30,
  capacidades = c(16500, 16500, 16500)
)

recife_3 <- processar_cras(
  cidade = "Recife",
  tempo_limite = 30,
  capacidades = c(16500, 16500, 16500)
)

belo_horizonte_3 <- processar_cras(
  cidade = "Belo Horizonte",
  tempo_limite = 30,
  capacidades = c(16500, 16500, 16500)
)

campinas_3 <- processar_cras(
  cidade = "Campinas",
  tempo_limite = 30,
  capacidades = c(16500, 16500, 16500)
)


generate_city_map(curitiba_3)
generate_city_map(recife_3)
generate_city_map(belo_horizonte_3)
generate_city_map(campinas_3)

