library(aopdata)
library(dplyr)
library(ggplot2)
library(sf)
library(viridis)
library(tidyr)
library(ggspatial)
library(readr)
#devtools::install_git("https://gitlab.ipea.gov.br/rafael.pereira/ipeadatalake")
library(ipeadatalake)

########## Carregar dados do AOP de população e acesso ##########



###### Figura 3 - Cleveland Plot ########

# Todas as cidades

all_cities <- read_access(city = 'all', mode = 'walk', year = 2019, showProgress = FALSE)

# Substituindo valor Inf de tempo de caminhada por 90, substituindo NA da populacao por 0, e filtrando público alvo

all_cities <- all_cities %>%
  filter(R001 <= 255) %>%
  mutate(TMICT = ifelse(is.infinite(TMICT), 90, TMICT),
         P001 = ifelse(is.na(P001), 0, P001))


# Adiciona uma nova coluna para verificar se o acesso à CRAS é feito em menos de 15 minutos
all_cities <- all_cities %>%
  mutate(acesso_15_min = ifelse(TMICT <= 15, 1, 0),
         acesso_30_min = ifelse(TMICT <= 30, 1, 0),
         acesso_45_min = ifelse(TMICT <= 45, 1, 0),
         acesso_60_min = ifelse(TMICT <= 60, 1, 0))

# Agrupa por município e calcula o percentual de pessoas com e sem acesso em 15 minutos
resumo_municipios <- all_cities %>%
  group_by(name_muni) %>%
  summarise(total_pessoas = sum(P001, na.rm = TRUE),
            acessam_15_min = sum(P001[acesso_15_min == 1], na.rm = TRUE),
            nao_acessam_15_min = sum(P001[acesso_15_min == 0], na.rm = TRUE),
            perc_acessam_15_min = acessam_15_min / total_pessoas * 100,
            perc_nao_acessam_15_min = nao_acessam_15_min / total_pessoas * 100,
            acessam_30_min = sum(P001[acesso_30_min == 1], na.rm = TRUE),
            nao_acessam_30_min = sum(P001[acesso_30_min == 0], na.rm = TRUE),
            perc_acessam_30_min = acessam_30_min / total_pessoas * 100,
            perc_nao_acessam_30_min = nao_acessam_30_min / total_pessoas * 100,
            acessam_45_min = sum(P001[acesso_45_min == 1], na.rm = TRUE),
            nao_acessam_45_min = sum(P001[acesso_45_min == 0], na.rm = TRUE),
            perc_acessam_45_min = acessam_45_min / total_pessoas * 100,
            perc_nao_acessam_45_min = nao_acessam_45_min / total_pessoas * 100,
            acessam_60_min = sum(P001[acesso_60_min == 1], na.rm = TRUE),
            nao_acessam_60_min = sum(P001[acesso_60_min == 0], na.rm = TRUE),
            perc_acessam_60_min = acessam_60_min / total_pessoas * 100,
            perc_nao_acessam_60_min = nao_acessam_60_min / total_pessoas * 100)

# Cleveland Plot (15 minutos)

# Ajuste para formato longo
resumo_long_15 <- resumo_municipios %>%
  select(name_muni, perc_acessam_15_min, perc_nao_acessam_15_min) %>%
  pivot_longer(cols = c(perc_acessam_15_min, perc_nao_acessam_15_min),
               names_to = "Categoria",
               values_to = "Percentual")

# Ajusta o nome das categorias para melhor visualização
resumo_long_15$Categoria <- recode(resumo_long_15$Categoria,
                                   perc_acessam_15_min = "Acesso em <= 15 minutos",
                                   perc_nao_acessam_15_min = "Sem acesso em 15 minutos")

# Criando o gráfico
ggplot(resumo_long_15, aes(x = reorder(name_muni, -Percentual), y = Percentual, group = name_muni)) +
  geom_line(aes(group = name_muni), color = "#CDD0D0", size = 3, alpha = 0.7) +  # Adiciona linhas com transparência
  geom_point(aes(fill = Categoria), stat = "identity", position = position_dodge(width = 0.5), size = 4, shape = 21, color = "black") +
  coord_flip() +  # Inverte os eixos para colocar os municípios no eixo y
  labs(title = "Proporção de acesso à CRAS em até 15 minutos por município",
       x = "Município",
       y = "Percentual") +
  scale_fill_manual(values = c("Acesso em <= 15 minutos" = "#41A874", "Sem acesso em 15 minutos" = "#5E86A3")) +
  theme_minimal() +
  theme(legend.title = element_blank())

#Cleveland Plot (30 minutos)

# Ajuste para formato longo
resumo_long_30 <- resumo_municipios %>%
  select(name_muni, perc_acessam_30_min, perc_nao_acessam_30_min) %>%
  pivot_longer(cols = c(perc_acessam_30_min, perc_nao_acessam_30_min),
               names_to = "Categoria",
               values_to = "Percentual")

# Ajusta o nome das categorias para melhor visualização
resumo_long_30$Categoria <- recode(resumo_long_30$Categoria,
                                   perc_acessam_30_min = "Acesso em <= 30 minutos",
                                   perc_nao_acessam_30_min = "Sem acesso em 30 minutos")

# Criando o gráfico
ggplot(resumo_long_30, aes(x = reorder(name_muni, -Percentual), y = Percentual, group = name_muni)) +
  geom_line(aes(group = name_muni), color = "#CDD0D0", size = 3, alpha = 0.7) +  # Adiciona linhas com transparência
  geom_point(aes(fill = Categoria), stat = "identity", position = position_dodge(width = 0.5), size = 4, shape = 21, color = "black") +
  coord_flip() +  # Inverte os eixos para colocar os municípios no eixo y
  labs(title = "Proporção de acesso à CRAS em até 30 minutos por município",
       x = "Município",
       y = "Percentual") +
  scale_fill_manual(values = c("Acesso em <= 30 minutos" = "#41A874", "Sem acesso em 30 minutos" = "#5E86A3")) +
  theme_minimal() +
  theme(legend.title = element_blank())

#Cleveland Plot (45 minutos)

# Ajuste para formato longo
resumo_long_45 <- resumo_municipios %>%
  select(name_muni, perc_acessam_45_min, perc_nao_acessam_45_min) %>%
  pivot_longer(cols = c(perc_acessam_45_min, perc_nao_acessam_45_min),
               names_to = "Categoria",
               values_to = "Percentual")

# Ajusta o nome das categorias para melhor visualização
resumo_long_45$Categoria <- recode(resumo_long_45$Categoria,
                                   perc_acessam_45_min = "Acesso em <= 45 minutos",
                                   perc_nao_acessam_45_min = "Sem acesso em 45 minutos")

# Criando o gráfico
ggplot(resumo_long_45, aes(x = reorder(name_muni, -Percentual), y = Percentual, group = name_muni)) +
  geom_line(aes(group = name_muni), color = "#CDD0D0", size = 3, alpha = 0.7) +  # Adiciona linhas com transparência
  geom_point(aes(fill = Categoria), stat = "identity", position = position_dodge(width = 0.5), size = 4, shape = 21, color = "black") +
  coord_flip() +  # Inverte os eixos para colocar os municípios no eixo y
  labs(title = "Proporção de acesso à CRAS em até 45 minutos por município",
       x = "Município",
       y = "Percentual") +
  scale_fill_manual(values = c("Acesso em <= 45 minutos" = "#41A874", "Sem acesso em 45 minutos" = "#5E86A3")) +
  theme_minimal() +
  theme(legend.title = element_blank())

#Cleveland Plot (60 minutos)

# Ajuste para formato longo
resumo_long_60 <- resumo_municipios %>%
  select(name_muni, perc_acessam_60_min, perc_nao_acessam_60_min) %>%
  pivot_longer(cols = c(perc_acessam_60_min, perc_nao_acessam_60_min),
               names_to = "Categoria",
               values_to = "Percentual")

# Ajusta o nome das categorias para melhor visualização
resumo_long_60$Categoria <- recode(resumo_long_60$Categoria,
                                   perc_acessam_60_min = "Acesso em <= 60 minutos",
                                   perc_nao_acessam_60_min = "Sem acesso em 60 minutos")

# Criando o gráfico
ggplot(resumo_long_60, aes(x = reorder(name_muni, -Percentual), y = Percentual, group = name_muni)) +
  geom_line(aes(group = name_muni), color = "#CDD0D0", size = 3, alpha = 0.7) +  # Adiciona linhas com transparência
  geom_point(aes(fill = Categoria), stat = "identity", position = position_dodge(width = 0.5), size = 4, shape = 21, color = "black") +
  coord_flip() +  # Inverte os eixos para colocar os municípios no eixo y
  labs(title = "Proporção de acesso à CRAS em até 60 minutos por município",
       x = "Município",
       y = "Percentual") +
  scale_fill_manual(values = c("Acesso em <= 60 minutos" = "#41A874", "Sem acesso em 60 minutos" = "#5E86A3")) +
  theme_minimal() +
  theme(legend.title = element_blank())

######## CadÚnico #####

bh_cad_unico <- read_cadunico(geocode = TRUE, type )

