library(RecordLinkage)
library(rio)
library(dplyr)
library(tidyverse)


setwd("/Users/davidricardogonzales/Library/CloudStorage/OneDrive-UDEP/MAC")

# Importando los apellidos limpiados en stata, y separándolos en 4 grupos para reducir el tiempo de procesado.

apellidos <- import("apellidos_unic.dta") %>% 
  replace(is.na(.), 0) %>% 
  rename(APELLIDO = 2)

apellido_filt <- apellidos %>% 
  filter(APELLIDO != "")

apell_batch1 <- apellido_filt %>% 
  filter(id < 10000)

apell_batch2 <- apellido_filt %>% 
  filter(id < 20000) %>% 
  filter(id > 9999)

apell_batch3 <- apellido_filt %>% 
  filter(id < 30000) %>% 
  filter(id > 19999)

apell_batch4 <- apellido_filt %>% 
  filter(id > 29999)

############### Importando bases de datos de apellidos clasificados ###############

# Importando del excel adjunto.

hispano <- import("hisp_bd.dta") %>% 
  rename(apell = 1) 

first_list <- import("ind_bd.dta") %>% 
  rename(apell = 1) 

############################ Aplicando levenshtein method por partes para reducir la carga del procesamiento ############################

apell_batch1 <- apell_batch1 %>% 
  group_by(APELLIDO) %>% 
  mutate(hisp_word = hispano$apell[which.max(levenshteinSim(APELLIDO, hispano$apell))], ind_word = first_list$apell[which.max(levenshteinSim(APELLIDO, first_list$apell))]) %>% 
  mutate(index_hisp = max(levenshteinSim(APELLIDO, hisp_word)), index_ind = max(levenshteinSim(APELLIDO, ind_word)))

apell_batch2 <- apell_batch2 %>% 
  group_by(APELLIDO) %>% 
  mutate(hisp_word = hispano$apell[which.max(levenshteinSim(APELLIDO, hispano$apell))], ind_word = first_list$apell[which.max(levenshteinSim(APELLIDO, first_list$apell))]) %>% 
  mutate(index_hisp = max(levenshteinSim(APELLIDO, hisp_word)), index_ind = max(levenshteinSim(APELLIDO, ind_word)))

apell_batch3 <- apell_batch3 %>% 
  group_by(APELLIDO) %>% 
  mutate(hisp_word = hispano$apell[which.max(levenshteinSim(APELLIDO, hispano$apell))], ind_word = first_list$apell[which.max(levenshteinSim(APELLIDO, first_list$apell))]) %>% 
  mutate(index_hisp = max(levenshteinSim(APELLIDO, hisp_word)), index_ind = max(levenshteinSim(APELLIDO, ind_word)))

apell_batch4 <- apell_batch4 %>% 
  group_by(APELLIDO) %>% 
  mutate(hisp_word = hispano$apell[which.max(levenshteinSim(APELLIDO, hispano$apell))], ind_word = first_list$apell[which.max(levenshteinSim(APELLIDO, first_list$apell))]) %>% 
  mutate(index_hisp = max(levenshteinSim(APELLIDO, hisp_word)), index_ind = max(levenshteinSim(APELLIDO, ind_word)))

apell_with_simil <- apell_batch1 %>% 
  bind_rows(apell_batch2, apell_batch3, apell_batch4)

################ Darle forma WIDE para ver los apellidos compuestos ################

apellidos_similitud <- apell_with_simil %>% 
    group_by(id) %>%
  mutate(leng = nchar(APELLIDO)) %>% 
  mutate(bigol = if_else(leng == max(leng), 1, 0))

apellidos_ori <- apellidos_similitud %>% 
  filter(bigol == 1)

apellidos_sep <- apellidos_similitud %>% 
  filter(bigol == 0) %>% 
  select(id, APELLIDO, hisp_word, index_hisp, ind_word, index_ind)

apellidos_sep2 <- apellidos_sep %>% 
  group_by(id) %>% 
  mutate(visit = 1:n()) %>% 
  gather("APELLIDO", "hisp_word", "index_hisp", "ind_word", "index_ind", key = variable, value = nam) %>% 
  unite(combi, variable, visit) %>% 
  spread(combi, nam)

apellidos_definitivo <- apellidos_ori %>% 
  select(id, APELLIDO, caso_2, nom_dos_letras, origen_eng, origen_croata, hisp_word, index_hisp, ind_word, index_ind) %>% 
  left_join(apellidos_sep2, by ="id")


apellidos_definitivo$index_hisp <- apellidos_definitivo$index_hisp %>% 
  replace(is.na(.), 0)
apellidos_definitivo$index_hisp_1 <- apellidos_definitivo$index_hisp_1 %>% 
  replace(is.na(.), 0)
apellidos_definitivo$index_hisp_2 <- apellidos_definitivo$index_hisp_2 %>% 
  replace(is.na(.), 0)
apellidos_definitivo$index_hisp_3 <- apellidos_definitivo$index_hisp_3 %>% 
  replace(is.na(.), 0)

apellidos_definitivo$index_ind <- apellidos_definitivo$index_ind %>% 
  replace(is.na(.), 0)

apellidos_definitivo$index_ind_1 <- apellidos_definitivo$index_ind_1 %>% 
  replace(is.na(.), 0)
apellidos_definitivo$index_ind_2 <- apellidos_definitivo$index_ind_2 %>% 
  replace(is.na(.), 0)
apellidos_definitivo$index_ind_3 <- apellidos_definitivo$index_ind_3 %>% 
  replace(is.na(.), 0)



############## Creando las distintas bases con distintos niveles ##############

# Incluyendo la variable de repetición entre nuestras fuentes para comparar.

apellidos_definitivo$fuentes_ind <- first_list$fuentes_ind[match(apellidos_definitivo$ind_word,first_list$apell)]
apellidos_definitivo$fuentes_ind_1 <- first_list$fuentes_ind[match(apellidos_definitivo$ind_word_1,first_list$apell)]
apellidos_definitivo$fuentes_ind_2 <- first_list$fuentes_ind[match(apellidos_definitivo$ind_word_2,first_list$apell)]
apellidos_definitivo$fuentes_ind_3 <- first_list$fuentes_ind[match(apellidos_definitivo$ind_word_3,first_list$apell)]

apellidos_definitivo$fuentes_hisp <- hispano$fuentes_hisp[match(apellidos_definitivo$hisp_word,hispano$apell)]
apellidos_definitivo$fuentes_hisp_1 <- hispano$fuentes_hisp[match(apellidos_definitivo$hisp_word_1,hispano$apell)]
apellidos_definitivo$fuentes_hisp_2 <- hispano$fuentes_hisp[match(apellidos_definitivo$hisp_word_2,hispano$apell)]
apellidos_definitivo$fuentes_hisp_3 <- hispano$fuentes_hisp[match(apellidos_definitivo$hisp_word_3,hispano$apell)]


apellidos_definitivo_50 <- apellidos_definitivo %>% 
  mutate(origen = if_else((index_ind >= 0.5 | index_hisp >= 0.5), if_else(index_ind > index_hisp, 1, if_else(index_hisp > index_ind, 2, if_else(fuentes_ind > fuentes_hisp, 1, if_else(fuentes_ind == fuentes_hisp, 0, 2)))), 0), origen_1 = if_else((index_ind_1 >= 0.6 | index_hisp_1 >= 0.6), if_else(index_ind_1 > index_hisp_1, 1, if_else(index_hisp_1 > index_ind_1, 2, if_else(fuentes_ind_1 > fuentes_hisp_1, 1, if_else(fuentes_ind_1 == fuentes_hisp_1, 0, 2)))), 0),  origen_2 = if_else((index_ind_2 >= 0.6 | index_hisp_2 >= 0.6), if_else(index_ind_2 > index_hisp_2, 1, if_else(index_hisp_2 > index_ind_2, 2, if_else(fuentes_ind_2 > fuentes_hisp_2, 1, if_else(fuentes_ind_2 == fuentes_hisp_2, 0, 2)))), 0), origen_3 = if_else((index_ind_3 >= 0.6 | index_hisp_3 >= 0.6), if_else(index_ind_3 > index_hisp_3, 1, if_else(index_hisp_3 > index_ind_3, 2, if_else(fuentes_ind_3 > fuentes_hisp_3, 1, if_else(fuentes_ind_3 == fuentes_hisp_3, 0, 2)))), 0))

apellidos_definitivo_55 <- apellidos_definitivo %>% 
  mutate(origen = if_else((index_ind >= 0.55 | index_hisp >= 0.55), if_else(index_ind > index_hisp, 1, if_else(index_hisp > index_ind, 2, if_else(fuentes_ind > fuentes_hisp, 1, if_else(fuentes_ind == fuentes_hisp, 0, 2)))), 0), origen_1 = if_else((index_ind_1 >= 0.6 | index_hisp_1 >= 0.6), if_else(index_ind_1 > index_hisp_1, 1, if_else(index_hisp_1 > index_ind_1, 2, if_else(fuentes_ind_1 > fuentes_hisp_1, 1, if_else(fuentes_ind_1 == fuentes_hisp_1, 0, 2)))), 0),  origen_2 = if_else((index_ind_2 >= 0.6 | index_hisp_2 >= 0.6), if_else(index_ind_2 > index_hisp_2, 1, if_else(index_hisp_2 > index_ind_2, 2, if_else(fuentes_ind_2 > fuentes_hisp_2, 1, if_else(fuentes_ind_2 == fuentes_hisp_2, 0, 2)))), 0), origen_3 = if_else((index_ind_3 >= 0.6 | index_hisp_3 >= 0.6), if_else(index_ind_3 > index_hisp_3, 1, if_else(index_hisp_3 > index_ind_3, 2, if_else(fuentes_ind_3 > fuentes_hisp_3, 1, if_else(fuentes_ind_3 == fuentes_hisp_3, 0, 2)))), 0))

apellidos_definitivo_60 <- apellidos_definitivo %>% 
  mutate(origen = if_else((index_ind >= 0.6 | index_hisp >= 0.6), if_else(index_ind > index_hisp, 1, if_else(index_hisp > index_ind, 2, if_else(fuentes_ind > fuentes_hisp, 1, if_else(fuentes_ind == fuentes_hisp, 0, 2)))), 0), origen_1 = if_else((index_ind_1 >= 0.6 | index_hisp_1 >= 0.6), if_else(index_ind_1 > index_hisp_1, 1, if_else(index_hisp_1 > index_ind_1, 2, if_else(fuentes_ind_1 > fuentes_hisp_1, 1, if_else(fuentes_ind_1 == fuentes_hisp_1, 0, 2)))), 0),  origen_2 = if_else((index_ind_2 >= 0.6 | index_hisp_2 >= 0.6), if_else(index_ind_2 > index_hisp_2, 1, if_else(index_hisp_2 > index_ind_2, 2, if_else(fuentes_ind_2 > fuentes_hisp_2, 1, if_else(fuentes_ind_2 == fuentes_hisp_2, 0, 2)))), 0), origen_3 = if_else((index_ind_3 >= 0.6 | index_hisp_3 >= 0.6), if_else(index_ind_3 > index_hisp_3, 1, if_else(index_hisp_3 > index_ind_3, 2, if_else(fuentes_ind_3 > fuentes_hisp_3, 1, if_else(fuentes_ind_3 == fuentes_hisp_3, 0, 2)))), 0))

apellidos_definitivo_65 <- apellidos_definitivo %>% 
  mutate(origen = if_else((index_ind >= 0.65 | index_hisp >= 0.65), if_else(index_ind > index_hisp, 1, if_else(index_hisp > index_ind, 2, if_else(fuentes_ind > fuentes_hisp, 1, if_else(fuentes_ind == fuentes_hisp, 0, 2)))), 0), origen_1 = if_else((index_ind_1 >= 0.6 | index_hisp_1 >= 0.6), if_else(index_ind_1 > index_hisp_1, 1, if_else(index_hisp_1 > index_ind_1, 2, if_else(fuentes_ind_1 > fuentes_hisp_1, 1, if_else(fuentes_ind_1 == fuentes_hisp_1, 0, 2)))), 0),  origen_2 = if_else((index_ind_2 >= 0.6 | index_hisp_2 >= 0.6), if_else(index_ind_2 > index_hisp_2, 1, if_else(index_hisp_2 > index_ind_2, 2, if_else(fuentes_ind_2 > fuentes_hisp_2, 1, if_else(fuentes_ind_2 == fuentes_hisp_2, 0, 2)))), 0), origen_3 = if_else((index_ind_3 >= 0.6 | index_hisp_3 >= 0.6), if_else(index_ind_3 > index_hisp_3, 1, if_else(index_hisp_3 > index_ind_3, 2, if_else(fuentes_ind_3 > fuentes_hisp_3, 1, if_else(fuentes_ind_3 == fuentes_hisp_3, 0, 2)))), 0))

apellidos_definitivo_70 <- apellidos_definitivo %>% 
  mutate(origen = if_else((index_ind >= 0.7 | index_hisp >= 0.7), if_else(index_ind > index_hisp, 1, if_else(index_hisp > index_ind, 2, if_else(fuentes_ind > fuentes_hisp, 1, if_else(fuentes_ind == fuentes_hisp, 0, 2)))), 0), origen_1 = if_else((index_ind_1 >= 0.6 | index_hisp_1 >= 0.6), if_else(index_ind_1 > index_hisp_1, 1, if_else(index_hisp_1 > index_ind_1, 2, if_else(fuentes_ind_1 > fuentes_hisp_1, 1, if_else(fuentes_ind_1 == fuentes_hisp_1, 0, 2)))), 0),  origen_2 = if_else((index_ind_2 >= 0.6 | index_hisp_2 >= 0.6), if_else(index_ind_2 > index_hisp_2, 1, if_else(index_hisp_2 > index_ind_2, 2, if_else(fuentes_ind_2 > fuentes_hisp_2, 1, if_else(fuentes_ind_2 == fuentes_hisp_2, 0, 2)))), 0), origen_3 = if_else((index_ind_3 >= 0.6 | index_hisp_3 >= 0.6), if_else(index_ind_3 > index_hisp_3, 1, if_else(index_hisp_3 > index_ind_3, 2, if_else(fuentes_ind_3 > fuentes_hisp_3, 1, if_else(fuentes_ind_3 == fuentes_hisp_3, 0, 2)))), 0))

apellidos_definitivo_75 <- apellidos_definitivo %>% 
  mutate(origen = if_else((index_ind >= 0.75 | index_hisp >= 0.75), if_else(index_ind > index_hisp, 1, if_else(index_hisp > index_ind, 2, if_else(fuentes_ind > fuentes_hisp, 1, if_else(fuentes_ind == fuentes_hisp, 0, 2)))), 0), origen_1 = if_else((index_ind_1 >= 0.6 | index_hisp_1 >= 0.6), if_else(index_ind_1 > index_hisp_1, 1, if_else(index_hisp_1 > index_ind_1, 2, if_else(fuentes_ind_1 > fuentes_hisp_1, 1, if_else(fuentes_ind_1 == fuentes_hisp_1, 0, 2)))), 0),  origen_2 = if_else((index_ind_2 >= 0.6 | index_hisp_2 >= 0.6), if_else(index_ind_2 > index_hisp_2, 1, if_else(index_hisp_2 > index_ind_2, 2, if_else(fuentes_ind_2 > fuentes_hisp_2, 1, if_else(fuentes_ind_2 == fuentes_hisp_2, 0, 2)))), 0), origen_3 = if_else((index_ind_3 >= 0.6 | index_hisp_3 >= 0.6), if_else(index_ind_3 > index_hisp_3, 1, if_else(index_hisp_3 > index_ind_3, 2, if_else(fuentes_ind_3 > fuentes_hisp_3, 1, if_else(fuentes_ind_3 == fuentes_hisp_3, 0, 2)))), 0))

apellidos_definitivo_80 <- apellidos_definitivo %>% 
  mutate(origen = if_else((index_ind >= 0.8 | index_hisp >= 0.8), if_else(index_ind > index_hisp, 1, if_else(index_hisp > index_ind, 2, if_else(fuentes_ind > fuentes_hisp, 1, if_else(fuentes_ind == fuentes_hisp, 0, 2)))), 0), origen_1 = if_else((index_ind_1 >= 0.6 | index_hisp_1 >= 0.6), if_else(index_ind_1 > index_hisp_1, 1, if_else(index_hisp_1 > index_ind_1, 2, if_else(fuentes_ind_1 > fuentes_hisp_1, 1, if_else(fuentes_ind_1 == fuentes_hisp_1, 0, 2)))), 0),  origen_2 = if_else((index_ind_2 >= 0.6 | index_hisp_2 >= 0.6), if_else(index_ind_2 > index_hisp_2, 1, if_else(index_hisp_2 > index_ind_2, 2, if_else(fuentes_ind_2 > fuentes_hisp_2, 1, if_else(fuentes_ind_2 == fuentes_hisp_2, 0, 2)))), 0), origen_3 = if_else((index_ind_3 >= 0.6 | index_hisp_3 >= 0.6), if_else(index_ind_3 > index_hisp_3, 1, if_else(index_hisp_3 > index_ind_3, 2, if_else(fuentes_ind_3 > fuentes_hisp_3, 1, if_else(fuentes_ind_3 == fuentes_hisp_3, 0, 2)))), 0))


export(apellidos_definitivo_50, "data_complete_50.dta")
export(apellidos_definitivo_55, "data_complete_55.dta")
export(apellidos_definitivo_60, "data_complete_60.dta")
export(apellidos_definitivo_65, "data_complete_65.dta")
export(apellidos_definitivo_70, "data_complete_70.dta")
export(apellidos_definitivo_75, "data_complete_75.dta")
export(apellidos_definitivo_80, "data_complete_80.dta")
