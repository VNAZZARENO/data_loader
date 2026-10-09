# Audit univers → consommateurs du DataLoader Bloomberg (9 octobre 2026)

Objet : rattacher chaque univers du DataLoader à ses consommateurs réels (ATLAS V3 à V6,
GBT, PSC/PBH, outils pergam-tools, SecretProject, QuickResearch, reporting), vérifier qui
produit quoi et à quel rythme, et relever ce qui est cassé, figé ou orphelin. Résultat
machine : `config/consumers.yaml` (contrôlé par `python -m dl.consumers check`).

## Méthode

Trois passes de découverte en lecture seule par des sous-agents (dépôt ATLAS ; pergam-tools
et autres dépôts QR ; partage et producteurs), puis trois vérificateurs indépendants qui ont
relu chaque `fichier:ligne` cité et cherché les oublis par une autre méthode (crontabs et
services d'abord, motifs ensuite). Troisième contrôle : un script a résolu les 152 citations
des deux listes de liens (fichier existant, numéro de ligne dans le fichier, mot-clé de
l'artefact ou de l'univers présent sur la ligne) ; 134 contiennent le mot-clé, 18 sont des
lignes de crontab ou de wrapper qui prouvent l'horaire ou la commande sans nommer
l'artefact, aucune n'est fausse. Les points qui changent une décision ont en plus été relus
à la main (logs du jour, manifestes, `crontab -l`).

Limites : le poste Bloomberg `Sovann` n'est pas lisible d'ici (tâches planifiées déduites des
manifestes et de la documentation) ; le crontab du lab a été lu une fois en lecture seule ;
les classeurs Excel à liens externes et les dossiers personnels n'ont pas été scannés.

## 1. Ce qui est produit, par qui, à quel rythme

Depuis le 21/09/2026 (premier manifeste), toutes les extractions viennent de l'hôte
`Sovann`, toujours en extraction complète (`daily=false`, profil `default`). Avant cette
date, le store de igv, jp, option_europe et pbh (et les six champs conviction de sxxr) vient
de l'import des classeurs du 18/09 par `dl.migrate` depuis le poste prod ; registre, file
de demandes et descriptions sont écrits côté prod (dashboard).

| Univers | Config | Registre | Dernière production | Cadence observée | État |
|---|---|---|---|---|---|
| sxxr | 6 champs depuis le 09/10 (3 auparavant) | index, 600 actifs + 72 sortants | 08/10 18:04 (xlsx), store au 08/10 | lun-ven, départ 17:50-17:58, 14 min | partial 13/16 (mêmes 5 à 16 tickers manquants), failed 2 (29/09, 01/10, WinError 5 à 18:05) ; 6 champs du store figés au 07/08 |
| global_macro | price seul | liste, 96 | 08/10 14:43 | 10 jours sur 13, horaires 08:25-14:43 | lancement manuel probable, pas de run le 09/10 |
| macro | 7 champs (6 épars) | liste, 40 | 08/10 10:06 | 5 jours sur 8 depuis le 29/09 | pas de run du 05 au 07/10 ni le 09/10 |
| igv | défaut | liste, 113 | store 17/09 ; xlsx réécrit chaque nuit à 01:30 par le cron PSC (yfinance) | aucun manifeste | deux écrivains ; feuilles EPS et Pxtobook absentes du classeur |
| jp | défaut | index, 125 | 13/08 | aucune depuis | figé 57 jours, encore lu |
| pbh | défaut | liste, 208 + 9 | 14/08 (static), 07/08 (conviction) | aucune depuis | figé 56 jours |
| option_europe | 49 champs | liste, 137 | 20/07 (static), 24/07 (bt) | aucune depuis | figé 81 jours, lu chaque matin |
| sx5e, euro_credit | défaut / 7 champs | index 50 / liste 15 | 21/09, 22/09 | 2 runs puis arrêt | euro_credit : champs crédit 0/14 |
| macro_inflation | absent | liste 28, fetch coupé | 29/09 | 1 run | sans lecteur, coupé volontairement |
| nky, spx, splpeqty | défaut | index 225 / 503 / 38 | jamais | jamais | registre sans store ni classeur |

Autres faits : une demande `index_members_hist` (sxxr, 55 fins de trimestre) attend depuis
le 08/10 18:32 et sera dépilée par la prochaine passe ; le dashboard (port 7016) tourne sans
relance automatique du watchdog ; un second collecteur Bloomberg, hors dépôt
(`Stagiaires/Vincent N/data/bloomberg_data_extraction.py`, xbbg), réécrit les CSV legacy
`data/<u>/*.csv` vers 17:51, juste avant la passe `sxxr`.

## 2. Carte univers → consommateurs

Statuts : actif, données figées (lecteur vivant sur une sortie plus produite), en échec,
ponctuel, dormant. Preuves complètes dans `config/consumers.yaml`.

### sxxr (STOXX 600) : 14 lecteurs, dont 9 planifiés

| Consommateur | Horaire | Artefact | Champs | Statut |
|---|---|---|---|---|
| SecretProject v3 (`run_daily_pipeline.sh`) | 18:05 lun-ven | xlsx | EPS, Pxtobook | actif, lit pendant l'écriture du classeur |
| Attribution (service 7008) | 18:00 | xlsx | benchmark | actif, lit avant l'écriture : J-1 |
| SecretProject TopSelection | 14h-18h et 20h | xlsx | price, EPS, Pxtobook, benchmark | en échec depuis le 22/09 (ordre des colonnes) |
| ATLAS V4 `stoxx600` | 18:30 | xlsx | price, EPS, Pxtobook, benchmark | actif |
| Cluster_Spread_MR (screener) | 18:30 | xlsx | price, Pxtobook, EPS | actif, as-of figé au 24/06 par config |
| ATLAS V5 `stoxx600_v5` | 18:45 | xlsx + refdata | + sector | actif |
| ATLAS V6 `stoxx600_v6` | 19:15 | xlsx gelé + refdata gelé | price, EPS, Pxtobook, benchmark, sector, currency | actif |
| ATLAS `atlas_gbt_v1` | 19:45 | xlsx gelé + store clean + refdata | + shares_out, div_yield, announcement_dt | actif, champs du store figés au 07/08 |
| ATLAS `stoxx600_v2` conviction | 09:40 mar-sam | xlsx `_conviction` | best_eps, best_sales, announcement_dt, shares_out, div_yield | en échec depuis le 30/09 (classeur absent) |
| Vigie (API, lab) | à la demande | API series raw | price | actif |
| QuickResearch Overnight, PEQ analyses | manuel | xlsx | benchmark, price | ponctuel |
| ATLAS recherche et outils dev | manuel | xlsx, store, refdata | tous | ponctuel |
| ATLAS env `prod`, SecretProject universes STOXX600 | aucun | xlsx | | dormant |

### igv

| Consommateur | Horaire | Artefact | Champs | Statut |
|---|---|---|---|---|
| SecretProject PSC incumbent | 01:30 mar-sam | xlsx `igv_static` : lit et RÉÉCRIT parameters et price (yfinance) | price | actif, écrivain |
| ATLAS PSC conviction | 09:00 mar-sam | xlsx `igv_static_conviction` | best_eps, best_sales, announcement_dt, shares_out, div_yield | données figées au 07/08, bras f4 neutres |
| Vigie (API), Overnight | à la demande / manuel | API, xlsx | price | actif / ponctuel |

### pbh

| ATLAS PBH conviction | 09:20 mar-sam | xlsx `pbh_static_conviction` | mêmes 5 champs | données figées au 07/08, bras f4 neutres |
|---|---|---|---|---|

Le classeur `ATLAS_data_pbh_static.xlsx` n'a aucun lecteur.

### global_macro

| Consommateur | Horaire | Artefact | Statut |
|---|---|---|---|
| ATLAS macro V1 et V2 | 10:00 lun-ven | xlsx price, 97 colonnes fail-fast | actif |
| ATS fiches (lab) | à la demande | API runs + series raw, 6 tickers de taux | actif |
| Vigie (lab) | à la demande | API series raw | actif |
| Reporting_Mensuel LPES, skills commentaire hebdo et mensuel, procédure GSM | manuel | xlsx price (EURGBP…) ; API | ponctuel |
| ATLAS tracker, SecretProject universes global_macro | manuel / aucun | xlsx | ponctuel / dormant |

### macro

| Consommateur | Horaire | Artefact | Statut |
|---|---|---|---|
| Macro (pergam-tools) via GetMacroData `/api/refresh` | 11:45 lun-ven | store clean : price, survey_*, actual_release, release_dt (parquet lu directement) | actif ; la passe Bloomberg n'a pas lieu tous les matins |
| GetMacroData (service 7019) | permanent | idem via le paquet macro | actif |
| Vigie | à la demande | API | actif |

### jp, option_europe

| Univers | Consommateur | Horaire | Artefact | Statut |
|---|---|---|---|---|
| jp | ATLAS stratégie `jp`, Overnight, SecretProject universes JP | manuel | xlsx EPS, Pxtobook, price | aucun lecteur planifié ; classeur du 13/08 |
| option_europe | PEQ screeners (earnings 08:50, RV 08:55) | lun-ven | xlsx price, earning_implied_move, iv_3m_*, iv_12m_* | en échec : surface du 20/07, erreur de fraîcheur chaque matin ; le screener RV plante à l'import depuis le 30/07 |
| option_europe, option_europe_bt | PEQ analyses P7, P8 | manuel | xlsx iv_*, vol_*, flux | ponctuel |

### Sans consommateur

nky, spx, splpeqty (jamais produits), sx5e, euro_credit, macro_inflation, et les classeurs
`ATLAS_data_pbh_static.xlsx`, `ATLAS_data.xlsx` (legacy Excel du 03/02), `ATLAS_squeeze_fields.xlsx`.
Le registre, les manifestes, la file de demandes, les couches `raw` et `fx_eur` du store et
`_fx` ne sont lus par aucun code hors dashboard ; l'API du dashboard est lue par Vigie et ATS.

### Hors DataLoader (pour mémoire)

CSV legacy `Stagiaires/Vincent N/data/data/<u>/*.csv` (v0, v3, Overnight, PEQ, `stoxx600_v2`,
tracker) produits par un collecteur xbbg séparé ; caches yfinance des incumbents PSC et PBH ;
`Divers/benchmarks_bbg.xlsx` (GetFundPerformances, Reporting_Hebdo) ; `Attribution/
positions_earnings_bbg.xlsx` (Attribution, GetScreenerData) ; `GetLivePrices` (`X:\Quant\LivePrices`) ;
BBGExtract (collecteur blpapi, aucun job déployé). MCP, API, StressLab, RRG, Insights,
DerivativesMonitor, Compass et les autres outils du portail n'utilisent pas le DataLoader.

## 3. Constats qui appellent une décision

1. **Fenêtre de 18 h sur le classeur `sxxr`.** La passe du poste Bloomberg finit entre 18:00
   et 18:11 (14 runs mesurés). Trois lecteurs l'encadrent : Attribution à 18:00:02 (donc
   toujours le benchmark de la veille), TopSelection de 18:05:02 à 18:05:23, v3 de 18:05:03
   à 18:05:19. Les deux échecs du loader (WinError 5 à 18:05:13 le 29/09 et 18:05:15 le 01/10)
   tombent dans ces lectures ; le loader ne réessaie que 9 secondes, moins qu'une lecture.
   Ces soirs-là V4 a fini en `noop` sans mail. Avec six champs la passe finira vers 18:15 :
   les trois lecteurs liraient la veille en permanence. Il faut finir avant 18:00 : avancer
   la tâche de `Sovann` d'au moins 45 minutes ou passer en `--daily` (quelques minutes).
2. **TopSelection est cassé depuis le 22/09.** Les 110 colonnes ajoutées au classeur `sxxr`
   le 21/09 (nouvelle composition du registre) sont à la suite des 490 premières, non triées,
   et `data/atlas_reader.py:126` fait un `reindex(method="ffill")` qui exige des colonnes
   croissantes : `ValueError: index must be monotonic` à chaque passage, aucune exécution
   réussie depuis le 21/09 15:06. La correction est côté lecteur (trier les colonnes) : le
   DataLoader ne doit pas réordonner le classeur, le gel V6 et GBT compare l'ordre des
   colonnes et refuserait la source.
3. **`igv` a deux écrivains.** Le cron PSC de 01:30 réécrit `ATLAS_data_igv_static.xlsx`
   depuis yfinance et en supprime EPS et Pxtobook ; le DataLoader n'a jamais produit cet
   univers depuis l'import du 18/09. Choisir un producteur : soit le DataLoader produit `igv`
   chaque nuit et PSC ne fait que lire, soit le registre `igv` est retiré du DataLoader.
4. **Le profil `conviction` a encore deux lecteurs quotidiens.** PSC et PBH lisent
   `ATLAS_data_{igv,pbh}_static_conviction.xlsx`, figés au 07/08 : leurs bras fondamentaux
   sont neutralisés depuis deux mois. `stoxx600_v2` échoue chaque jour depuis le 30/09 faute
   du classeur `sxxr` équivalent. Options : (a) ajouter best_eps, best_sales, announcement_dt,
   shares_out, div_yield aux listes de champs `igv` et `pbh` et faire lire le classeur
   principal par `research/conviction/research.yaml` ; (b) relancer une extraction
   `--agents conviction` ponctuelle sur igv, pbh, sxxr ; (c) arrêter les bras f4 et
   `stoxx600_v2`. La règle « un univers = une liste de champs » pousse vers (a).
5. **Lecteurs sur données mortes.** Screener PEQ earnings (`option_europe`, 81 jours ; le
   screener RV plante à l'import depuis le 30/07), PBH (`pbh` conviction, 56 jours) ; `jp`
   (57 jours) n'a plus de lecteur planifié. Reprendre la production ou couper les lecteurs.
6. **`macro` et `global_macro` sans cadence.** Lancements manuels et irréguliers alors que
   GetMacroData (11:45), ATS et le shadow ATLAS macro (10:00, en FATAL les 07 et 08/10 faute
   de passe avant 10:00) en dépendent. Planifier les deux passes sur `Sovann` avant 9:30.
7. **Nettoyage.** nky, spx, splpeqty jamais produits ; sx5e, euro_credit, macro_inflation
   sans lecteur ; `bin/cron_macro_shadow.sh` (ATLAS) appelle encore l'ancien collecteur de
   SecretProject (84 échecs sur 84) ; SecretProject universes `commodities` et `euro_credit`
   pointent des classeurs inexistants ; tickers manquants constants de `sxxr` (5 price,
   13 Pxtobook, 16 EPS) à déclarer `sparse` ou à retirer du registre.
8. **Fragilités de schéma.** Macro lit le parquet du store directement (pas `dl.store`) ;
   le catalogue ATLAS macro refuse toute colonne nouvelle ou manquante de `global_macro` ;
   le gel V6 et GBT refuse tout changement de colonnes du classeur `sxxr` (appliquer la
   demande `index_members_hist` en attente changera la composition : prévoir une révision
   explicite des journaux) ; un client API inconnu interroge `series?field=PX_LAST` (alias
   invalide, réponse 200 vide).

## 4. Le rattachement

`config/consumers.yaml` déclare 25 consommateurs (nature, statut, horaire, lectures,
preuves). Le loader journalise au début de chaque passe les consommateurs de l'univers et
avertit quand l'un d'eux attend un champ absent de la liste de champs ; `python -m
dl.consumers check` sort les anomalies (contrôle initial : les cinq champs conviction
attendus par PSC et PBH ne sont pas collectés par les passes `igv` et `pbh` ; deux
consommateurs en échec ; six univers sans consommateur). Suite proposée : afficher les
consommateurs sur la page de chaque univers du dashboard et refuser la coupure d'un univers
ou d'un champ qui a un consommateur `actif` (à faire après la fin des travaux en cours sur
le dashboard).

## 5. Vérification

Section complétée avec les retours des trois vérificateurs : voir « Résultat des
vérifications » ci-dessous.
