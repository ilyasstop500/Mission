from ConDB import con_to_db

cnx = con_to_db("root","1234","127.0.0.1","test2") # replace with username, password, server ip, and db name
cur = cnx.cursor()

query1 = ("""
CREATE TABLE `dmrc_fact` (
  `idLigne` int DEFAULT NULL,
  `DAR_REF` varchar(10) DEFAULT NULL,
  `TBD` varchar(200) DEFAULT NULL,
  `PAGE` varchar(200) DEFAULT NULL,
  `OBJET` varchar(200) DEFAULT NULL,
  `idObjet` int DEFAULT NULL,
  `RA_CODE` varchar(10) DEFAULT NULL,
  `COLS_CODE` varchar(10) DEFAULT NULL,
  `ROWS_CODE` varchar(10) DEFAULT NULL,
  `PERD` varchar(10) DEFAULT NULL,
  `VAL` float DEFAULT NULL,
  `MSG` varchar(1000) DEFAULT NULL,
  `PERIMETRE` varchar(10) DEFAULT NULL,
  `DATE_TRT` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
""")

query2 = ("""
CREATE TABLE `prm_cols_filtre` (
  `COLS_NATURE` varchar(10) NOT NULL,
  `RA_CODE` varchar(10) NOT NULL,
  `idCols` int NOT NULL,
  `COLS_CODE` varchar(10) NOT NULL,
  `COLS_DATAMART` varchar(60) NOT NULL,
  `COLS_FILTRE_DOMAINE` varchar(10) NOT NULL,
  `DIM_VAL_CODE` varchar(10) NOT NULL,
  `idObjet` int NOT NULL,
  `FILTRE_TAB` varchar(60) NOT NULL,
  `FILTRE_CHA` varchar(60) NOT NULL,
  `FILTRE_VAL` varchar(60) NOT NULL,
  `FILTRE_SENS` varchar(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
""")

query3 = ("""
CREATE TABLE `prm_ra_liens` (
  `idRACible` int NOT NULL,
  `idObjet` int NOT NULL,
  `idRA` int NOT NULL,
  `RA_CODE` varchar(10) NOT NULL,
  `idColsCib` int NOT NULL,
  `idRowsCib` int NOT NULL,
  `COLS_CODE` varchar(10) NOT NULL,
  `ROWS_CODE` varchar(10) NOT NULL,
  `LIEN_VALIDE` varchar(10) NOT NULL,
  PRIMARY KEY (`idRACible`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
""")

query4 = ("""
CREATE TABLE `prm_ref_result` (
  `idLigne` int DEFAULT NULL,
  `idObjet` int DEFAULT NULL,
  `TBD` varchar(200) DEFAULT NULL,
  `PAGE` varchar(200) DEFAULT NULL,
  `OBJET` varchar(200) DEFAULT NULL,
  `DAR_REF` varchar(10) DEFAULT NULL,
  `PERD` varchar(10) DEFAULT NULL,
  `RA_CODE` varchar(10) DEFAULT NULL,
  `COLS_CODE` varchar(10) DEFAULT NULL,
  `ROWS_CODE` varchar(10) DEFAULT NULL,
  `SQL_CODE_SRC` varchar(1000) DEFAULT NULL,
  `SQL_CODE_FINAL` varchar(1000) DEFAULT NULL,
  `PERIMETRE` varchar(10) DEFAULT NULL,
  `DATE_TRT` varchar(200) DEFAULT NULL,
  `VALEUR` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
""")

query5 = ("""
CREATE TABLE `prm_ref_sql` (
  `idLigne` int DEFAULT NULL,
  `idObjet` int DEFAULT NULL,
  `TBD` varchar(200) DEFAULT NULL,
  `PAGE` varchar(200) DEFAULT NULL,
  `OBJET` varchar(200) DEFAULT NULL,
  `DAR_REF` varchar(10) DEFAULT NULL,
  `PERD` varchar(10) DEFAULT NULL,
  `RA_CODE` varchar(10) DEFAULT NULL,
  `COLS_CODE` varchar(10) DEFAULT NULL,
  `ROWS_CODE` varchar(10) DEFAULT NULL,
  `SQL_CODE_SRC` varchar(1000) DEFAULT NULL,
  `SQL_CODE_FINAL` varchar(1000) DEFAULT NULL,
  `PERIMETRE` varchar(10) DEFAULT NULL,
  `DATE_TRT` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
""")

query6 = ("""
CREATE TABLE `prm_rows_filtre` (
  `idObjet` int NOT NULL,
  `idRows` int NOT NULL,
  `ROWS_CODE` varchar(10) NOT NULL,
  `DIM_VAL_CODE` varchar(10) NOT NULL,
  `FILTRE_TAB` varchar(60) NOT NULL,
  `FILTRE_CHA` varchar(60) NOT NULL,
  `FILTRE_VAL` varchar(60) NOT NULL,
  `FILTRE_SENS` varchar(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
""")

query7 = ("""
CREATE TABLE `prm_sql_model` (
  `idSQL` int NOT NULL,
  `INDI_CODE_SQL` varchar(100) NOT NULL,
  `CODE_SQL` varchar(10) NOT NULL,
  `TEXT_SQL` varchar(1000) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
""")

query8 = ("""
CREATE TABLE `prm_tdb_objets` (
  `idObjet` int NOT NULL,
  `TDB` varchar(200) NOT NULL,
  `PAGE` varchar(200) NOT NULL,
  `OBJET` varchar(200) NOT NULL,
  `TITRE_OBJET` varchar(200) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
""")

query9 = ("""
CREATE TABLE `vw_cube_dplt_icare_bpce_encr` (
  `id` varchar(255) DEFAULT NULL,
  `PERD_ARRT_INFO` varchar(255) DEFAULT NULL,
  `DATE_TRT` varchar(255) DEFAULT NULL,
  `CODE_ORGN_FINN` varchar(255) DEFAULT NULL,
  `CODE_INDIC_REQT` varchar(255) DEFAULT NULL,
  `CODE_PRDT_ICARE` varchar(255) DEFAULT NULL,
  `CODE_MARC_ICARE` varchar(255) DEFAULT NULL,
  `CODE_PHAS_ICARE` varchar(255) DEFAULT NULL,
  `LIBL_SOUR_ICARE` varchar(255) DEFAULT NULL,
  `MT_VERST` varchar(255) DEFAULT NULL,
  `MT_REMB` varchar(255) DEFAULT NULL,
  `MT_ENCR_DEVS` varchar(255) DEFAULT NULL,
  `MT_ENCR_MOYE` varchar(255) DEFAULT NULL,
  `MT_ENGG` varchar(255) DEFAULT NULL,
  `NB_OUVR` varchar(255) DEFAULT NULL,
  `NB_FERM` varchar(255) DEFAULT NULL,
  `NB_STCK_CONT` varchar(255) DEFAULT NULL,
  `MT_EXCDT` varchar(255) DEFAULT NULL,
  `MT_VENT_NET` varchar(255) DEFAULT NULL,
  `MT_ENCR_MOYE_MARC` varchar(255) DEFAULT NULL,
  `MT_COMM` varchar(255) DEFAULT NULL,
  `MT_COMM_APPR` varchar(255) DEFAULT NULL,
  `MT_COMM_RISQ` varchar(255) DEFAULT NULL,
  `MT_SURC` varchar(255) DEFAULT NULL,
  `MT_COMM_ENCR` varchar(255) DEFAULT NULL,
  `MT_COMM_VERST` varchar(255) DEFAULT NULL,
  `MT_FRS` varchar(255) DEFAULT NULL,
  `TX_FRS` varchar(255) DEFAULT NULL,
  `TX_COMM` varchar(255) DEFAULT NULL,
  `MT_ENCR_MOYE_CUML` varchar(255) DEFAULT NULL,
  `MT_EXCDT_MENS` varchar(255) DEFAULT NULL,
  `NB_VENT_NET_MENS` varchar(255) DEFAULT NULL,
  `MT_VART_STCK` varchar(255) DEFAULT NULL,
  `MT_VART_STCK_CUML` varchar(255) DEFAULT NULL,
  `MT_ENGG_MENS` varchar(255) DEFAULT NULL,
  `MT_ENCR_MENS` varchar(255) DEFAULT NULL,
  `MT_ENCR_VART` varchar(255) DEFAULT NULL,
  `CODE_PRVN` varchar(255) DEFAULT NULL,
  `LIBL_INDIC_REQT` varchar(255) DEFAULT NULL,
  `FAMI_INDIC_REQT` varchar(255) DEFAULT NULL,
  `LIBL_MARC_ICARE` varchar(255) DEFAULT NULL,
  `C_NIV_1` varchar(255) DEFAULT NULL,
  `L_NIV_1` varchar(255) DEFAULT NULL,
  `C_NIV_2` varchar(255) DEFAULT NULL,
  `L_NIV_2` varchar(255) DEFAULT NULL,
  `C_NIV_3` varchar(255) DEFAULT NULL,
  `L_NIV_3` varchar(255) DEFAULT NULL,
  `C_NIV_4` varchar(255) DEFAULT NULL,
  `L_NIV_4` varchar(255) DEFAULT NULL,
  `C_NIV_41` varchar(255) DEFAULT NULL,
  `L_NIV_41` varchar(255) DEFAULT NULL,
  `CODE_NIV0_ICARE` varchar(255) DEFAULT NULL,
  `LIBL_NIV0_ICARE` varchar(255) DEFAULT NULL,
  `CODE_NIV1_ICARE` varchar(255) DEFAULT NULL,
  `LIBL_NIV1_ICARE` varchar(255) DEFAULT NULL,
  `CODE_NIV2_ICARE` varchar(255) DEFAULT NULL,
  `LIBL_NIV2_ICARE` varchar(255) DEFAULT NULL,
  `CODE_NIV3_ICARE` varchar(255) DEFAULT NULL,
  `LIBL_NIV3_ICARE` varchar(255) DEFAULT NULL,
  `CODE_NIV4_ICARE` varchar(255) DEFAULT NULL,
  `LIBL_NIV4_ICARE` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
""")

query10 = ("""
CREATE TABLE `vw_cube_vbpce_apc_faits` (
  `PERD_ARRT_INFO` varchar(255) DEFAULT NULL,
  `DATE_TRT` varchar(255) DEFAULT NULL,
  `CODE_ORGN_FINN` varchar(255) DEFAULT NULL,
  `CODE_PRDT_ICARE` varchar(255) DEFAULT NULL,
  `CODE_MARC_ICARE` varchar(255) DEFAULT NULL,
  `LIBL_SOUR` varchar(255) DEFAULT NULL,
  `CODE_INDC` varchar(255) DEFAULT NULL,
  `FREQUENCE` varchar(255) DEFAULT NULL,
  `NUMR_SEMN` varchar(255) DEFAULT NULL,
  `VALR_INDC` varchar(255) DEFAULT NULL,
  `C_NIV_1` varchar(255) DEFAULT NULL,
  `L_NIV_1` varchar(255) DEFAULT NULL,
  `C_NIV_2` varchar(255) DEFAULT NULL,
  `L_NIV_2` varchar(255) DEFAULT NULL,
  `C_NIV_3` varchar(255) DEFAULT NULL,
  `L_NIV_3` varchar(255) DEFAULT NULL,
  `C_NIV_4` varchar(255) DEFAULT NULL,
  `L_NIV_4` varchar(255) DEFAULT NULL,
  `C_NIV_41` varchar(255) DEFAULT NULL,
  `L_NIV_41` varchar(255) DEFAULT NULL,
  `LIBL_MARC_ICARE` varchar(255) DEFAULT NULL,
  `CODE_GROUPE_SOURCE` varchar(255) DEFAULT NULL,
  `LIBL_TYPE_SOURCE` varchar(255) DEFAULT NULL,
  `CODE_TYPE_SOURCE` varchar(255) DEFAULT NULL,
  `LIBL_SOURCE` varchar(255) DEFAULT NULL,
  `CODE_SOURCE` varchar(255) DEFAULT NULL,
  `LIBL_SOUR_TECH` varchar(255) DEFAULT NULL,
  `LIBL_INDC` varchar(255) DEFAULT NULL,
  `CODE_INDC_METIER` varchar(255) DEFAULT NULL,
  `LIBL_NIV3_APC` varchar(255) DEFAULT NULL,
  `CODE_NIV3_APC` varchar(255) DEFAULT NULL,
  `LIBL_NIV2_APC` varchar(255) DEFAULT NULL,
  `CODE_NIV2_APC` varchar(255) DEFAULT NULL,
  `LIBL_NIV1_APC` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
""")

# Execute the queries
cur.execute(query1)
cur.execute(query2)
cur.execute(query3)
cur.execute(query4)
cur.execute(query5)
cur.execute(query6)
cur.execute(query7)
cur.execute(query8)
cur.execute(query9)
cur.execute(query10)

# Commit the changes
cnx.commit()

# Close cursor and connection
cur.close()
cnx.close()
