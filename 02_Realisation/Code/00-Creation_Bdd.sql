

CREATE DATABASE IF NOT EXISTS  6k;

USE 6k;


CREATE TABLE IF NOT EXISTS `prm_tdb_objets` (
  `idObjet` int NOT NULL PRIMARY KEY,
  `RAPPR_CODE` varchar(200) NOT NULL,
  `PAGE_CODE` varchar(200) NOT NULL,
  `OBJT_CODE` varchar(200) NOT NULL,
  `OBJT_TYPE` varchar(200) NOT NULL,
  `OBJT_LIBL` varchar(200) NOT NULL,
  `TITRE_OBJET` varchar(200) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `prm_rows` (
  `idObjet` int DEFAULT NULL,
  `idRows` int PRIMARY KEY,
  `ROWS_CODE` varchar(200) DEFAULT NULL,
  `ROWS_NIV` varchar(200) DEFAULT '0',
  `ROWS_ORDR` varchar(200) DEFAULT '0',
  FOREIGN KEY (`idObjet`) REFERENCES `prm_tdb_objets` (`idObjet`) ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



CREATE TABLE IF NOT EXISTS `prm_cols_calcul` (
  `CALC_CODE` varchar(200) PRIMARY KEY,
  `CALC_FORMULE` varchar(200) DEFAULT NULL,
  `CALC_FAMILLE` varchar(200) DEFAULT NULL,
  `CALC_TYPE` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



CREATE TABLE IF NOT EXISTS `prm_cols` (
  `idObjet` int ,
  `idCols` int PRIMARY KEY,
  `COLS_CODE` varchar(200) DEFAULT NULL,
  `COLS_ENTETE1` varchar(200) DEFAULT NULL,
  `COLS_ENTETE2` varchar(200) DEFAULT NULL,
  `COLS_ENTETE3` varchar(200) DEFAULT NULL,
  `COLS_FORMAT` varchar(200) DEFAULT NULL,
  `COLS_PRM_GRAPHE` varchar(200) DEFAULT NULL,
  `COLS_LIBL` varchar(200) DEFAULT NULL,
  `COLS_COEF` varchar(200) DEFAULT NULL,
  `COLS_NATURE` varchar(200) DEFAULT NULL,
  `COLS_DATAMART` varchar(200) DEFAULT NULL,
  `COLS_FORMULE` varchar(200) ,
  `COLS_ORDRE` int DEFAULT 0 ,
  FOREIGN KEY (`idObjet`) REFERENCES `prm_tdb_objets` (`idObjet`) ON UPDATE CASCADE ON DELETE CASCADE

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `prm_cols_filtre` (
  `idObjet` int NOT NULL,
  `idCols` int NOT NULL,
  `COLS_CODE` varchar(10) NOT NULL,
  `COLS_FILTRE_DOMAINE` varchar(10) NOT NULL,
  `DIM_CODE` varchar(10) NOT NULL,
  `DIM_VAL_CODE` varchar(10) NOT NULL,
  `FILTRE_TAB` varchar(60) NOT NULL,
  `FILTRE_CHA` varchar(60) NOT NULL,
  `FILTRE_VAL` varchar(60) NOT NULL,
  `FILTRE_SENS` varchar(10) NOT NULL,
  PRIMARY KEY (`idCols`, `FILTRE_CHA`,`FILTRE_VAL`),
  FOREIGN KEY (`idObjet`) REFERENCES `prm_tdb_objets` (`idObjet`) ON UPDATE CASCADE ON DELETE CASCADE

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;





CREATE TABLE IF NOT EXISTS `dmrc_fact` (
  `idFACTLigne` int ,
  `idCols` int NOT NULL,
  `idRows` int NOT NULL,
  `idObjet` int NOT NULL,
  `DAR_REF` varchar(10) NOT NULL,
  `PERD` varchar(10) DEFAULT NULL,
  `COLS_CODE` varchar(10) DEFAULT NULL,  
  `ROWS_CODE` varchar(10) DEFAULT NULL,
  `idSQLLigne` varchar(200) ,
  `PERIMETRE` varchar(10) DEFAULT NULL,
  `VALEUR` varchar(200) DEFAULT NULL,
  `STATUS` varchar(200) DEFAULT NULL,
  `MSG` varchar(200) DEFAULT 'GOOD',
  `LIEN_INVALIDE` varchar(200) DEFAULT 'GOOD',
  `FORMULE` varchar(200) DEFAULT NULL,
  `FORMULE_VALORISE` varchar(200) DEFAULT NULL,
  `FORMULE_REF` varchar(200) DEFAULT NULL,
  `DATE_TRT` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`idCols`,`idRows`,`idObjet`,`DAR_REF`),
  FOREIGN KEY (`idObjet`) REFERENCES `prm_tdb_objets` (`idObjet`) ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `dmrc_ref_sql` (
  `idSQLLigne` int ,
  `idObjet` int NOT NULL,
  `idCol` int NOT NULL,
  `idRow` int NOT NULL,
  `DAR_REF` varchar(10) NOT NULL,
  `PERD` varchar(10) DEFAULT NULL,
  `COLS_CODE` varchar(10) DEFAULT NULL,
  `ROWS_CODE` varchar(10) DEFAULT NULL,
  `SQL_CODE_SRC` varchar(1000) DEFAULT NULL,
  `SQL_CODE_FINAL` varchar(1000) DEFAULT NULL,
  `PERIMETRE` varchar(10) DEFAULT NULL,
  `DATE_TRT` varchar(200) DEFAULT NULL,
  `NIV` varchar(200) DEFAULT '0',
  PRIMARY KEY (`idCol`,`idRow`,`idObjet`,`DAR_REF`),
  FOREIGN KEY (`idObjet`) REFERENCES `prm_tdb_objets` (`idObjet`) ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT UC_LIEN UNIQUE (`idSQLLigne`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `prm_rows_filtre` (
  `idObjet` int NOT NULL,
  `idRows` int NOT NULL,
  `ROWS_CODE` varchar(10) NOT NULL,
  `DIM_CODE` varchar(10) NOT NULL,
  `DIM_VAL_CODE` varchar(10) NOT NULL,
  `FILTRE_TAB` varchar(60) NOT NULL,
  `FILTRE_CHA` varchar(60) NOT NULL,
  `FILTRE_VAL` varchar(60) NOT NULL,
  `FILTRE_SENS` varchar(10) NOT NULL,
  PRIMARY KEY (`idRows`, `FILTRE_CHA`,`FILTRE_VAL`),
  FOREIGN KEY (`idObjet`) REFERENCES `prm_tdb_objets` (`idObjet`) ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `prm_sql_model` (
  `idSQL` int NOT NULL PRIMARY KEY,
  `INDI_CODE_SQL` varchar(100) NOT NULL,
  `CODE_SQL` varchar(10) NOT NULL,
  `TEXT_SQL` varchar(1000) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;




CREATE TABLE IF NOT EXISTS `vw_cube_dplt_icare_bpce_encr` (
  `id` varchar(255) PRIMARY KEY,
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

CREATE TABLE IF NOT EXISTS `vw_cube_vbpce_apc_faits` (
  `id` varchar(255) PRIMARY KEY,
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
  `C_NIV_2` varchar(255) DEFAULT NULL,
  `C_NIV_3` varchar(255) DEFAULT NULL,
  `C_NIV_4` varchar(255) DEFAULT NULL,
  `C_NIV_41` varchar(255) DEFAULT NULL,
  `CODE_GROUPE_SOURCE` varchar(255) DEFAULT NULL,
  `CODE_TYPE_SOURCE` varchar(255) DEFAULT NULL,
  `CODE_SOURCE` varchar(255) DEFAULT NULL,
  `CODE_NIV3_APC` varchar(255) DEFAULT NULL,
  `CODE_NIV2_APC` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;






CREATE TABLE IF NOT EXISTS `prm_cols_composant` (
  `idRaComp` int PRIMARY KEY,
  `idObjetSrc` int DEFAULT NULL,
  `idObjetCib` int DEFAULT NULL,
  `idColscib` int DEFAULT NULL,
  `COLS_CODE` varchar(200) DEFAULT NULL,
  `CODE_COMPOSANT` varchar(200) DEFAULT NULL,
  `idColsSrc` int DEFAULT NULL,
  `COLS_CODE_SRC` varchar(200) DEFAULT NULL,
  FOREIGN KEY (`idObjetSrc`) REFERENCES `prm_tdb_objets` (`idObjet`) ON UPDATE CASCADE ON DELETE CASCADE,
  FOREIGN KEY (`idObjetSrc`) REFERENCES `prm_tdb_objets` (`idObjet`) ON UPDATE CASCADE ON DELETE CASCADE,
  FOREIGN KEY (`idColscib`) REFERENCES `prm_cols` (`idCols`) ON UPDATE CASCADE ON DELETE CASCADE,
  FOREIGN KEY (`idColsSrc`) REFERENCES `prm_cols` (`idCols`) ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE IF NOT EXISTS `dmrc_lineage` (
    `idCols` int ,
    `idRows` int,
    `Value` int,
    `Origine` varchar(200) ,
    `Formule_valo` varchar(200) , 
    `liste_composants` varchar(200) ,
    FOREIGN KEY (`idCols`) REFERENCES `prm_cols` (`idCols`) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (`idRows`) REFERENCES `prm_rows` (`idRows`) ON UPDATE CASCADE ON DELETE CASCADE

)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE IF NOT EXISTS `dmrc_lineage_final` (
    `id` int PRIMARY KEY AUTO_INCREMENT,
    `idCols` int ,
    `idRows` int,
    `Value` int,
    `Origine` varchar(200) ,
    `Formule_valo` varchar(200) , 
    `liste_composants` varchar(200) ,
    FOREIGN KEY (`idCols`) REFERENCES `prm_cols` (`idCols`) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (`idRows`) REFERENCES `prm_rows` (`idRows`) ON UPDATE CASCADE ON DELETE CASCADE

)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;