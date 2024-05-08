CREATE DATABASE  IF NOT EXISTS `test1` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `test1`;
-- MySQL dump 10.13  Distrib 8.0.36, for Win64 (x86_64)
--
-- Host: 127.0.0.1    Database: test1
-- ------------------------------------------------------
-- Server version	8.0.36

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `prm_cols_filtre`
--

DROP TABLE IF EXISTS `prm_cols_filtre`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
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
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `prm_ra_liens`
--

DROP TABLE IF EXISTS `prm_ra_liens`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
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
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `prm_ref_sql`
--

DROP TABLE IF EXISTS `prm_ref_sql`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
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
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `prm_rows_filtre`
--

DROP TABLE IF EXISTS `prm_rows_filtre`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
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
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `prm_sql_model`
--

DROP TABLE IF EXISTS `prm_sql_model`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `prm_sql_model` (
  `idSQL` int NOT NULL,
  `INDI_CODE_SQL` varchar(100) NOT NULL,
  `CODE_SQL` varchar(10) NOT NULL,
  `TEXT_SQL` varchar(1000) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `prm_tdb_objets`
--

DROP TABLE IF EXISTS `prm_tdb_objets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `prm_tdb_objets` (
  `idObjet` int NOT NULL,
  `TDB` varchar(200) NOT NULL,
  `PAGE` varchar(200) NOT NULL,
  `OBJET` varchar(200) NOT NULL,
  `TITRE_OBJET` varchar(200) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping events for database 'test1'
--

--
-- Dumping routines for database 'test1'
--
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2024-05-05 11:11:27
