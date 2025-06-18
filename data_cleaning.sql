-- Data cleaning

SELECT * 
FROM world_layoffs.layoffs;

-- 1 Remove duplicates
-- 2 Standardize the data
-- 3 Null values or blank values
-- 4 Remove Any columns

CREATE TABLE world_layoffs.layoffs_staging
LIKE layoffs;