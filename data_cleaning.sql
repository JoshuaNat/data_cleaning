-- Data cleaning

SELECT * 
FROM world_layoffs.layoffs;

-- 1 Remove duplicates
-- 2 Standardize the data
-- 3 Null values or blank values
-- 4 Remove Any columns

CREATE TABLE world_layoffs.layoffs_staging
LIKE layoffs;

INSERT world_layoffs.layoffs_staging
SELECT * 
FROM layoffs;

SELECT * 
FROM world_layoffs.layoffs_staging;

-- Removing Duplicates / There's not an ID column to make things easy
-- First, use ROW_NUMBER to identifie rows with the exact same content

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging
)
SELECT * 
FROM duplicate_cte 
WHERE row_num > 1;

-- MYSQL doesn't allow to DELETE directly from a CTE
-- Create another staged table, this time including the row_number column, to delete duplicate rows

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging;

DELETE 
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

-- Standardize data, find and fix issues with the data

SELECT company, TRIM(company)
FROM world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2
SET company = TRIM(company);
