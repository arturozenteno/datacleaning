-- Data Cleaning
-- 1. Remove Duplicates
-- 2. Standarize the Data
-- 3. Null Values/Blank Values
-- 4. Remove Any Columns/Rows



-- 1. Remove Duplicates
-- Create a new table with identical data to work on without altering the original table
CREATE TABLE layoffs_staging AS
SELECT *
FROM layoffs;

SELECT * FROM world_layoffs.layoffs;


-- Partioning our data in order to check for duplicates
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


-- Creating a second stage to delete duplicates
CREATE TABLE `layoffs_staging_two` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Populating our second stage with the original data
INSERT INTO layoffs_staging_two
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Deleting duplicates from our data
DELETE
FROM layoffs_staging_two
WHERE row_num > 1;


-- 2. Standardize Data
-- Update Bally's Interactive industry to 'Gambling Facilities and Casinos'
UPDATE layoffs_staging_two
SET industry = 'Gambling Facilities and Casinos'
WHERE company = "Bally's Interactive";

-- Set blanks to null for easier data handling
UPDATE world_layoffs.layoffs_staging_two
SET industry = NULL
WHERE industry = '';

-- Populate nulls
UPDATE layoffs_staging_two t1
JOIN layoffs_staging_two t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Standardize Crypto variations to 'Crypto'
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging_two
ORDER BY industry;

UPDATE layoffs_staging_two
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Standardize country names by removing trailing period
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging_two
ORDER BY country;

UPDATE layoffs_staging_two
SET country = TRIM(TRAILING '.' FROM country);

-- Verify country names are standardized
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

-- Convert string dates to proper date format using STR_TO_DATE
SELECT *
FROM world_layoffs.layoffs_staging_two;

UPDATE layoffs_staging_two
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Convert column data type to DATE
ALTER TABLE layoffs_staging_two
MODIFY COLUMN `date` DATE;

SELECT *
FROM world_layoffs.layoffs_staging_two;


-- 3. Look at Null Values
-- No changes needed for null values in total_laid_off, percentage_laid_off, and funds_raised_millions
-- Keeping them as null facilitates calculations during the EDA phase



-- 4. Remove Any Columns/Rows
-- Remove rows where total_laid_off is null
SELECT *
FROM world_layoffs.layoffs_staging_two
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE FROM world_layoffs.layoffs_staging_two
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging_two;

ALTER TABLE layoffs_staging_two
DROP COLUMN row_num;

SELECT * 
FROM world_layoffs.layoffs_staging_two;