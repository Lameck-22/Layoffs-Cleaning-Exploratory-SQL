-- DATA CLEANING


CREATE TABLE Layoffs.layoffs (
	company VARCHAR(100) NULL,
	location VARCHAR(100) NULL,
	industry VARCHAR(100) NULL,
	total_laid_off VARCHAR(100) NULL,
	percentage_laid_off VARCHAR(100) NULL,
	`date` VARCHAR(100) NULL,
	stage VARCHAR(100) NULL,
	country VARCHAR(100) NULL,
	funds_raised_millions VARCHAR(100) NULL,
	id INT AUTO_INCREMENT PRIMARY KEY
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT * FROM layoffs;

SELECT * FROM layoffs_staging;

-- Removing duplicates

SELECT *,
       ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
    FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE Layoffs.layoffs_staging2 (
	company VARCHAR(100) NULL,
	location VARCHAR(100) NULL,
	industry VARCHAR(100) NULL,
	total_laid_off VARCHAR(100) NULL,
	percentage_laid_off VARCHAR(100) NULL,
	`date` VARCHAR(100) NULL,
	stage VARCHAR(100) NULL,
	country VARCHAR(100) NULL,
	funds_raised_millions VARCHAR(100) NULL,
	id INT AUTO_INCREMENT PRIMARY KEY,
	row_num INT
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num >1;

DELETE
FROM layoffs_staging2
WHERE row_num >1;


-- standardizing data

-- removing spaces
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging 
SET company = TRIM(company);

SELECT * FROM layoffs_staging2;


-- changing crypto
SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

SELECT * FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2 
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


-- checking other columns
SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY country;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


-- standardizind date
SELECT `date`, 
       STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')
WHERE `date` IS NOT NULL AND `date` != 'NULL';



-- null values and blanks
SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
  AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2 
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;


SELECT * FROM layoffs_staging2;


-- droping a column
ALTER TABLE layoffs_staging2 
DROP COLUMN row_num;






-- EXPLORATORY DATA ANALYSIS


SELECT * FROM layoffs_staging2;

SELECT SUM(total_laid_off)
FROM layoffs_staging2;


-- looking for companies where percentage laid off is 100% in a day

SELECT * FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY 2;

-- looking for the date ranges of the layoffs
SELECT MAX(`date`) , MIN(`date`)
FROM layoffs_staging2;


-- looking for the industry affected most
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry 
ORDER BY 2 DESC;


-- looking for the country with the highest layoffs
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country 
ORDER BY 2 DESC;

-- looking for the layoffs vs years
SELECT YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1;

-- rolling total_laid_off
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL 
GROUP BY `MONTH`
ORDER BY 1 ASC;
 

WITH Rolling_Total AS (
    SELECT 
        SUBSTRING(`date`, 1, 7) AS `MONTH`, 
        SUM(total_laid_off) AS monthly_total
    FROM layoffs_staging2
    WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
    GROUP BY `MONTH`
    ORDER BY `MONTH` ASC
)
SELECT 
    `MONTH`, 
    monthly_total, 
    SUM(monthly_total) OVER (ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;


