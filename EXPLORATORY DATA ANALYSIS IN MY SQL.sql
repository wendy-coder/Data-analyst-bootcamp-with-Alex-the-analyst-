-- exploratory data analysis 

select * from layoffs_staging2 ;

select max(total_laid_off)
from layoffs_staging2 ;  -- that is  '12000'
 -- the maximum to total layoff is 12000. a company laidoff  that many employee in one day 
 
 -- let see how much percent of their workforce is that 
 select max(total_laid_off),  max(percentage_laid_off)
from layoffs_staging2 ; -- it is 1 which means 100% of the company was laid off


select * from layoffs_staging2
where   percentage_laid_off = 1  -- all the companies who laid off all their comapny
order by   total_laid_off desc; -- this is to see which ones had most people laid off


select company, sum(total_laid_off)
from layoffs_staging2
 group by company  -- in mine google is top, in alex's amazon is number one
 order by    2 desc ; -- two stand for 2nd column in the select statement 

-- all my dates all null so my data cleaning was not right 
select min(`new_date`), max(`new_date`)
from  layoffs_staging2;

select industry, sum(total_laid_off) 
from   layoffs_staging2 ;

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by YEAR(`date`)
order by 1 desc ; -- THIS GIVES THE YEAR WITH THE MOST LAYOFFS 

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc ; -- the stage with most lasy oof Post-IPO like google amazons ect

select company, avg(percentage_laid_off)
from layoffs_staging2
group by company 
order by 2 desc ; 

-- let look at the progression of layoffs the rolling sums 

-- he decided based on month, it will not work because my date is not good 
-- i will chnage date first 
SET SQL_SAFE_UPDATES = 0;
ALTER TABLE layoffs_staging2   DROP COLUMN date ;
ALTER TABLE layoffs_staging2 ADD COLUMN date text ;

UPDATE layoffs_staging2 t1
JOIN backup_table  t2 
	ON t1.company = t2.company
SET t1.date = t2.date;

select distinct(date)  from layoffs_staging2 ;

SELECT DISTINCT date FROM layoffs_staging2;
UPDATE layoffs_staging2
SET date = STR_TO_DATE(date, '%m/%d/%Y');

-- now we can follow alex as normal 
select  substring( `date`, 1,7) as `month`, sum(total_laid_off)
from   layoffs_staging2 
where substring( `date`, 1,7) is not null 
group by `month` -- this only shows us months
order by `month` asc;

select * 
from  layoffs_staging2 ;

with rolling_total as (
select  substring( `date`, 1,7) as `month`, sum(total_laid_off) as total_off
from   layoffs_staging2 
where substring( `date`, 1,7) is not null 
group by `month` -- this only shows us months
order by `month` asc 
) 
select `Month`, total_off,
sum( total_off ) over(order by `Month`) as rolling_total
from rolling_total ;  -- we can see that as themonth goes on, the lay offs increases, 

-- no we wnat to see how much they were laying off per year 

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 DESC ; 
-- -- -- -- -- 
-- -- -- -- -- 
SELECT company, YEAR(`date`) , SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 desc ;

-- we wnat to find the year in which the most people were laid off

-- with company_year(company, years, Total_laid_off) as
-- (  
-- SELECT company
-- 		, YEAR(`date`) 
--         , SUM(total_laid_off) 
-- FROM layoffs_staging2
-- GROUP BY company, YEAR(`date`)
-- ORDER BY SUM(total_laid_off)  desc -- we wantt o partition it based on year and see how many were aid off per year
-- ) select *, dense_rank() over(partition by  years order by total_laid_off desc )  as ranking
-- from company_year  -- devide years into groups and gives total laid off for each year and ranks year with most laid off
-- where years is not null 
-- order by ranking asc ;

-- mine does not work with cte so i used a query to do it and cte to clean it up and just rename the columns 

with company_year(company, years, Total_laid_off, ranking) as
(select company 
		, YEAR(`date`) 
        , SUM(total_laid_off) 
		,dense_rank() over(partition by  YEAR(`date`)  order by SUM(total_laid_off) desc )  as ranking
FROM layoffs_staging2
where YEAR(`date`)  is not null  
GROUP BY company, YEAR(`date`)
ORDER BY SUM(total_laid_off) desc , ranking asc 
) select * from company_year 
where ranking <= 5
order by ranking ;
-- this means that google had the most laid off and it hapenned in 2023




