-- DATA CLEANING 

select * from layoffs; -- this gives an overview off all the data we imported
 
 ########## Data cleaning 1 chnaging date from text to date time. ##########
-- now let chnage the date time in here since when we chnage it in the setting the data doesnot come through 
-- to turn it into datetime it need to be in the format YYYY-MM-DD HH:MM:SS BUT MINE IS IN THIS FORMAT D/M/YYYY SO I NEED TO CHNAGE THAT 
-- FIRST I NEEDTO BACKUP MY DATA 
CREATE TABLE backup_table AS SELECT * FROM layoffs;
-- I NEED TO Create a new column to THE test conversion
ALTER TABLE layoffs ADD COLUMN new_date_column DATETIME;
-- Now i can conver the text values into date time 
SET SQL_SAFE_UPDATES = 0; -- i was using safe mode preventing the below query to execute so i set safe mode off and then rerun the below and it worked
UPDATE layoffs 
SET new_date_column = STR_TO_DATE(date, '%d/%m/%Y');
SET SQL_SAFE_UPDATES = 1; -- then i turned safe mode back on 
-- now i need to drop the text date column and rename the new column date 
ALTER TABLE layoffs DROP COLUMN date; -- drop the old column date 
ALTER TABLE layoffs CHANGE new_date_column date DATETIME; -- rename the new column date 
-- now the date colun is in datetime format.

########## Data cleaning 1: remove duplicates if there are any  ##########
-- frist let create a staging table 

drop table layoffs_staging;
create table layoffs_staging 
like layoffs ; -- this brings data from layoff to the staging table 

insert layoffs_staging -- insert date into the staging table the like took the shell but not the date 
select * from layoffs ;
-- now let see the staging table 
select * from  layoffs_staging ;
-- -- -- removing duplicates 
-- let identify duplicates 
-- assign every column a row number 
select * from  layoffs ;
select * ,
-- the below works because partition gives one when there is unique values so if there is a number of 2 that means there are duplicates 
row_number() over( partition by company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
from   layoffs_staging; 


-- -- -- -- -- -- -- - let do the whole data cleaning process with a CTE 
with duplicates_CTE as 
( 
select * ,
-- the below works because partition gives one when there is unique values so if there is a number of 2 that means there are duplicates 
row_number() over( partition by company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
from   layoffs_staging

) 
SELECT * 
FROM  duplicates_CTE
WHERE ROW_NUM > 1 ;
-- TO CHECK WHETHER THIS IS REALLY INDEED A DUPLICATE WE DO THIS 
SELECT * FROM   layoffs_staging
WHERE COMPANY  = 'Oyster' ;  -- INDED THIS IS A DUPLICATE THEY ARE THE EXACT SAME. 
-- IN ALEX'S THE THE DATA ENTRIES FOR THE COMPANY HE CHEKED Oda, the funda raised are not the same so it is not duplicate entries 
SELECT * FROM   layoffs_staging
WHERE LOCATION  = 'Oda' ; -- that company does not exist in my date set as my computer did not import everything
-- -- -- -- -- -- -- -- -- -- But alex decided to partition over everything. -- -- -- -- -- -- -- -- -- --
-- -- here is the CTE with partitions again

With duplicates_CTE AS ( 
select * ,
row_number() over( partition by company, industry, total_laid_off, percentage_laid_off, 'date',stage,country , funds_raised_millions ) as row_num -- we added the rest of the columns 
from   layoffs_staging

) 
SELECT * 
FROM  duplicates_CTE
WHERE ROW_NUM > 1 ;
-- NOW ODDER WAS NOT PART OF THE DUPLICATES SO HE CHECKED ANOTHER DULICATES COLUMN SUGGESTED WITH COMPANY NAME 
SELECT * FROM   layoffs_staging
WHERE COMPANY  = 'CASPER ' ;   -- BUT CASPER IS NOT IN MINE. IN HIS Casper repeats more than twice
-- we do not wnat to remove both the real and duplicate, here is how 
-- we can do it without removing it. in msssql we can just idetify the 
-- row number in the cte and delet them, but in my sql we canno do this
--  is  different 
-- like this 
With duplicates_CTE AS ( 
select * ,
row_number() over( partition by company, industry, total_laid_off, percentage_laid_off, 'date',stage,country , funds_raised_millions ) as row_num -- we added the rest of the columns 
from   layoffs_staging

) 
delete 
FROM  duplicates_CTE
WHERE ROW_NUM > 1 ; -- this will just delet duplicate , 
-- this is because you cannot update a cte in my sql

-- -- -- -- here is what we can do.
-- we can take the query in the ct and put it into a staging two data base and delet wher teh row is 2
select * ,
row_number() over
( partition by company, industry, total_laid_off, 
percentage_laid_off, 'date',stage,country ,
 funds_raised_millions ) as row_num  
from   layoffs_staging;

SELECT * FROM   layoffs_staging
WHERE LOCATION  = 'Oda'; 
-- normally to have a select statement you have to copy to clipboard and then select a se;ect statement but in this case it does not work
-- so i have to do it on my own
-- THIS IS THE CREATE TABLE 
DROP TABLE  layoffs_staging2 ; 
CREATE TABLE `layoffs_staging2` ( -- WHEN CREATEING TABLES REMEBER TO USE CREATE TABLE ```` INTEAD OF '''
  `company` TEXT,
  `location` TEXT,
  `industry` TEXT,
  `total_laid_off` INT DEFAULT NULL,
  `percentage_laid_off` INT, 
  `stage` TEXT,
  `country` TEXT,
  `funds_raised_millions` INT DEFAULT NULL,
   `date` datetime,
  `row_num` INT -- WE ADDED THIS
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;


SELECT * FROM layoffs_staging2 ;
-- INSERT DATA INTO THE SECOND STAGING TABLE 

INSERT INTO layoffs_staging2 -- THE DATE INTO LAYOFF TSGING 2 
select * ,
row_number() over
( partition by company, industry, total_laid_off, 
percentage_laid_off, 'date',stage,country ,
 funds_raised_millions ) as row_num  
from   layoffs_staging;
-- NOW LET SEE THE DATA 
SELECT * FROM layoffs_staging2 ; -- THIS SI ALL THE DATA WITH THE ROW NUMNERS FOUND IN LAYOFFSTAGGING. 
-- THE DIFFERENT IS THAT IN HERE ROW_NUMER IS NOT PART OF THE cte only it is there without the CTE as an official row, so now you can 
-- remove duplicates by only selecting where row number is bigger than 1. 
SELECT * FROM layoffs_staging2
where row_num > 1 ;-- this si to check that it is the same duplicates again that is showing 
 -- now let delet the duplicates.
SET SQL_SAFE_UPDATES = 0 ;-- since we are in safe mode we need to 
-- reemeber to go out of safe mode and come back in after we deleet 
delete 
FROM layoffs_staging2
where row_num > 1 ; -- this query delet all duplicates 
SET SQL_SAFE_UPDATES = 1; -- this put things back in safe mode
-- now let seeif there is still some duplicates in teh staging 2 table 
SELECT * FROM layoffs_staging2
where row_num > 1 ;-- nope none. 
-- now we can replace staging table with staging 2 table 
select * from layoffs_staging2 ;-- there is one less record so it is now 563 entries 
-- layoffs_staging2-- the table without duplicates 

########## Data cleaning 2: standardise the data   ##########
 -- finding issues in your data  and fixing it. 
 select DISTINCT(COMPANY ) -- 23 DISTINCT COMPANIES 
 from layoffs_staging2 ;
 
SELECT company, TRIM(company) -- 25 
from layoffs_staging2 ; 

-- how to remove safe updates manuelly 
-- 1. edit, preferences, sql editor, at the bottom there is safe update on, unselect it and safe it. 
-- you might need to restart my sql for this to take place
SET SQL_SAFE_UPDATES = 0 ;
update layoffs_staging2 
set company = TRIM(company); -- trin removes white spaces 

select distinct( industry)
from layoffs_staging2 ; -- in mine i have one crypto, in aex he has, crypto, crypo currency and cryptocurrency. this need to be changed
-- when we doing data analysis, like if you do pie chart there will be three crypto types seperatly in the pie which all represent crypto
-- crypto
-- crypto currency 
-- cryptocurrency 
select *
from layoffs_staging2 
where industry like '%crypto%' ; 
-- let update all of them to crypto 
update layoffs_staging2 
set industry = 'Crypto '
where industry like '%crypto%' ; -- make it all crypto 

select *
from layoffs_staging2 
where industry like '%crypto%' ;  -- let check to see if it worked 

select distinct  (industry)
from    layoffs_staging2;

select distinct(location)
from   layoffs_staging2
order by 1 ; -- it is fine 

select distinct(country)
from   layoffs_staging2
order by 1 ; -- there is united states twice becasuse someone put a full stop at the end
 
update layoffs_staging2  -- let update that 
set country = 'United States'
where industry like '%United states.%' ;
-- let check if it worked 

select distinct(country)
from   layoffs_staging2
order by 1 ; -- not ti does not work so let use trim

select distinct (country), trim(trailing '.' from 'country') 
-- this is looking from somehting that is not a white space
from   layoffs_staging2 ;


update layoffs_staging2  
set country = trim(trailing '.' from 'country') 
where industry like 'United States%' ;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States.%'; -- this does it
SET country = TRIM(BOTH ' .' FROM country); -- <- to trim both extract  . and extract space use this

select distinct(country)
from   layoffs_staging2
order by 1 ; -- to check if it worked, egenrally good to check all or most columns to see if it worked.

select * from   layoffs_staging2; 
-- -- -- -- chnagig date to date columns 
-- update layoffs_staging2 -- this doe snot work --
-- set  `date` = str_to_date(`date`, '%m/%d/%y' ) -- what alex did and it worked

ALTER TABLE layoffs_staging2 ADD COLUMN new_date DATETIME;
SET SQL_SAFE_UPDATES = 0;

UPDATE layoffs_staging2
SET new_date = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2 DROP COLUMN `date`;
ALTER TABLE layoffs_staging2 CHANGE new_date `date` DATETIME;
SET SQL_SAFE_UPDATES = 0 ;

SET SQL_SAFE_UPDATES = 1; -- close updating at the end
########## Data cleaning 3: look at the null or blank values   ##########
 -- null and blanks always hapeen, you can either remove or populate or leave them.
 

 select * from  layoffs_staging2
 where   total_laid_off = null  ; -- i have none , neither does alex
  select * from  layoffs_staging2
 where   total_laid_off is  null  ; -- is does give it but look like the other tables have two null consecutive in two columns 
 
select * from  layoffs_staging2
 where   total_laid_off is  null  and 
 percentage_laid_off is null ;-- all of these datasets will be useless to us so we have to remove them 
 
 
 select distinct industry 
 from  layoffs_staging2
 where  industry is null or industry = ' '; -- can we populate teh adte 
 
 
 
 select * from  layoffs_staging2
 where company = 'Airbnb'  ;-- in another one the industry is called travel so we can populate wher there is null or blanks 
 -- populate blank industry with airb and b with travel 
 
 -- in here i accidently set all the nuls to travel, this is bad, so here how i will redress it
--
 -- alter is for chnaging the structure of a table, update is for changing data inside it

UPDATE layoffs_staging2 T1
JOIN layoffs_staging  T2 
	ON 	T1.company = T2.company
    SET T1.industry = T2.industry ;
    

select company , industry 
 from  layoffs_staging2 ;  -- in alex table he discovers from this that there are 
 -- two Airbnb and that one has an entry on its industry column of travel so he decided to do this.
 
 -- the one below , where he joins two layoffs_stagings table and when they both have teh same company, but industry is null in one but not null in the other, they should populate with what industry is in the other
select * 
from layoffs_staging2  t1
join  layoffs_staging2 t2
	on t1.company = t2.company 
    and t1.location = t2.location 
    where (t1.industry  is null or t1.industry = '') and  
    t2.industry is not null   ;-- cavana and air b and b have some where industry is balnkand some where it is not null or blank
    -- when one is blank wher have to use information from the one to update the one that is blank 
-- set null to blanks firt and then do this. 
update   layoffs_staging2 t1 
join  layoffs_staging2 t2
	on t1.company = t2.company 
    set t.industry  = t2.industry 
    where t2.industry is not null ;


select * from layoffs_staging2
where industry = 'travel' ;

-- what was left is baily but it does not have another populated row
-- total laid off, percentage laid off and fund raised cannot be populated with the data we got
-- if we had employee, we could use percentagelaid off to populate total laid off. funds raised could use data from the internet to populate it.



########## Data cleaning 3: remove any columns that should not be there  ##########
-- some columns are completly irrelevant and blank 
-- sometimes it is not wise to do this. 
SELECT *  
FROM layoffs_staging2
WHERE total_laid_off IS NULL  
  AND percentage_laid_off IS NULL
LIMIT 1000;


-- how to delete the selected data 

delete 
FROM layoffs_staging2
WHERE total_laid_off IS NULL  
  AND percentage_laid_off IS NULL ;
  
  
  Alter table   layoffs_staging2
  drop column row_num ;
  
  select * from   layoffs_staging2




